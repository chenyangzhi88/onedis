impl KvStore {
    const ROOT_TABLE: &'static str = "default";

    pub fn open(options: Options) -> Self {
        let write_options = if options.wal_sync_interval.is_zero() {
            WriteOptions::sync_wal()
        } else {
            WriteOptions::buffered()
        };
        let version_compaction = Arc::new(crate::store::db::VersionCompactionTracker::default());
        let compaction_filter = Arc::new(crate::store::db::OnedisVersionCompactionFilter::new(
            version_compaction.clone(),
        ));
        let db = DbImpl::open_with_components(
            Arc::new(options),
            vec![Arc::new(OnedisIntegerMergeOperator)],
            Some(compaction_filter),
        )
        .expect("failed to open kv_engine for onedis");
        let table = Self::open_or_create_table(&db, Self::ROOT_TABLE);
        KvStore {
            db,
            table,
            table_name: Arc::from(Self::ROOT_TABLE),
            write_options,
            version_compaction,
            txn: None,
        }
    }

    pub fn new<P: AsRef<Path>>(db_path: P, wal_dir: P, engine_id: u32) -> Self {
        let options = Options {
            db_path: db_path.as_ref().to_path_buf(),
            wal_dir: wal_dir.as_ref().to_path_buf(),
            engine_id,
            ..Options::default()
        };
        Self::open(options)
    }

    pub fn for_db_index(&self, db_index: u16) -> Self {
        self.for_table(&Self::db_table_name(db_index))
    }

    pub fn for_table(&self, table_name: &str) -> Self {
        let table = Self::open_or_create_table(&self.db, table_name);
        KvStore {
            db: self.db.clone(),
            table,
            table_name: Arc::from(table_name),
            write_options: self.write_options.clone(),
            version_compaction: self.version_compaction.clone(),
            txn: self.txn.clone(),
        }
    }

    fn db_table_name(db_index: u16) -> String {
        format!("onedis_db_{db_index}")
    }

    pub(in crate::store) fn is_canonical_db_table(&self) -> bool {
        let Some(raw_index) = self.table_name.strip_prefix("onedis_db_") else {
            return false;
        };
        raw_index
            .parse::<u16>()
            .ok()
            .is_some_and(|db_index| Self::db_table_name(db_index) == self.table_name.as_ref())
    }

    fn open_or_create_table(db: &Arc<DbImpl>, table_name: &str) -> SchemalessTable {
        let table_options =
            SchemalessTableOptions::default().with_merge_operator(OnedisIntegerMergeOperator::NAME);
        match db.open_schemaless_table(table_name) {
            Ok(table)
                if table.descriptor().merge_operator_name.as_deref()
                    == Some(OnedisIntegerMergeOperator::NAME) =>
            {
                table
            }
            Ok(_) => db
                .update_schemaless_table_options(table_name, table_options)
                .unwrap_or_else(|update_err| {
                    panic!(
                        "failed to configure kv_engine schemaless table {table_name:?}: {update_err}"
                    )
                }),
            Err(open_err) => db
                .create_schemaless_table(table_name, table_options)
                .or_else(|_| db.open_schemaless_table(table_name))
                .unwrap_or_else(|create_err| {
                    panic!(
                        "failed to open or create kv_engine schemaless table {table_name:?}: open={open_err}; create={create_err}"
                    )
                }),
        }
    }

    pub fn begin_transaction(&self) -> anyhow::Result<Self> {
        let txn = self
            .table
            .begin_transaction_with_options(SchemalessTransactionOptions {
                write_options: self.write_options.clone(),
                ..SchemalessTransactionOptions::default()
            })?;
        let mut txns = BTreeMap::new();
        txns.insert(self.table_name.to_string(), txn);
        Ok(KvStore {
            db: self.db.clone(),
            table: self.table.clone(),
            table_name: self.table_name.clone(),
            write_options: self.write_options.clone(),
            version_compaction: self.version_compaction.clone(),
            txn: Some(Arc::new(KvStoreTransactionContext {
                txns: Mutex::new(Some(txns)),
            })),
        })
    }

    pub fn non_transactional_view(&self) -> Self {
        KvStore {
            db: self.db.clone(),
            table: self.table.clone(),
            table_name: self.table_name.clone(),
            write_options: self.write_options.clone(),
            version_compaction: self.version_compaction.clone(),
            txn: None,
        }
    }

    pub(crate) fn register_live_version(&self, version: u64) {
        self.version_compaction.register_live(version);
    }

    pub(crate) fn retire_version(&self, version: u64) {
        self.version_compaction.retire(version);
    }

    pub(crate) fn mark_version_compaction_ready(&self) {
        self.version_compaction.mark_ready();
    }

    pub(crate) fn version_owner_scan_start(&self, db_index: u16, prefix: &[u8]) -> Vec<u8> {
        self.version_compaction.owner_scan_start(db_index, prefix)
    }

    pub(crate) fn finish_version_owner_scan(
        &self,
        db_index: u16,
        last_version: Option<u64>,
        exhausted: bool,
    ) {
        self.version_compaction
            .finish_owner_scan(db_index, last_version, exhausted);
    }

    fn with_transaction_mut<T>(
        &self,
        action: impl FnOnce(&mut SchemalessTransaction) -> T,
    ) -> Option<T> {
        let txn_context = self.txn.as_ref()?;
        let mut guard = txn_context.txns.lock().expect("transaction mutex poisoned");
        let txns = guard
            .as_mut()
            .expect("attempted to use transaction after completion");
        let shared_snapshot = txns
            .values()
            .next()
            .map(|transaction| transaction.snapshot().clone());
        let txn = match txns.entry(self.table_name.to_string()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                self.table
                    .begin_transaction_with_options(SchemalessTransactionOptions {
                        snapshot: shared_snapshot,
                        write_options: self.write_options.clone(),
                        ..SchemalessTransactionOptions::default()
                    })
                    .expect("failed to begin kv_engine schemaless transaction"),
            ),
        };
        Some(action(txn))
    }

    pub fn commit_transaction(&self) -> anyhow::Result<()> {
        let Some(txn_context) = &self.txn else {
            return Ok(());
        };
        let txns = {
            let mut guard = txn_context.txns.lock().expect("transaction mutex poisoned");
            guard.take().unwrap_or_default()
        };
        SchemalessTransaction::commit_many(txns.into_values().collect())
            .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        Ok(())
    }

    pub fn discard_transaction(&self) {
        let Some(txn_context) = &self.txn else {
            return;
        };
        let txns = {
            let mut guard = txn_context.txns.lock().expect("transaction mutex poisoned");
            guard.take().unwrap_or_default()
        };
        for (_, txn) in txns {
            let _ = txn.rollback();
        }
    }

    pub async fn commit_transaction_async(&self) -> anyhow::Result<()> {
        let Some(txn_context) = &self.txn else {
            return Ok(());
        };
        let txns = {
            let mut guard = txn_context.txns.lock().expect("transaction mutex poisoned");
            guard.take().unwrap_or_default()
        };
        SchemalessTransaction::commit_many_async(txns.into_values().collect())
            .await
            .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        Ok(())
    }

    pub fn is_transactional(&self) -> bool {
        self.txn.is_some()
    }

    pub fn manual_compaction(&self) -> KvResult<()> {
        self.db.manual_compaction()
    }

    pub fn sync_wal(&self) -> KvResult<()> {
        self.db.sync_wal()
    }

    pub fn get_property(&self, property: &str) -> KvResult<Option<String>> {
        self.db.get_property(property)
    }

    pub(crate) fn engine_handle_for_monitoring(&self) -> Arc<DbImpl> {
        self.db.clone()
    }
}
