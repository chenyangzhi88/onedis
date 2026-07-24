impl KvStore {
    const ROOT_TABLE: &'static str = "default";

    pub fn open(options: Options) -> Self {
        let db = DbImpl::open_with_merge_operator(
            Arc::new(options),
            Some(Arc::new(OnedisIntegerMergeOperator)),
        )
        .expect("failed to open kv_engine for onedis");
        let table = Self::open_or_create_table(&db, Self::ROOT_TABLE);
        KvStore {
            db,
            table,
            table_name: Arc::from(Self::ROOT_TABLE),
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
            txn: self.txn.clone(),
        }
    }

    fn db_table_name(db_index: u16) -> String {
        format!("onedis_db_{db_index}")
    }

    fn open_or_create_table(db: &Arc<DbImpl>, table_name: &str) -> SchemalessTable {
        match db.open_schemaless_table(table_name) {
            Ok(table) => table,
            Err(open_err) => db
                .create_schemaless_table(table_name, SchemalessTableOptions::default())
                .or_else(|_| db.open_schemaless_table(table_name))
                .unwrap_or_else(|create_err| {
                    panic!(
                        "failed to open or create kv_engine schemaless table {table_name:?}: open={open_err}; create={create_err}"
                    )
                }),
        }
    }

    pub fn begin_transaction(&self) -> anyhow::Result<Self> {
        let txn = self.table.begin_transaction()?;
        let mut txns = BTreeMap::new();
        txns.insert(self.table_name.to_string(), txn);
        Ok(KvStore {
            db: self.db.clone(),
            table: self.table.clone(),
            table_name: self.table_name.clone(),
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
            txn: None,
        }
    }

    fn with_transaction_mut<T>(&self, action: impl FnOnce(&mut SchemalessTransaction) -> T) -> Option<T> {
        let txn_context = self.txn.as_ref()?;
        let mut guard = txn_context.txns.lock().expect("transaction mutex poisoned");
        let txns = guard
            .as_mut()
            .expect("attempted to use transaction after completion");
        let txn = match txns.entry(self.table_name.to_string()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                self.table
                    .begin_transaction()
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
        for (_, txn) in txns {
            txn.commit()
                .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        }
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
        for (_, txn) in txns {
            txn.commit_async()
                .await
                .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        }
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
