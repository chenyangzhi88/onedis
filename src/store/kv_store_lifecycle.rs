impl KvStore {
    pub fn open(options: Options) -> Self {
        let db = DbImpl::open_with_merge_operator(
            Arc::new(options),
            Some(Arc::new(OnedisIntegerMergeOperator)),
        )
        .expect("failed to open kv_engine for onedis");
        let table = db
            .open_default_schemaless_table()
            .expect("failed to open default schemaless kv_engine table for onedis");
        let txn_db = Arc::new(OptimisticTransactionDb::new(db.clone()));
        KvStore {
            db,
            table,
            txn_db,
            txn: None,
        }
    }

    pub fn new<P: AsRef<Path>>(db_path: P, wal_dir: P, engine_id: u32) -> Self {
        let mut options = Options::default();
        options.db_path = db_path.as_ref().to_path_buf();
        options.wal_dir = wal_dir.as_ref().to_path_buf();
        options.engine_id = engine_id;
        Self::open(options)
    }

    pub fn begin_transaction(&self) -> anyhow::Result<Self> {
        let txn = self
            .txn_db
            .clone()
            .begin_transaction(&TransactionOptions::default())?;
        Ok(KvStore {
            db: self.db.clone(),
            table: self.table.clone(),
            txn_db: self.txn_db.clone(),
            txn: Some(Arc::new(Mutex::new(Some(txn)))),
        })
    }

    pub fn non_transactional_view(&self) -> Self {
        KvStore {
            db: self.db.clone(),
            table: self.table.clone(),
            txn_db: self.txn_db.clone(),
            txn: None,
        }
    }

    pub fn commit_transaction(&self) -> anyhow::Result<()> {
        let Some(txn) = &self.txn else {
            return Ok(());
        };
        let mut guard = txn.lock().expect("transaction mutex poisoned");
        let Some(txn) = guard.take() else {
            return Ok(());
        };
        txn.commit()
            .map_err(|err| anyhow::Error::msg(err.to_string()))
    }

    pub fn discard_transaction(&self) {
        let Some(txn) = &self.txn else {
            return;
        };
        let mut guard = txn.lock().expect("transaction mutex poisoned");
        let _ = guard.take();
    }

    pub async fn commit_transaction_async(&self) -> anyhow::Result<()> {
        let Some(txn) = &self.txn else {
            return Ok(());
        };
        let txn = {
            let mut guard = txn.lock().expect("transaction mutex poisoned");
            guard.take()
        };
        let Some(txn) = txn else {
            return Ok(());
        };
        txn.commit_async()
            .await
            .map_err(|err| anyhow::Error::msg(err.to_string()))
    }

    pub fn is_transactional(&self) -> bool {
        self.txn.is_some()
    }

    pub fn db(&self) -> Arc<DbImpl> {
        self.db.clone()
    }
}
