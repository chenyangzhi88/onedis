impl Db {
    /**
     * 创建数据库
     */
    pub fn new(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
    ) -> Self {
        Self::new_with_mutation_tracker(
            db_index,
            store,
            version_counter,
            ttl_manager,
            Arc::new(KeyMutationTracker::default()),
        )
    }

    pub fn new_with_mutation_tracker(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
        mutation_tracker: Arc<KeyMutationTracker>,
    ) -> Self {
        let store = store.for_db_index(db_index);
        let key_layout = KeyEncodingLayout::open_or_initialize_for_table(&store);
        Db {
            db_index,
            store,
            key_layout,
            changes: Arc::new(AtomicU64::new(0)),
            version_counter,
            ttl_manager,
            counter_cache: Arc::new(DashMap::new()),
            counter_cache_epoch: Arc::new(AtomicU64::new(0)),
            list_meta_cache: Arc::new(DashMap::new()),
            vector_runtimes: Arc::new(VectorRuntimeRegistry::default()),
            fulltext_runtimes: Arc::new(FullTextRuntimeRegistry::default()),
            set_write_locks: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
            mutation_tracker,
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        }
    }

    pub fn transactional_view(&self) -> Result<Self, Error> {
        Ok(Db {
            db_index: self.db_index,
            store: self.store.begin_transaction()?,
            key_layout: self.key_layout,
            changes: self.changes.clone(),
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            counter_cache: self.counter_cache.clone(),
            counter_cache_epoch: self.counter_cache_epoch.clone(),
            list_meta_cache: self.list_meta_cache.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            set_write_locks: self.set_write_locks.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        })
    }

    fn set_write_lock(&self, key: &str) -> &tokio::sync::Mutex<()> {
        &self.set_write_locks[set_write_lock_shard(self.db_index, key)]
    }

    fn next_persisted_version(&self) -> u64 {
        Self::next_persisted_version_for_store(&self.store, &self.version_counter)
    }

    async fn next_persisted_version_async(&self) -> u64 {
        Self::next_persisted_version_for_store_async(&self.store, &self.version_counter).await
    }

    pub fn ttl_observability_snapshot(&self) -> TtlObservabilitySnapshot {
        let stats = self.ttl_manager.stats();
        let (expires, avg_ttl_millis) = self.ttl_manager.index_snapshot_for_db(self.db_index);
        TtlObservabilitySnapshot {
            expired_keys: stats.keys_expired.load(Ordering::Relaxed),
            stale_entries_skipped: stats.stale_entries_skipped.load(Ordering::Relaxed),
            sweep_cycles: stats.sweep_cycles.load(Ordering::Relaxed),
            expires,
            avg_ttl_millis,
        }
    }

    fn next_persisted_version_for_store(store: &KvStore, version_counter: &VersionCounter) -> u64 {
        version_counter.next_reserved(|high_water| {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, high_water);
            store.write_batch_direct(&batch);
        })
    }

    async fn next_persisted_version_for_store_async(
        store: &KvStore,
        version_counter: &VersionCounter,
    ) -> u64 {
        version_counter
            .next_reserved_async(|high_water| async move {
                let mut batch = WriteBatch::new();
                reserve_version_high_water_to_batch(&mut batch, high_water);
                store.write_batch_direct_async(batch).await;
            })
            .await
    }

    pub fn commit_transaction(&self) -> Result<(), Error> {
        let (keys, dbs) = self.take_pending_mutations();
        if keys.is_empty() && dbs.is_empty() {
            self.store.discard_transaction();
            return Ok(());
        }
        self.store.commit_transaction()?;
        let direct_db = self.non_transactional_view();
        direct_db.fulltext_reconcile_committed_keys(&keys)?;
        self.publish_mutations(keys, dbs);
        Ok(())
    }

    pub fn discard_transaction(&self) {
        self.store.discard_transaction();
    }

    pub async fn commit_transaction_async(&self) -> Result<(), Error> {
        let (keys, dbs) = self.take_pending_mutations();
        if keys.is_empty() && dbs.is_empty() {
            self.store.discard_transaction();
            return Ok(());
        }
        self.store.commit_transaction_async().await?;
        let direct_db = self.non_transactional_view();
        direct_db.fulltext_reconcile_committed_keys(&keys)?;
        self.publish_mutations(keys, dbs);
        Ok(())
    }

    fn non_transactional_view(&self) -> Self {
        Db {
            db_index: self.db_index,
            store: self.store.non_transactional_view(),
            key_layout: self.key_layout,
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            changes: self.changes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
            list_meta_cache: self.list_meta_cache.clone(),
            counter_cache: self.counter_cache.clone(),
            counter_cache_epoch: self.counter_cache_epoch.clone(),
            set_write_locks: self.set_write_locks.clone(),
        }
    }
}
