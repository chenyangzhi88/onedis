use super::*;

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
        Self::new_with_mutation_tracker_and_vector_runtimes(
            db_index,
            store,
            version_counter,
            ttl_manager,
            mutation_tracker,
            Arc::new(VectorRuntimeRegistry::default()),
            Arc::new(CounterCacheRuntime::default()),
        )
    }

    pub(crate) fn new_with_mutation_tracker_and_vector_runtimes(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
        mutation_tracker: Arc<KeyMutationTracker>,
        vector_runtimes: Arc<VectorRuntimeRegistry>,
        counter_cache: Arc<CounterCacheRuntime>,
    ) -> Self {
        Self::try_new_with_mutation_tracker_and_vector_runtimes(
            db_index,
            store,
            version_counter,
            ttl_manager,
            mutation_tracker,
            vector_runtimes,
            counter_cache,
        )
        .expect("failed to initialize onedis logical database")
    }

    pub(crate) fn try_new_with_mutation_tracker_and_vector_runtimes(
        db_index: u16,
        store: KvStore,
        version_counter: Arc<VersionCounter>,
        ttl_manager: Arc<TtlManager>,
        mutation_tracker: Arc<KeyMutationTracker>,
        vector_runtimes: Arc<VectorRuntimeRegistry>,
        counter_cache: Arc<CounterCacheRuntime>,
    ) -> Result<Self, Error> {
        let store = store.try_for_db_index(db_index)?;
        // The layout marker describes the table itself, not a user transaction.
        // Initialize it through the durable table view before the transactional
        // snapshot is first touched.
        let key_layout =
            KeyEncodingLayout::try_open_or_initialize_for_table(&store.non_transactional_view())?;
        let key_write_locks = ttl_manager.key_write_locks();
        let hash_field_write_locks = ttl_manager.hash_field_write_locks();
        Ok(Db {
            db_index,
            store,
            key_layout,
            changes: Arc::new(AtomicU64::new(0)),
            version_counter,
            ttl_manager,
            counter_cache,
            list_meta_cache: Arc::new(DashMap::new()),
            list_meta_cache_maybe_non_empty: Arc::new(AtomicBool::new(false)),
            vector_runtimes,
            fulltext_runtimes: Arc::new(FullTextRuntimeRegistry::default()),
            key_write_locks,
            hash_field_write_locks,
            mutation_tracker,
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        })
    }

    pub(crate) fn db_index(&self) -> u16 {
        self.db_index
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
            list_meta_cache: self.list_meta_cache.clone(),
            list_meta_cache_maybe_non_empty: self.list_meta_cache_maybe_non_empty.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            key_write_locks: self.key_write_locks.clone(),
            hash_field_write_locks: self.hash_field_write_locks.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
        })
    }

    pub(in crate::store::db) fn set_write_lock(&self, key: &str) -> &KeyWriteLock {
        &self.key_write_locks[key_write_lock_shard(self.db_index, key)]
    }

    pub(in crate::store::db) fn hash_field_write_lock(
        &self,
        key: &str,
        field: &str,
    ) -> &KeyWriteLock {
        &self.hash_field_write_locks[hash_field_write_lock_shard(self.db_index, key, field)]
    }

    pub(in crate::store::db) async fn lock_hash_field_write_shards(
        &self,
        shards: &[usize],
    ) -> Vec<tokio::sync::RwLockWriteGuard<'_, ()>> {
        let mut guards = Vec::with_capacity(shards.len());
        for &shard in shards {
            guards.push(self.hash_field_write_locks[shard].lock().await);
        }
        guards
    }

    pub(in crate::store::db) async fn lock_hash_field_read_shards(
        &self,
        shards: &[usize],
    ) -> Vec<tokio::sync::RwLockReadGuard<'_, ()>> {
        let mut guards = Vec::with_capacity(shards.len());
        for &shard in shards {
            guards.push(self.hash_field_write_locks[shard].read().await);
        }
        guards
    }

    pub(in crate::store::db) async fn lock_write_shards(
        &self,
        shards: &[usize],
    ) -> Vec<tokio::sync::RwLockWriteGuard<'_, ()>> {
        let mut guards = Vec::with_capacity(shards.len());
        for &shard in shards {
            guards.push(self.key_write_locks[shard].lock().await);
        }
        guards
    }

    pub(in crate::store::db) async fn lock_read_shards(
        &self,
        shards: &[usize],
    ) -> Vec<tokio::sync::RwLockReadGuard<'_, ()>> {
        let mut guards = Vec::with_capacity(shards.len());
        for &shard in shards {
            guards.push(self.key_write_locks[shard].read().await);
        }
        guards
    }

    fn transaction_write_lock_shards(&self, keys: &[(u16, Vec<u8>)]) -> Vec<usize> {
        let mut shards = keys
            .iter()
            .map(|(db_index, key)| key_write_lock_shard_bytes(*db_index, key))
            .collect::<Vec<_>>();
        shards.sort_unstable();
        shards.dedup();
        shards
    }

    pub(in crate::store) async fn run_blocking_store_task<T, F>(
        &self,
        operation: F,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(Db) -> Result<T, Error> + Send + 'static,
    {
        let db = self.shared_task_view();
        tokio::task::spawn_blocking(move || operation(db))
            .await
            .map_err(|error| Error::msg(format!("store worker task failed: {error}")))?
    }

    pub(crate) fn shared_task_view(&self) -> Self {
        Db {
            db_index: self.db_index,
            store: self.store.clone(),
            key_layout: self.key_layout,
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            changes: self.changes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: self.pending_mutations.clone(),
            list_meta_cache: self.list_meta_cache.clone(),
            list_meta_cache_maybe_non_empty: self.list_meta_cache_maybe_non_empty.clone(),
            counter_cache: self.counter_cache.clone(),
            key_write_locks: self.key_write_locks.clone(),
            hash_field_write_locks: self.hash_field_write_locks.clone(),
        }
    }

    pub(crate) fn is_transactional(&self) -> bool {
        self.store.is_transactional()
    }

    pub(in crate::store::db) fn next_version(&self) -> u64 {
        self.version_counter.next()
    }

    pub(in crate::store::db) async fn next_version_async(&self) -> u64 {
        self.version_counter.next()
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

    pub(in crate::store::db) fn next_version_for_store(
        _store: &KvStore,
        version_counter: &VersionCounter,
    ) -> u64 {
        version_counter.next()
    }

    pub(in crate::store::db) async fn next_version_for_store_async(
        _store: &KvStore,
        version_counter: &VersionCounter,
    ) -> u64 {
        version_counter.next()
    }

    pub fn commit_transaction(&self) -> Result<(), Error> {
        let (keys, dbs) = self.take_pending_mutations();
        if keys.is_empty() && dbs.is_empty() {
            self.store.discard_transaction();
            return Ok(());
        }
        let shards = if dbs.is_empty() {
            self.transaction_write_lock_shards(&keys)
        } else {
            (0..KEY_WRITE_LOCK_SHARDS).collect()
        };
        let _write_guards = shards
            .iter()
            .map(|&shard| self.key_write_locks[shard].blocking_lock())
            .collect::<Vec<_>>();
        self.store.commit_transaction()?;
        self.invalidate_counter_cache_for_committed_mutations(&keys, &dbs);
        self.publish_mutations(keys.clone(), dbs.clone());
        for &db_index in &dbs {
            self.vector_runtimes.remove_db(db_index);
        }
        if let Err(err) = self.reconcile_committed_keys(&keys) {
            // The storage transaction is already durable and cannot be rolled
            // back here. Keep Redis transaction semantics truthful and let the
            // durable fulltext repair path reconcile the index later.
            log::error!("failed to reconcile fulltext indexes after commit: {err}");
        }
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
        let shards = if dbs.is_empty() {
            self.transaction_write_lock_shards(&keys)
        } else {
            (0..KEY_WRITE_LOCK_SHARDS).collect()
        };
        let _write_guards = self.lock_write_shards(&shards).await;
        self.store.commit_transaction_async().await?;
        self.invalidate_counter_cache_for_committed_mutations(&keys, &dbs);
        self.publish_mutations(keys.clone(), dbs.clone());
        for &db_index in &dbs {
            self.vector_runtimes.remove_db(db_index);
        }
        let reconcile_keys = keys.clone();
        if let Err(err) = self
            .run_blocking_store_task(move |db| db.reconcile_committed_keys(&reconcile_keys))
            .await
        {
            log::error!("failed to reconcile fulltext indexes after async commit: {err}");
        }
        Ok(())
    }

    fn reconcile_committed_keys(&self, keys: &[(u16, Vec<u8>)]) -> Result<(), Error> {
        let mut keys_by_db = BTreeMap::<u16, Vec<Vec<u8>>>::new();
        for (db_index, key) in keys {
            keys_by_db.entry(*db_index).or_default().push(key.clone());
        }
        for (db_index, keys) in keys_by_db {
            let direct_db = self.non_transactional_view_for_db(db_index);
            for key in &keys {
                if let Ok(index) = std::str::from_utf8(key) {
                    direct_db.reconcile_vector_runtime_index(db_index, index);
                }
            }
            // The current DB shares this view's runtime registry, so an
            // immediate refresh is safe. Cross-DB mutations leave the durable
            // outbox for that DB's maintenance/search path instead of
            // consuming it through a private runtime registry.
            direct_db.fulltext_reconcile_committed_keys(&keys, db_index == self.db_index)?;
        }
        Ok(())
    }

    pub(in crate::store::db) fn non_transactional_view(&self) -> Self {
        self.non_transactional_view_for_db(self.db_index)
    }

    pub(in crate::store::db) fn non_transactional_view_for_db(&self, db_index: u16) -> Self {
        Db {
            db_index,
            store: self.store.non_transactional_view().for_db_index(db_index),
            key_layout: self.key_layout,
            version_counter: self.version_counter.clone(),
            ttl_manager: self.ttl_manager.clone(),
            changes: self.changes.clone(),
            fulltext_runtimes: self.fulltext_runtimes.clone(),
            vector_runtimes: self.vector_runtimes.clone(),
            mutation_tracker: self.mutation_tracker.clone(),
            pending_mutations: Arc::new(Mutex::new(PendingMutations::default())),
            list_meta_cache: self.list_meta_cache.clone(),
            list_meta_cache_maybe_non_empty: self.list_meta_cache_maybe_non_empty.clone(),
            counter_cache: self.counter_cache.clone(),
            key_write_locks: self.key_write_locks.clone(),
            hash_field_write_locks: self.hash_field_write_locks.clone(),
        }
    }
}
