impl TtlManager {
    pub fn new(store: KvStore, config: TtlConfig) -> Arc<Self> {
        Arc::new(Self {
            db_count: AtomicU32::new(1),
            store,
            config,
            notify: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
            stats: TtlStats {
                keys_expired: AtomicU64::new(0),
                stale_entries_skipped: AtomicU64::new(0),
                sweep_cycles: AtomicU64::new(0),
            },
            expire_hook: RwLock::new(None),
        })
    }

    pub fn set_expire_hook(&self, hook: Arc<ExpireHook>) {
        let mut guard = self
            .expire_hook
            .write()
            .expect("ttl expire hook lock poisoned");
        *guard = Some(hook);
    }

    pub fn stats(&self) -> &TtlStats {
        &self.stats
    }

    pub fn index_size(&self) -> usize {
        self.store.scan_prefix_raw(TTL_INDEX_PREFIX).len()
    }

    pub async fn index_size_async(&self) -> usize {
        self.store
            .scan_prefix_raw_async(TTL_INDEX_PREFIX)
            .await
            .len()
    }
}
