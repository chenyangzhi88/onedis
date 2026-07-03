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

    fn store_for_db(&self, db_index: u16) -> KvStore {
        self.store.for_db_index(db_index)
    }

    pub fn index_size(&self) -> usize {
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        (0..db_count)
            .map(|db_idx| {
                self.store_for_db(db_idx)
                    .scan_prefix_raw(TTL_INDEX_PREFIX)
                    .len()
            })
            .sum()
    }

    pub fn index_snapshot_for_db(&self, db_index: u16) -> (usize, u64) {
        let now = now_ms();
        let mut count = 0usize;
        let mut total_ttl = 0u64;
        for (ttl_key, _) in self.store_for_db(db_index).scan_prefix_raw(&ttl_db_prefix(db_index)) {
            let Some((expire_ms, parsed_db, _)) = parse_ttl_index_key(&ttl_key) else {
                continue;
            };
            if parsed_db != db_index {
                continue;
            }
            count += 1;
            total_ttl = total_ttl.saturating_add(expire_ms.saturating_sub(now));
        }
        let avg_ttl = if count == 0 {
            0
        } else {
            total_ttl / count as u64
        };
        (count, avg_ttl)
    }

    pub async fn index_size_async(&self) -> usize {
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        let mut total = 0usize;
        for db_idx in 0..db_count {
            total += self
                .store_for_db(db_idx)
                .scan_prefix_raw_async(TTL_INDEX_PREFIX)
                .await
                .len();
        }
        total
    }
}
