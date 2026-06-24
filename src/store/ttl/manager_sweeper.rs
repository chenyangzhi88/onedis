impl TtlManager {
    pub fn start_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move { mgr.sweeper_loop().await });
        info!(
            "TTL sweeper started (interval = {} ms, batch = {})",
            self.config.sweep_interval_ms, self.config.batch_size
        );
    }

    /// Signal the sweeper to exit.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    async fn sweeper_loop(&self) {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                info!("TTL sweeper shutting down");
                return;
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.config.sweep_interval_ms)) => {}
                _ = self.notify.notified() => {}
            }
            if self.shutdown.load(Ordering::Acquire) {
                info!("TTL sweeper shutting down");
                return;
            }

            let more_expired = self.sweep_once_async().await;
            if more_expired {
                tokio::task::yield_now().await;
            }
        }
    }

    /// One sweep cycle: drain expired entries, Double Check, delete.
    #[allow(dead_code)]
    fn sweep_once(&self) -> bool {
        let now = now_ms();
        let expired = self.scan_expired_batch(now, self.config.batch_size);

        if expired.is_empty() {
            return false;
        }

        self.stats.sweep_cycles.fetch_add(1, Ordering::Relaxed);

        let mut deleted = 0usize;
        let mut stale = 0usize;
        let mut batch = WriteBatch::new();

        for entry in expired.iter().take(self.config.batch_size) {
            match self.plan_expire_key(entry, &mut batch) {
                ExpireResult::Deleted => deleted += 1,
                ExpireResult::Stale | ExpireResult::NotFound => stale += 1,
            }
        }
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }

        self.stats
            .keys_expired
            .fetch_add(deleted as u64, Ordering::Relaxed);
        self.stats
            .stale_entries_skipped
            .fetch_add(stale as u64, Ordering::Relaxed);

        if deleted > 0 || stale > 0 {
            debug!("TTL sweep: {} deleted, {} stale/skipped", deleted, stale);
        }

        expired.len() == self.config.batch_size
    }

    async fn sweep_once_async(&self) -> bool {
        let now = now_ms();
        let expired = self
            .scan_expired_batch_async(now, self.config.batch_size)
            .await;

        if expired.is_empty() {
            return false;
        }

        self.stats.sweep_cycles.fetch_add(1, Ordering::Relaxed);

        let mut deleted = 0usize;
        let mut stale = 0usize;
        let mut batch = WriteBatch::new();

        for entry in expired.iter().take(self.config.batch_size) {
            match self.plan_expire_key(entry, &mut batch) {
                ExpireResult::Deleted => deleted += 1,
                ExpireResult::Stale | ExpireResult::NotFound => stale += 1,
            }
        }
        if batch.count() > 0 {
            self.store.write_batch(&batch);
        }

        self.stats
            .keys_expired
            .fetch_add(deleted as u64, Ordering::Relaxed);
        self.stats
            .stale_entries_skipped
            .fetch_add(stale as u64, Ordering::Relaxed);

        if deleted > 0 || stale > 0 {
            debug!("TTL sweep: {} deleted, {} stale/skipped", deleted, stale);
        }

        expired.len() == self.config.batch_size
    }

    #[allow(dead_code)]
    fn scan_expired_batch(&self, now: u64, batch_size: usize) -> Vec<TtlEntry> {
        let mut expired = Vec::with_capacity(batch_size);
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        for db_idx in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            for (ttl_key, _) in self.store.scan_prefix_raw(&ttl_db_prefix(db_idx)) {
                if let Some((expire_ms, parsed_db, key)) = parse_ttl_index_key(&ttl_key) {
                    debug_assert_eq!(parsed_db, db_idx);
                    if expire_ms > now {
                        break;
                    }
                    expired.push(TtlEntry {
                        expire_ms,
                        db_index: parsed_db,
                        key,
                    });
                    if expired.len() >= batch_size {
                        break;
                    }
                }
            }
        }
        expired
    }

    async fn scan_expired_batch_async(&self, now: u64, batch_size: usize) -> Vec<TtlEntry> {
        let mut expired = Vec::with_capacity(batch_size);
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        for db_idx in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            for (ttl_key, _) in self
                .store
                .scan_prefix_raw_async(&ttl_db_prefix(db_idx))
                .await
            {
                if let Some((expire_ms, parsed_db, key)) = parse_ttl_index_key(&ttl_key) {
                    debug_assert_eq!(parsed_db, db_idx);
                    if expire_ms > now {
                        break;
                    }
                    expired.push(TtlEntry {
                        expire_ms,
                        db_index: parsed_db,
                        key,
                    });
                    if expired.len() >= batch_size {
                        break;
                    }
                }
            }
        }
        expired
    }

    // ================================================================
    // Lazy Double Check
    // ================================================================
    //
    // Protocol:
    //
    //   1. Read meta key from KV engine.
    //      → absent?  Already DEL'd by user → discard index entry.
    //
    //   2. Compare real expire_ms with the index entry's expire_ms.
    //      → mismatch? User called EXPIRE / PERSIST / SET EX again after
    //        this entry was inserted → discard (the new deadline has its
    //        own index entry).
    //
    //   3. Both checks pass → build WriteBatch:
    //        • Delete(meta_key)
    //        • DeleteRange(sub-keys, bounded by version)
    //      Commit atomically.

    fn plan_expire_key(&self, entry: &TtlEntry, batch: &mut WriteBatch) -> ExpireResult {
        let meta_key = main_key(entry.db_index, &entry.key);

        // ── Check 1: meta key still alive? ──
        let Some(raw) = self.store.get_raw(&meta_key) else {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::NotFound;
        };

        // ── Check 2: expire_ms matches index entry? ──
        let Some(header) = decode_meta_header(&raw) else {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::Stale;
        };
        if header.expire_ms != entry.expire_ms {
            batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
            return ExpireResult::Stale;
        }

        let hook = self
            .expire_hook
            .read()
            .expect("ttl expire hook lock poisoned")
            .clone();
        if let Some(hook) = hook
            && !hook(entry.db_index, &entry.key, header.type_tag, batch)
        {
            return ExpireResult::Stale;
        }

        // ── Double Check passed — physical deletion ──
        batch.delete(&meta_key);
        batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
        delete_sub_keys_to_batch(
            batch,
            entry.db_index,
            &entry.key,
            header.version,
            header.type_tag,
        );
        if header.type_tag == TYPE_JSON {
            for (node_key, _) in self.store.scan_prefix_raw(&json_node_prefix(
                entry.db_index,
                &entry.key,
                header.version,
            )) {
                batch.delete(&node_key);
            }
        }
        ExpireResult::Deleted
    }
}

enum ExpireResult {
    Deleted,
    Stale,
    NotFound,
}
