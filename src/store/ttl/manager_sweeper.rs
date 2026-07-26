impl TtlManager {
    pub fn start_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        let task = tokio::spawn(async move { mgr.sweeper_loop().await });
        info!(
            "TTL sweeper started (interval = {} ms, batch = {})",
            self.config.sweep_interval_ms, self.config.batch_size
        );
        task
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

            while self.sweep_once_async().await {
                if self.shutdown.load(Ordering::Acquire) {
                    info!("TTL sweeper shutting down");
                    return;
                }
                tokio::task::yield_now().await;
            }
        }
    }

    /// Double-check and delete one expired key atomically. The observation is
    /// part of the engine compare-and-write, so a concurrent SET/PERSIST/
    /// EXPIRE cannot be deleted by a stale sweeper decision.
    async fn expire_key_async(&self, entry: &TtlEntry) -> ExpireResult {
        let store = self.store_for_db(entry.db_index);
        let meta_key = entry.key_encoding.main_key(entry.db_index, &entry.key);
        let observed = store.get_raw_observed_async(&meta_key).await;
        let mut batch = WriteBatch::new();
        let mut expired_header = None;

        let planned_result = match observed.value() {
            None => {
                batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
                ExpireResult::NotFound
            }
            Some(raw) => {
                let Some(header) = decode_meta_header(raw) else {
                    batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
                    return self
                        .commit_expire_plan_async(
                            &store,
                            &observed,
                            &batch,
                            ExpireResult::Stale,
                        )
                        .await;
                };
                if header.expire_ms != entry.expire_ms {
                    batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
                    ExpireResult::Stale
                } else {
                    expired_header = Some(header);
                    let hook = self
                        .expire_hook
                        .read()
                        .expect("ttl expire hook lock poisoned")
                        .clone();
                    if let Some(hook) = hook
                        && !hook(entry.db_index, &entry.key, header.type_tag, &mut batch)
                    {
                        // A hook failure must leave both the value and its TTL index
                        // intact so a later sweep can retry.
                        return ExpireResult::Stale;
                    }

                    batch.delete(&meta_key);
                    batch.delete(&ttl_index_key(entry.expire_ms, entry.db_index, &entry.key));
                    delete_sub_keys_to_batch_with_encoding(
                        &mut batch,
                        entry.key_encoding,
                        entry.db_index,
                        &entry.key,
                        header.version,
                        header.type_tag,
                    );
                    ExpireResult::Deleted
                }
            }
        };

        let result = self
            .commit_expire_plan_async(&store, &observed, &batch, planned_result)
            .await;
        if matches!(result, ExpireResult::Deleted)
            && let Some(header) = expired_header
            && let Some(observer) = self
                .expire_observer
                .read()
                .expect("ttl expire observer lock poisoned")
                .clone()
        {
            observer(
                entry.db_index,
                &entry.key,
                header.type_tag,
                header.version,
            );
        }
        result
    }

    async fn commit_expire_plan_async(
        &self,
        store: &KvStore,
        observed: &crate::store::kv_store::ObservedRawValue,
        batch: &WriteBatch,
        planned_result: ExpireResult,
    ) -> ExpireResult {
        match store
            .compare_and_write_batch_async(&[observed.condition()], batch)
            .await
        {
            Ok(()) => planned_result,
            Err(err) => {
                debug!("TTL sweep compare-and-write skipped after concurrent change: {err}");
                ExpireResult::Stale
            }
        }
    }

    async fn sweep_once_async(&self) -> bool {
        let started = Instant::now();
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
        for entry in expired.iter().take(self.config.batch_size) {
            match self.expire_key_async(entry).await {
                ExpireResult::Deleted => deleted += 1,
                ExpireResult::Stale | ExpireResult::NotFound => stale += 1,
            }
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

        global_metrics().record_ttl_sweep_duration(elapsed_us(started));
        expired.len() == self.config.batch_size
    }

    async fn scan_expired_batch_async(&self, now: u64, batch_size: usize) -> Vec<TtlEntry> {
        let mut expired = Vec::with_capacity(batch_size);
        let db_count = self.db_count.load(Ordering::Acquire).max(1);
        let start_db = self.next_db.load(Ordering::Acquire) % db_count;
        let mut last_db = start_db;
        for offset in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            let db_u32 = (start_db + offset) % db_count;
            let db_idx = db_u32 as u16;
            last_db = db_u32;
            let remaining = batch_size - expired.len();
            let store = self.store_for_db(db_idx);
            let key_encoding = ttl_key_encoding_for_store(&store);
            for (ttl_key, _) in store
                .scan_range_raw_limited_async(
                    &ttl_db_prefix(db_idx),
                    Some(ttl_db_expire_upper_bound(db_idx, now)),
                    remaining,
                )
                .await
            {
                if let Some((expire_ms, parsed_db, key)) = parse_ttl_index_key(&ttl_key) {
                    debug_assert_eq!(parsed_db, db_idx);
                    expired.push(TtlEntry {
                        expire_ms,
                        db_index: parsed_db,
                        key,
                        key_encoding,
                    });
                    if expired.len() >= batch_size {
                        break;
                    }
                }
            }
        }
        self.next_db
            .store((last_db + 1) % db_count, Ordering::Release);
        expired
    }

}

enum ExpireResult {
    Deleted,
    Stale,
    NotFound,
}
