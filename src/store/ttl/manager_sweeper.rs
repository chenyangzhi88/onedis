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

            let more_expired = self.sweep_once_async().await;
            if more_expired {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Double-check and delete one expired key atomically. The observation is
    /// part of the engine compare-and-write, so a concurrent SET/PERSIST/
    /// EXPIRE cannot be deleted by a stale sweeper decision.
    async fn expire_key_async(&self, entry: &TtlEntry) -> ExpireResult {
        let store = self.store_for_db(entry.db_index);
        let meta_key = main_key(entry.db_index, &entry.key);
        let observed = store.get_raw_observed_async(&meta_key).await;
        let mut batch = WriteBatch::new();

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
                    delete_sub_keys_to_batch(
                        &mut batch,
                        entry.db_index,
                        &entry.key,
                        header.version,
                        header.type_tag,
                    );
                    if header.type_tag == TYPE_JSON {
                        for (node_key, _) in store
                            .scan_prefix_raw_async(&json_node_prefix(
                                entry.db_index,
                                &entry.key,
                                header.version,
                            ))
                            .await
                        {
                            batch.delete(&node_key);
                        }
                    }
                    ExpireResult::Deleted
                }
            }
        };

        self.commit_expire_plan_async(&store, &observed, &batch, planned_result)
            .await
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
        let db_count = self.db_count.load(Ordering::Acquire).max(1) as u16;
        for db_idx in 0..db_count {
            if expired.len() >= batch_size {
                break;
            }
            for (ttl_key, _) in self
                .store_for_db(db_idx)
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

}

enum ExpireResult {
    Deleted,
    Stale,
    NotFound,
}
