impl TtlManager {
    pub fn add(&self, expire_ms: u64, db_index: u16, key: String) {
        if expire_ms == 0 {
            return;
        }
        let mut batch = WriteBatch::new();
        self.add_to_batch(&mut batch, expire_ms, db_index, &key);
        if batch.count() > 0 {
            self.store_for_db(db_index).write_batch(&batch);
        }
    }

    pub fn add_to_batch(&self, batch: &mut WriteBatch, expire_ms: u64, db_index: u16, key: &str) {
        if expire_ms == 0 {
            return;
        }
        self.db_count
            .fetch_max(db_index as u32 + 1, Ordering::AcqRel);
        batch.put(&ttl_index_key(expire_ms, db_index, key), TTL_INDEX_VALUE);
        self.notify.notify_one();
    }

    pub fn remove(&self, db_index: u16, key: &str) {
        let mut batch = WriteBatch::new();
        self.remove_to_batch(&mut batch, db_index, key);
        if batch.count() > 0 {
            self.store_for_db(db_index).write_batch(&batch);
        }
    }

    pub fn remove_to_batch(&self, _batch: &mut WriteBatch, db_index: u16, key: &str) {
        let _ = (db_index, key);
    }

    pub fn remove_known_to_batch(
        &self,
        batch: &mut WriteBatch,
        expire_ms: u64,
        db_index: u16,
        key: &str,
    ) {
        if expire_ms == 0 {
            return;
        }
        batch.delete(&ttl_index_key(expire_ms, db_index, key));
    }

    pub fn remove_db_to_batch(&self, batch: &mut WriteBatch, db_index: u16) {
        for (key, _) in self.store_for_db(db_index).scan_prefix_raw(&ttl_db_prefix(db_index)) {
            batch.delete(&key);
        }
    }

    pub async fn remove_db_to_batch_async(&self, batch: &mut WriteBatch, db_index: u16) {
        for (key, _) in self
            .store_for_db(db_index)
            .scan_prefix_raw_async(&ttl_db_prefix(db_index))
            .await
        {
            batch.delete(&key);
        }
    }

    // ---------------------------------------------------------- rebuild
    /// Scan persisted metadata needed by the TTL subsystem.
    ///
    /// The TTL namespace itself is the sweeper's source of truth, so rebuild no
    /// longer materializes an in-memory expiration tree.
    pub fn rebuild_from_store(&self, num_dbs: u16, version_counter: &VersionCounter) {
        self.db_count
            .store(num_dbs.max(1) as u32, Ordering::Release);
        let mut with_ttl = 0usize;
        for db_idx in 0..num_dbs {
            for (ttl_key, _) in self.store_for_db(db_idx).scan_prefix_raw(&ttl_db_prefix(db_idx)) {
                if parse_ttl_index_key(&ttl_key).is_some() {
                    with_ttl += 1;
                }
            }
        }

        for db_idx in 0..num_dbs.max(1) {
            if let Some(raw) = self.store_for_db(db_idx).get_raw(VERSION_COUNTER_KEY)
                && raw.len() == 8
            {
                let max_version = u64::from_be_bytes(raw[0..8].try_into().unwrap());
                version_counter.observe(max_version);
            }
            for (version_key, _) in self.store_for_db(db_idx).scan_prefix_raw(VERSION_MARK_PREFIX) {
                if let Some(version) = parse_version_mark_key(&version_key) {
                    version_counter.observe(version);
                }
            }
        }

        let max_version = version_counter.current();
        if max_version > 0 {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, max_version);
            if batch.count() > 0 {
                for db_idx in 0..num_dbs.max(1) {
                    self.store_for_db(db_idx).write_batch(&batch);
                }
            }
        }

        info!(
            "TTL index rebuilt from namespace: {} keys with TTL, max_version = {}",
            with_ttl,
            version_counter.current()
        );
    }

    pub async fn rebuild_from_store_async(&self, num_dbs: u16, version_counter: &VersionCounter) {
        self.db_count
            .store(num_dbs.max(1) as u32, Ordering::Release);
        let mut with_ttl = 0usize;
        for db_idx in 0..num_dbs {
            for (ttl_key, _) in self
                .store_for_db(db_idx)
                .scan_prefix_raw_async(&ttl_db_prefix(db_idx))
                .await
            {
                if parse_ttl_index_key(&ttl_key).is_some() {
                    with_ttl += 1;
                }
            }
        }

        for db_idx in 0..num_dbs.max(1) {
            if let Some(raw) = self
                .store_for_db(db_idx)
                .get_raw_async(VERSION_COUNTER_KEY)
                .await
                && raw.len() == 8
            {
                let max_version = u64::from_be_bytes(raw[0..8].try_into().unwrap());
                version_counter.observe(max_version);
            }
            for (version_key, _) in self
                .store_for_db(db_idx)
                .scan_prefix_raw_async(VERSION_MARK_PREFIX)
                .await
            {
                if let Some(version) = parse_version_mark_key(&version_key) {
                    version_counter.observe(version);
                }
            }
        }

        let max_version = version_counter.current();
        if max_version > 0 {
            let mut batch = WriteBatch::new();
            reserve_version_high_water_to_batch(&mut batch, max_version);
            if batch.count() > 0 {
                for db_idx in 0..num_dbs.max(1) {
                    self.store_for_db(db_idx).write_batch_async(&batch).await;
                }
            }
        }

        info!(
            "TTL index rebuilt from namespace: {} keys with TTL, max_version = {}",
            with_ttl,
            version_counter.current()
        );
    }
}
