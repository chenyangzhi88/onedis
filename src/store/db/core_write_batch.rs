use super::*;

impl Db {
    pub(in crate::store::db) fn write_batch_if_not_empty(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch(batch);
        self.record_or_publish_mutations(batch);
    }

    pub(in crate::store::db) async fn write_batch_if_not_empty_async(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
        self.record_or_publish_mutations(batch);
    }

    pub(in crate::store::db) async fn write_batch_if_not_empty_without_watch_publish_async(
        &self,
        batch: &WriteBatch,
    ) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
    }

    pub(in crate::store::db) async fn compare_and_write_batch_if_not_empty_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        match self
            .store
            .compare_and_write_batch_async(conditions, batch)
            .await
        {
            Ok(()) => {
                self.record_or_publish_mutations(batch);
                Ok(true)
            }
            Err(Status::Conflict(_)) => Ok(false),
            Err(err) => Err(Error::msg(err.to_string())),
        }
    }

    pub(in crate::store::db) fn record_or_publish_mutations(&self, batch: &WriteBatch) {
        let (keys, dbs) = collect_logical_mutations(self.key_layout, self.db_index, batch);
        if keys.is_empty() && dbs.is_empty() {
            return;
        }

        if self.store.is_transactional() {
            let mut pending = self
                .pending_mutations
                .lock()
                .expect("pending mutation mutex poisoned");
            pending.keys.extend(keys);
            pending.dbs.extend(dbs);
            return;
        }

        self.publish_mutations(keys, dbs);
    }

    pub(in crate::store::db) fn take_pending_mutations(&self) -> (Vec<Vec<u8>>, Vec<u16>) {
        let mut pending = self
            .pending_mutations
            .lock()
            .expect("pending mutation mutex poisoned");
        let keys = std::mem::take(&mut pending.keys);
        let dbs = std::mem::take(&mut pending.dbs);
        (keys, dbs)
    }

    pub(in crate::store::db) fn publish_mutations(&self, keys: Vec<Vec<u8>>, dbs: Vec<u16>) {
        let mut seen_keys = HashSet::new();
        for key in keys {
            if seen_keys.insert(key.clone()) {
                self.mutation_tracker.bump_key(key);
            }
        }

        let mut seen_dbs = HashSet::new();
        for db_index in dbs {
            if seen_dbs.insert(db_index) {
                self.mutation_tracker.bump_db(db_index);
            }
        }
    }

    pub(in crate::store::db) fn invalidate_counter_cache_for_batch(&self, batch: &WriteBatch) {
        let mut clear_all = false;
        let mut keys = Vec::new();
        for (write_type, key, _) in batch.iter() {
            match write_type {
                common::types::write_batch::WriteType::Put
                | common::types::write_batch::WriteType::PutBlobMedium
                | common::types::write_batch::WriteType::PutBlobExternal
                | common::types::write_batch::WriteType::Delete
                | common::types::write_batch::WriteType::Merge => {
                    if let Some(key) =
                        logical_main_key_from_raw_key(self.key_layout, self.db_index, key)
                    {
                        keys.push(key);
                    }
                }
                common::types::write_batch::WriteType::RangeDelete => {
                    clear_all = true;
                    break;
                }
            }
        }

        if clear_all {
            self.counter_cache.clear();
            self.counter_cache_epoch.fetch_add(1, Ordering::Release);
            return;
        }
        if !keys.is_empty() {
            for key in keys {
                self.counter_cache.remove(&key);
            }
            self.counter_cache_epoch.fetch_add(1, Ordering::Release);
        }
    }

    pub(in crate::store::db) fn invalidate_list_meta_cache_for_batch(&self, batch: &WriteBatch) {
        if self.store.is_transactional() {
            return;
        }
        let mut clear_all = false;
        let mut keys = Vec::new();
        for (write_type, key, _) in batch.iter() {
            match write_type {
                WriteType::Put
                | WriteType::PutBlobMedium
                | WriteType::PutBlobExternal
                | WriteType::Delete
                | WriteType::Merge => {
                    if let Some(key) =
                        logical_main_key_from_raw_key(self.key_layout, self.db_index, key)
                    {
                        keys.push(key);
                    }
                }
                WriteType::RangeDelete => {
                    clear_all = true;
                    break;
                }
            }
        }
        if clear_all {
            self.list_meta_cache.clear();
            return;
        }
        for key in keys {
            self.list_meta_cache.remove(&key);
        }
    }

    pub(in crate::store::db) fn cache_list_meta_if_non_transactional(
        &self,
        key: &str,
        meta: ListMeta,
    ) {
        if !self.store.is_transactional() {
            self.list_meta_cache.insert(self.mk(key), meta);
        }
    }

    pub(in crate::store::db) fn remove_list_meta_cache_if_non_transactional(&self, key: &str) {
        if !self.store.is_transactional() {
            self.list_meta_cache.remove(&self.mk(key));
        }
    }
}
