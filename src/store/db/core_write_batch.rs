use super::*;

impl Db {
    pub(in crate::store::db) fn write_batch_if_not_empty(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        let augmented = self.batch_with_version_owner_markers(batch);
        let batch = augmented.as_ref().unwrap_or(batch);
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch(batch);
        self.record_or_publish_mutations(batch);
    }

    pub(in crate::store::db) async fn write_batch_if_not_empty_async(&self, batch: &WriteBatch) {
        self.write_batch_if_not_empty_async_inner(batch, true).await;
    }

    /// Commit a mutation to a structure whose version-owner marker is already durable.
    ///
    /// Callers must only use this after reading an existing, non-expired structure version. New
    /// structures and expired-key replacements must use `write_batch_if_not_empty_async` so their
    /// owner marker is created atomically with the main metadata.
    pub(in crate::store::db) async fn write_existing_version_batch_if_not_empty_async(
        &self,
        batch: &WriteBatch,
    ) {
        self.write_batch_if_not_empty_async_inner(batch, false)
            .await;
    }

    async fn write_batch_if_not_empty_async_inner(
        &self,
        batch: &WriteBatch,
        add_version_owner_markers: bool,
    ) {
        if batch.count() == 0 {
            return;
        }
        let augmented = add_version_owner_markers
            .then(|| self.batch_with_version_owner_markers(batch))
            .flatten();
        let batch = augmented.as_ref().unwrap_or(batch);
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
        self.record_or_publish_mutations(batch);
    }

    pub(in crate::store::db) fn write_plain_string_batch_if_not_empty(&self, batch: &WriteBatch) {
        if batch.count() == 0 {
            return;
        }
        self.write_batch_if_not_empty(batch);
    }

    pub(in crate::store::db) async fn write_plain_string_batch_if_not_empty_async(
        &self,
        batch: &WriteBatch,
    ) {
        if batch.count() == 0 {
            return;
        }
        self.write_batch_if_not_empty_async(batch).await;
    }

    pub(in crate::store::db) async fn write_plain_string_batch_owned_if_not_empty_async(
        &self,
        batch: WriteBatch,
    ) {
        if batch.count() == 0 {
            return;
        }
        self.write_batch_if_not_empty_async(&batch).await;
    }

    pub(in crate::store::db) async fn write_plain_string_batch_if_not_empty_without_watch_publish_async(
        &self,
        batch: &WriteBatch,
    ) {
        if batch.count() == 0 {
            return;
        }
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.store.write_batch_async(batch).await;
        if !self.store.is_transactional() {
            self.reconcile_vector_runtimes_for_batch(batch);
        }
    }

    pub(in crate::store::db) async fn compare_and_write_batch_if_not_empty_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        let augmented = self.batch_with_version_owner_markers(batch);
        let batch = augmented.as_ref().unwrap_or(batch);
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
            Err(Status::ConditionFailed(_) | Status::WriteConflict(_)) => Ok(false),
            Err(err) => Err(Error::msg(err.to_string())),
        }
    }

    pub(in crate::store::db) fn compare_and_write_batch_if_not_empty(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        let augmented = self.batch_with_version_owner_markers(batch);
        let batch = augmented.as_ref().unwrap_or(batch);
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        match self.store.compare_and_write_batch(conditions, batch) {
            Ok(()) => {
                self.record_or_publish_mutations(batch);
                Ok(true)
            }
            Err(Status::ConditionFailed(_) | Status::WriteConflict(_)) => Ok(false),
            Err(err) => Err(Error::msg(err.to_string())),
        }
    }

    pub(in crate::store::db) fn record_or_publish_mutations(&self, batch: &WriteBatch) {
        if !self.store.is_transactional() {
            self.reconcile_vector_runtimes_for_batch(batch);
        }
        // This check runs after a non-transactional write. A watch registered
        // before the write is therefore visible here; one registered after
        // this check starts after the mutation and need not observe it.
        if !self.store.is_transactional() && !self.mutation_tracker.has_watched_keys() {
            return;
        }
        let (keys, dbs) = collect_logical_mutations(self.key_layout, self.db_index, batch);
        if keys.is_empty() && dbs.is_empty() {
            return;
        }

        if self.store.is_transactional() {
            let mut pending = self
                .pending_mutations
                .lock()
                .expect("pending mutation mutex poisoned");
            pending
                .keys
                .extend(keys.into_iter().map(|key| (self.db_index, key)));
            pending.dbs.extend(dbs);
            return;
        }

        self.publish_mutations(
            keys.into_iter().map(|key| (self.db_index, key)).collect(),
            dbs,
        );
    }

    pub(in crate::store::db) fn record_external_key_mutation(&self, db_index: u16, key: Vec<u8>) {
        if self.store.is_transactional() {
            self.pending_mutations
                .lock()
                .expect("pending mutation mutex poisoned")
                .keys
                .push((db_index, key));
        } else {
            self.publish_mutations(vec![(db_index, key)], Vec::new());
        }
    }

    pub(in crate::store::db) fn take_pending_mutations(&self) -> (Vec<(u16, Vec<u8>)>, Vec<u16>) {
        let mut pending = self
            .pending_mutations
            .lock()
            .expect("pending mutation mutex poisoned");
        let keys = std::mem::take(&mut pending.keys);
        let dbs = std::mem::take(&mut pending.dbs);
        (keys, dbs)
    }

    pub(in crate::store::db) fn publish_mutations(&self, keys: Vec<(u16, Vec<u8>)>, dbs: Vec<u16>) {
        let mut seen_keys = HashSet::new();
        for (db_index, key) in keys {
            if seen_keys.insert((db_index, key.clone())) {
                self.mutation_tracker.bump_key(db_index, key);
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
        // A transaction has not changed durable state yet. Its logical keys are invalidated while
        // the transaction commit holds the same structural write barriers as counter merges.
        if self.store.is_transactional()
            || !self.counter_cache.ever_populated.load(Ordering::Acquire)
        {
            return;
        }
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
            self.counter_cache.invalidate_db(self.db_index);
            return;
        }
        for key in keys {
            self.counter_cache.invalidate_key(self.db_index, &key);
        }
    }

    pub(in crate::store::db) fn invalidate_counter_cache_for_committed_mutations(
        &self,
        keys: &[(u16, Vec<u8>)],
        dbs: &[u16],
    ) {
        if !self.counter_cache.ever_populated.load(Ordering::Acquire) {
            return;
        }
        for &(db_index, ref key) in keys {
            self.counter_cache.invalidate_key(db_index, key);
        }
        for &db_index in dbs {
            self.counter_cache.invalidate_db(db_index);
        }
    }

    pub(in crate::store::db) fn invalidate_list_meta_cache_for_batch(&self, batch: &WriteBatch) {
        if self.store.is_transactional() {
            return;
        }
        if !self.list_meta_cache_maybe_non_empty.load(Ordering::Acquire) {
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
            self.list_meta_cache_maybe_non_empty
                .store(false, Ordering::Release);
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
            self.list_meta_cache_maybe_non_empty
                .store(true, Ordering::Release);
        }
    }

    pub(in crate::store::db) fn remove_list_meta_cache_if_non_transactional(&self, key: &str) {
        if !self.store.is_transactional() {
            self.list_meta_cache.remove(&self.mk(key));
        }
    }
}
