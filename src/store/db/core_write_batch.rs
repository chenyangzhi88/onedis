use super::*;

pub(in crate::store::db) fn fail_successful_batch_replies<T>(
    replies: Vec<Result<T, Error>>,
    error: Error,
) -> Vec<Result<T, Error>> {
    let message = error.to_string();
    replies
        .into_iter()
        .map(|reply| match reply {
            Ok(_) => Err(Error::msg(message.clone())),
            Err(error) => Err(error),
        })
        .collect()
}

pub(in crate::store::db) fn storage_batch_error<T>(
    count: usize,
    error: impl std::fmt::Display,
) -> Vec<Result<T, Error>> {
    let message = error.to_string();
    (0..count)
        .map(|_| Err(Error::msg(message.clone())))
        .collect()
}

impl Db {
    pub(in crate::store::db) fn write_batch_if_not_empty(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        let augmented = self.batch_with_version_owner_markers(batch)?;
        let batch = augmented.as_ref().unwrap_or(batch);
        self.store
            .write_batch(batch)
            .map_err(|err| Error::msg(err.to_string()))?;
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_hash_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.invalidate_zset_length_cache_for_batch(batch);
        self.fulltext_observe_committed_outbox_batch(batch);
        self.record_or_publish_mutations(batch);
        Ok(())
    }

    pub(in crate::store::db) async fn write_batch_if_not_empty_async(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        self.write_batch_if_not_empty_async_inner(batch, true, None)
            .await
    }

    /// Commit a batch whose planner already knows and deduplicated every mutated logical key.
    pub(in crate::store::db) async fn write_batch_with_logical_keys_if_not_empty_async(
        &self,
        batch: &WriteBatch,
        logical_keys: &[&str],
    ) -> Result<(), Error> {
        self.write_batch_if_not_empty_async_inner(batch, true, Some(logical_keys))
            .await
    }

    /// Owned equivalent for planners that have already deduplicated their logical keys. Version
    /// owners are appended in place and the engine takes ownership of the same batch allocation.
    pub(in crate::store::db) async fn write_batch_with_logical_keys_owned_if_not_empty_async(
        &self,
        mut batch: WriteBatch,
        logical_keys: &[&str],
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        self.append_version_owner_markers(&mut batch)?;
        let committed_outbox = self.fulltext_collect_committed_outbox_batch(&batch);
        self.store
            .write_batch_owned_async(batch)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        // The owned batch has moved into kv-engine. The planner supplied the exact logical keys,
        // so cache invalidation can stay on the durable side of the commit boundary.
        let committed_keys = logical_keys
            .iter()
            .map(|key| (self.db_index, key.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        self.invalidate_caches_for_committed_mutations(&committed_keys, &[]);
        self.fulltext_publish_committed_outbox(committed_outbox);
        self.record_or_publish_known_key_mutations(logical_keys);
        Ok(())
    }

    /// Commit a mutation to a structure whose version-owner marker is already durable.
    ///
    /// Callers must only use this after reading an existing, non-expired structure version. New
    /// structures and expired-key replacements must use `write_batch_if_not_empty_async` so their
    /// owner marker is created atomically with the main metadata.
    pub(in crate::store::db) async fn write_existing_version_batch_if_not_empty_async(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        self.write_batch_if_not_empty_async_inner(batch, false, None)
            .await
    }

    async fn write_batch_if_not_empty_async_inner(
        &self,
        batch: &WriteBatch,
        add_version_owner_markers: bool,
        logical_keys: Option<&[&str]>,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        let augmented = if add_version_owner_markers {
            self.batch_with_version_owner_markers(batch)?
        } else {
            None
        };
        let batch = augmented.as_ref().unwrap_or(batch);
        self.store
            .write_batch_async(batch)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_hash_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.invalidate_zset_length_cache_for_batch(batch);
        self.fulltext_observe_committed_outbox_batch(batch);
        if let Some(logical_keys) = logical_keys {
            self.record_or_publish_known_key_mutations(logical_keys);
        } else {
            self.record_or_publish_mutations(batch);
        }
        Ok(())
    }

    pub(in crate::store::db) fn write_plain_string_batch_if_not_empty(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        self.write_batch_if_not_empty(batch)
    }

    pub(in crate::store::db) async fn write_plain_string_batch_if_not_empty_async(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        self.write_batch_if_not_empty_async(batch).await
    }

    pub(in crate::store::db) async fn write_plain_string_batch_owned_if_not_empty_async(
        &self,
        mut batch: WriteBatch,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        self.append_version_owner_markers(&mut batch)?;
        let committed_outbox = self.fulltext_collect_committed_outbox_batch(&batch);
        let (keys, dbs) = collect_logical_mutations(self.key_layout, self.db_index, &batch);
        self.store
            .write_batch_owned_async(batch)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let committed_keys = keys
            .iter()
            .cloned()
            .map(|key| (self.db_index, key))
            .collect::<Vec<_>>();
        self.invalidate_caches_for_committed_mutations(&committed_keys, &dbs);
        self.fulltext_publish_committed_outbox(committed_outbox);
        if !self.store.is_transactional() && self.vector_runtimes.has_active_runtimes() {
            if dbs.contains(&self.db_index) {
                self.vector_runtimes.remove_db(self.db_index);
            } else {
                let mut seen = HashSet::new();
                for key in &keys {
                    if seen.insert(key)
                        && let Ok(index) = std::str::from_utf8(key)
                    {
                        self.reconcile_vector_runtime_index(self.db_index, index);
                    }
                }
            }
        }
        self.record_or_publish_collected_mutations(keys, dbs);
        Ok(())
    }

    pub(in crate::store::db) async fn write_plain_string_batch_if_not_empty_without_watch_publish_async(
        &self,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        if batch.count() == 0 {
            return Ok(());
        }
        self.store
            .write_batch_async(batch)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        self.invalidate_counter_cache_for_batch(batch);
        self.invalidate_hash_counter_cache_for_batch(batch);
        self.invalidate_list_meta_cache_for_batch(batch);
        self.invalidate_zset_length_cache_for_batch(batch);
        if !self.store.is_transactional() && self.vector_runtimes.has_active_runtimes() {
            self.reconcile_vector_runtimes_for_batch(batch);
        }
        Ok(())
    }

    pub(in crate::store::db) async fn compare_and_write_batch_if_not_empty_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        self.compare_and_write_batch_if_not_empty_async_inner(conditions, batch, true)
            .await
    }

    /// Async counterpart used by vector commands which publish their exact
    /// runtime delta after the durable conditional batch succeeds.
    pub(in crate::store::db) async fn compare_and_write_vector_batch_if_not_empty_async(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        self.compare_and_write_batch_if_not_empty_async_inner(conditions, batch, false)
            .await
    }

    async fn compare_and_write_batch_if_not_empty_async_inner(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
        reconcile_vector_runtimes: bool,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        let augmented = self.batch_with_version_owner_markers(batch)?;
        let batch = augmented.as_ref().unwrap_or(batch);
        match self
            .store
            .compare_and_write_batch_async(conditions, batch)
            .await
        {
            Ok(()) => {
                self.invalidate_counter_cache_for_batch(batch);
                self.invalidate_hash_counter_cache_for_batch(batch);
                self.invalidate_list_meta_cache_for_batch(batch);
                self.invalidate_zset_length_cache_for_batch(batch);
                self.fulltext_observe_committed_outbox_batch(batch);
                self.record_or_publish_mutations_inner(batch, reconcile_vector_runtimes);
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
        self.compare_and_write_batch_if_not_empty_inner(conditions, batch, true)
    }

    /// Commit a batch for a vector operation that updates its runtime explicitly.
    ///
    /// Generic keyspace writes must reconcile vector runtimes because DEL,
    /// RENAME, expiry and transactions can invalidate an index behind the
    /// vector API. Native vector mutations already hold the collection write
    /// lock and publish the exact runtime delta after the durable commit, so a
    /// second registry-wide reconciliation is redundant.
    pub(in crate::store::db) fn compare_and_write_vector_batch_if_not_empty(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<bool, Error> {
        self.compare_and_write_batch_if_not_empty_inner(conditions, batch, false)
    }

    fn compare_and_write_batch_if_not_empty_inner(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
        reconcile_vector_runtimes: bool,
    ) -> Result<bool, Error> {
        if batch.count() == 0 {
            return Ok(true);
        }
        let augmented = self.batch_with_version_owner_markers(batch)?;
        let batch = augmented.as_ref().unwrap_or(batch);
        match self.store.compare_and_write_batch(conditions, batch) {
            Ok(()) => {
                self.invalidate_counter_cache_for_batch(batch);
                self.invalidate_hash_counter_cache_for_batch(batch);
                self.invalidate_list_meta_cache_for_batch(batch);
                self.invalidate_zset_length_cache_for_batch(batch);
                self.fulltext_observe_committed_outbox_batch(batch);
                self.record_or_publish_mutations_inner(batch, reconcile_vector_runtimes);
                Ok(true)
            }
            Err(Status::ConditionFailed(_) | Status::WriteConflict(_)) => Ok(false),
            Err(err) => Err(Error::msg(err.to_string())),
        }
    }

    pub(in crate::store::db) fn record_or_publish_mutations(&self, batch: &WriteBatch) {
        self.record_or_publish_mutations_inner(batch, true);
    }

    fn record_or_publish_mutations_inner(
        &self,
        batch: &WriteBatch,
        reconcile_vector_runtimes: bool,
    ) {
        if reconcile_vector_runtimes
            && !self.store.is_transactional()
            && self.vector_runtimes.has_active_runtimes()
        {
            self.reconcile_vector_runtimes_for_batch(batch);
        }
        // This check runs after a non-transactional write. A watch registered
        // before the write is therefore visible here; one registered after
        // this check starts after the mutation and need not observe it.
        if !self.store.is_transactional() && !self.mutation_tracker.has_observers() {
            return;
        }
        let (keys, dbs) = collect_logical_mutations(self.key_layout, self.db_index, batch);
        self.record_or_publish_collected_mutations(keys, dbs);
    }

    fn record_or_publish_collected_mutations(&self, keys: Vec<Vec<u8>>, dbs: Vec<u16>) {
        if !self.store.is_transactional() && !self.mutation_tracker.has_observers() {
            return;
        }
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

    fn record_or_publish_known_key_mutations(&self, logical_keys: &[&str]) {
        if !self.store.is_transactional() && self.vector_runtimes.has_active_runtimes() {
            self.reconcile_vector_runtimes_for_known_keys(logical_keys);
        }
        if !self.store.is_transactional() && !self.mutation_tracker.has_observers() {
            return;
        }

        if self.store.is_transactional() {
            self.pending_mutations
                .lock()
                .expect("pending mutation mutex poisoned")
                .keys
                .extend(logical_keys.iter().map(|key| (self.db_index, self.mk(key))));
            return;
        }

        // The caller already deduplicated these keys, so do not rebuild another HashSet here.
        for key in logical_keys {
            self.mutation_tracker.bump_key(self.db_index, self.mk(key));
        }
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

    pub(in crate::store::db) fn invalidate_hash_counter_cache_for_batch(&self, batch: &WriteBatch) {
        if self.store.is_transactional()
            || !self
                .counter_cache
                .hash_ever_populated
                .load(Ordering::Acquire)
        {
            return;
        }
        let mut logical_keys = HashSet::new();
        let mut field_keys = HashSet::new();
        let mut clear_db = false;
        for (write_type, raw_key, _) in batch.iter() {
            match write_type {
                WriteType::Put
                | WriteType::PutBlobMedium
                | WriteType::PutBlobExternal
                | WriteType::Delete
                | WriteType::Merge => {
                    if let Some(logical_key) =
                        logical_main_key_from_raw_key(self.key_layout, self.db_index, raw_key)
                    {
                        logical_keys.insert(logical_key);
                    } else if let Some(field_key) =
                        hash_field_key_from_raw_sub_key(self.key_layout, self.db_index, raw_key)
                    {
                        field_keys.insert(field_key);
                        if let Some(logical_key) =
                            hash_owner_from_raw_sub_key(self.key_layout, self.db_index, raw_key)
                        {
                            logical_keys.insert(logical_key);
                        }
                    }
                }
                WriteType::RangeDelete => clear_db = true,
            }
        }
        if clear_db {
            self.counter_cache.invalidate_db(self.db_index);
            return;
        }
        for logical_key in logical_keys {
            self.counter_cache
                .invalidate_hash_key(self.db_index, &logical_key);
        }
        for field_key in field_keys {
            self.counter_cache
                .invalidate_hash_field(self.db_index, &field_key);
        }
    }

    pub(in crate::store::db) fn invalidate_caches_for_committed_mutations(
        &self,
        keys: &[(u16, Vec<u8>)],
        dbs: &[u16],
    ) {
        if !self.counter_cache.ever_populated.load(Ordering::Acquire)
            && !self
                .counter_cache
                .hash_ever_populated
                .load(Ordering::Acquire)
            && !self
                .counter_cache
                .zset_ever_populated
                .load(Ordering::Acquire)
            && !self.list_meta_cache_maybe_non_empty.load(Ordering::Acquire)
        {
            return;
        }
        for &(db_index, ref key) in keys {
            self.counter_cache.invalidate_key(db_index, key);
            self.counter_cache.invalidate_hash_key(db_index, key);
            self.counter_cache.invalidate_zset_key(db_index, key);
            if db_index == self.db_index {
                self.list_meta_cache.remove(key);
            }
        }
        for &db_index in dbs {
            self.counter_cache.invalidate_db(db_index);
            if db_index == self.db_index {
                self.list_meta_cache.clear();
                self.list_meta_cache_maybe_non_empty
                    .store(false, Ordering::Release);
            }
        }
    }

    pub(in crate::store::db) fn invalidate_zset_length_cache_for_batch(&self, batch: &WriteBatch) {
        if self.store.is_transactional()
            || !self
                .counter_cache
                .zset_ever_populated
                .load(Ordering::Acquire)
        {
            return;
        }
        let mut logical_keys = HashSet::new();
        let mut clear_db = false;
        for (write_type, raw_key, _) in batch.iter() {
            match write_type {
                WriteType::Put
                | WriteType::PutBlobMedium
                | WriteType::PutBlobExternal
                | WriteType::Delete
                | WriteType::Merge => {
                    if let Some(logical_key) =
                        logical_main_key_from_raw_key(self.key_layout, self.db_index, raw_key)
                    {
                        logical_keys.insert(logical_key);
                    } else if let Some(logical_key) =
                        zset_owner_from_raw_sub_key(self.key_layout, self.db_index, raw_key)
                    {
                        logical_keys.insert(logical_key);
                    }
                }
                WriteType::RangeDelete => {
                    if self
                        .key_layout
                        .is_db_range_delete_start(self.db_index, raw_key)
                    {
                        clear_db = true;
                    } else if let Some(logical_key) =
                        zset_owner_from_raw_sub_key(self.key_layout, self.db_index, raw_key)
                    {
                        logical_keys.insert(logical_key);
                    }
                }
            }
        }
        if clear_db {
            self.counter_cache.zset_lengths.clear();
            self.counter_cache.zset_key_epochs.clear();
            self.counter_cache
                .zset_db_epochs
                .entry(self.db_index)
                .and_modify(|epoch| *epoch = epoch.wrapping_add(1))
                .or_insert(1);
            return;
        }
        for logical_key in logical_keys {
            self.counter_cache
                .invalidate_zset_key(self.db_index, &logical_key);
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
