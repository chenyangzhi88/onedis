use super::*;
impl Db {
    pub(crate) fn fulltext_clear_runtimes_for_db(&self) {
        self.fulltext_runtimes.remove_db(self.db_index);
        if let Err(err) = delete_fulltext_aggregate_cursors_for_db(self.db_index) {
            log::error!(
                "failed to clear fulltext cursors for DB {}: {err}",
                self.db_index
            );
        }
    }

    pub(crate) fn fulltext_maintenance_tick(&self) -> Result<(), Error> {
        self.fulltext_maintenance_tick_mode(true)
    }

    fn fulltext_maintenance_tick_mode(&self, force_refresh: bool) -> Result<(), Error> {
        let snapshots = self.read_all_fulltext_metas()?;
        let mut first_error = None;
        for (index, snapshot) in snapshots {
            if let Err(error) =
                self.fulltext_maintain_index_snapshot_mode(&index, &snapshot, force_refresh)
                && first_error.is_none()
            {
                first_error = Some(Error::msg(format!("index {index}: {error}")));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn fulltext_maintain_index_snapshot(
        &self,
        index: &str,
        snapshot: &FullTextIndexMeta,
    ) -> Result<(), Error> {
        self.fulltext_maintain_index_snapshot_mode(index, snapshot, true)
    }

    fn fulltext_maintain_index_snapshot_mode(
        &self,
        index: &str,
        snapshot: &FullTextIndexMeta,
        force_refresh: bool,
    ) -> Result<(), Error> {
        if self.fulltext_index_expired(index, snapshot)
            || matches!(
                snapshot.state,
                FullTextIndexState::Creating
                    | FullTextIndexState::Dirty
                    | FullTextIndexState::Dropping
            )
        {
            return self.fulltext_maintain_index_exclusive(index, snapshot.incarnation);
        }

        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .read()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let Some((meta, _)) = self.read_fulltext_meta_versioned_optional(index)? else {
            return Ok(());
        };
        if meta.incarnation != snapshot.incarnation
            || self.fulltext_index_expired(index, &meta)
            || !matches!(
                meta.state,
                FullTextIndexState::Backfilling
                    | FullTextIndexState::Rebuilding
                    | FullTextIndexState::Ready
            )
        {
            return Ok(());
        }
        if self.fulltext_runtime_schema_needs_rebuild(index)? {
            drop(_lifecycle_guard);
            return self.fulltext_rebuild_index_snapshot(index, snapshot.incarnation);
        }
        let refresh_lock = self.fulltext_runtimes.refresh_lock(self.db_index, index);
        let _refresh_guard = refresh_lock
            .lock()
            .map_err(|_| Error::msg("ERR fulltext refresh lock poisoned"))?;
        let started = Instant::now();
        let result = self.fulltext_refresh_index_mode(index, force_refresh, None, true, false);
        global_metrics().record_fulltext_refresh(elapsed_us(started), result.is_err());
        result
    }

    fn fulltext_maintain_index_exclusive(
        &self,
        index: &str,
        expected_incarnation: u64,
    ) -> Result<(), Error> {
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let Some((meta, expected_raw)) = self.read_fulltext_meta_versioned_optional(index)? else {
            return Ok(());
        };
        if meta.incarnation != expected_incarnation {
            return Ok(());
        }
        if self.fulltext_index_expired(index, &meta)
            || matches!(meta.state, FullTextIndexState::Dropping)
        {
            return self.fulltext_purge_index_inner(index, &meta, &expected_raw);
        }
        match meta.state {
            FullTextIndexState::Creating => self.fulltext_recover_creating_index_inner(index),
            FullTextIndexState::Dirty => {
                let refresh_lock = self.fulltext_runtimes.refresh_lock(self.db_index, index);
                let _refresh_guard = refresh_lock
                    .lock()
                    .map_err(|_| Error::msg("ERR fulltext refresh lock poisoned"))?;
                let started = Instant::now();
                let result = self.fulltext_refresh_index_mode(index, true, None, true, true);
                global_metrics().record_fulltext_refresh(elapsed_us(started), result.is_err());
                result
            }
            FullTextIndexState::Backfilling
            | FullTextIndexState::Rebuilding
            | FullTextIndexState::Ready
            | FullTextIndexState::Dropping => Ok(()),
        }
    }

    fn fulltext_rebuild_index_snapshot(
        &self,
        index: &str,
        expected_incarnation: u64,
    ) -> Result<(), Error> {
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let Some((meta, _)) = self.read_fulltext_meta_versioned_optional(index)? else {
            return Ok(());
        };
        if meta.incarnation != expected_incarnation
            || !matches!(
                meta.state,
                FullTextIndexState::Backfilling
                    | FullTextIndexState::Rebuilding
                    | FullTextIndexState::Ready
            )
            || !self.fulltext_runtime_schema_needs_rebuild(index)?
        {
            return Ok(());
        }
        let started = Instant::now();
        let result = self.fulltext_rebuild_index(index);
        global_metrics().record_fulltext_refresh(elapsed_us(started), result.is_err());
        result
    }

    pub(crate) async fn fulltext_maintenance_tick_async(&self) -> Result<(), Error> {
        self.run_blocking_store_task(|db| db.fulltext_maintenance_tick_mode(false))
            .await
    }

    pub(crate) fn fulltext_request_refresh(&self, key: &str) -> Result<(), Error> {
        self.fulltext_request_refresh_for_source(key, FullTextSourceType::Hash)
    }

    pub(crate) fn fulltext_request_json_refresh(&self, key: &str) -> Result<(), Error> {
        self.fulltext_request_refresh_for_source(key, FullTextSourceType::Json)
    }

    pub(crate) fn fulltext_reconcile_committed_keys(
        &self,
        raw_keys: &[Vec<u8>],
        refresh_immediately: bool,
    ) -> Result<(), Error> {
        let mut keys = HashSet::new();
        for raw_key in raw_keys {
            let Ok(key) = String::from_utf8(raw_key.clone()) else {
                continue;
            };
            keys.insert(key);
        }
        for key in keys {
            let mut batch = WriteBatch::new();
            match self
                .store
                .get_raw(&self.mk(&key))
                .and_then(|raw| decode_meta_header(&raw))
                .map(|header| header.type_tag)
            {
                Some(TYPE_HASH) => {
                    self.fulltext_enqueue_hash_upsert_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_json_delete_to_batch(&mut batch, &key)?;
                }
                Some(TYPE_JSON) => {
                    self.fulltext_enqueue_json_upsert_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_hash_delete_to_batch(&mut batch, &key)?;
                }
                _ => {
                    self.fulltext_enqueue_hash_delete_to_batch(&mut batch, &key)?;
                    self.fulltext_enqueue_json_delete_to_batch(&mut batch, &key)?;
                }
            }
            if batch.count() > 0 {
                self.store.write_batch_direct(&batch);
                self.fulltext_observe_committed_outbox_batch(&batch);
                if refresh_immediately {
                    self.fulltext_request_refresh(&key)?;
                    self.fulltext_request_json_refresh(&key)?;
                }
            }
        }
        Ok(())
    }
}
