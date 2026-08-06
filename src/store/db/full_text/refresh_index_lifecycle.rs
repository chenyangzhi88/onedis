use super::*;
impl Db {
    pub(crate) fn shutdown_fulltext_runtime(&self) {
        self.fulltext_runtimes.remove_db(self.db_index);
        if let Err(err) = delete_fulltext_aggregate_cursors_for_db(self.db_index) {
            log::error!(
                "failed to clear fulltext cursors while shutting down DB {}: {err}",
                self.db_index
            );
        }
    }

    pub(super) fn fulltext_refresh_index(&self, index: &str, force: bool) -> Result<(), Error> {
        let started = Instant::now();
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let result = self.fulltext_refresh_index_inner(index, force, None);
        global_metrics().record_fulltext_refresh(elapsed_us(started), result.is_err());
        result
    }

    pub(super) fn fulltext_refresh_index_inner(
        &self,
        index: &str,
        force: bool,
        external_deadline: Option<Instant>,
    ) -> Result<(), Error> {
        let (mut meta, expected_meta_raw) = self.read_fulltext_meta_versioned(index)?;
        if matches!(meta.state, FullTextIndexState::Dropping) {
            return Ok(());
        }
        if matches!(meta.state, FullTextIndexState::Dirty) {
            if force && self.fulltext_dirty_repair_allowed(index)? {
                return self.fulltext_rebuild_index(index);
            }
            return Ok(());
        }
        self.ensure_fulltext_runtime(index)?;
        let Some(runtime) = self.fulltext_runtimes.get(self.db_index, index) else {
            return Ok(());
        };
        let policy = self.fulltext_effective_refresh_policy(&meta)?;
        {
            let runtime_guard = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            if !force && !runtime_guard.refresh_due(&policy) {
                return Ok(());
            }
        }

        let threshold = self.fulltext_outbox_compact_threshold()?;
        self.fulltext_compact_outbox_if_needed(index, threshold)?;
        let refresh_timeout_ms = self.fulltext_refresh_timeout_ms()?;
        let deadline = match (external_deadline, refresh_timeout_ms) {
            (_, 0) => Instant::now(),
            (Some(deadline), _) => deadline,
            (None, timeout_ms) => {
                let now = Instant::now();
                now.checked_add(Duration::from_millis(timeout_ms))
                    .unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 60 * 60))
            }
        };
        let result = self.fulltext_apply_pending(
            index,
            &mut meta,
            &expected_meta_raw,
            &runtime,
            &policy,
            deadline,
        );
        if let Err(err) = result {
            self.fulltext_mark_dirty(index)?;
            self.fulltext_runtimes.remove(self.db_index, index);
            return Err(err);
        }
        Ok(())
    }

    pub(super) fn fulltext_refresh_index_until_caught_up(
        &self,
        index: &str,
        deadline: Instant,
    ) -> Result<bool, Error> {
        self.ensure_fulltext_runtime(index)?;
        loop {
            let before = self.fulltext_refresh_progress(index)?;
            if before.0 {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            let started = Instant::now();
            let result = self.fulltext_refresh_index_inner(index, true, Some(deadline));
            global_metrics().record_fulltext_refresh(elapsed_us(started), result.is_err());
            result?;
            let after = self.fulltext_refresh_progress(index)?;
            if after.0 {
                return Ok(true);
            }
            if after == before || Instant::now() >= deadline {
                return Ok(false);
            }
        }
    }

    pub(super) fn fulltext_refresh_progress(
        &self,
        index: &str,
    ) -> Result<(bool, usize, Option<String>), Error> {
        let meta = self.read_fulltext_meta_direct(index)?;
        let pending = self.fulltext_pending_outbox_count(index) as usize;
        let complete = pending == 0
            && !matches!(
                meta.state,
                FullTextIndexState::Backfilling
                    | FullTextIndexState::Rebuilding
                    | FullTextIndexState::Dirty
            );
        Ok((complete, pending, meta.backfill_cursor))
    }

    pub(super) fn fulltext_rebuild_index(&self, index: &str) -> Result<(), Error> {
        let (old_meta, expected_meta_raw) = self.read_fulltext_meta_versioned(index)?;
        let previous_storage = self.fulltext_active_storage_name(index, &old_meta);
        let mut meta = old_meta.clone();
        meta.state = FullTextIndexState::Rebuilding;
        meta.generation = self.next_fulltext_sequence();
        meta.active_storage = fulltext_generation_storage_name(index, meta.generation);
        meta.backfill_cursor = None;
        meta.last_indexed_outbox_seq = 0;
        meta.indexed_docs = 0;
        meta.indexed_bytes = 0;

        let stage_result = (|| {
            self.fulltext_create_vector_indexes(index, &meta)?;
            let runtime_config = self.fulltext_runtime_config()?;
            let mut runtime = FullTextRuntime::new(
                self.store.clone(),
                self.db_index,
                index,
                &meta.active_storage,
                &meta,
                &runtime_config,
            )?;
            self.fulltext_build_generation(index, &meta, &mut runtime)?;
            Ok::<FullTextRuntime, Error>(runtime)
        })();
        let runtime = match stage_result {
            Ok(runtime) => runtime,
            Err(error) => {
                self.fulltext_cleanup_generation(index, &meta);
                return Err(error);
            }
        };
        meta.indexed_docs = runtime.num_docs();
        meta.indexed_bytes = self.fulltext_file_bytes(&meta.active_storage) as u64;
        meta.state = FullTextIndexState::Ready;
        let mut swap_batch = WriteBatch::new();
        if let Err(error) =
            self.fulltext_write_meta_cas(index, &expected_meta_raw, &mut meta, &mut swap_batch)
        {
            self.fulltext_cleanup_generation(index, &meta);
            return Err(error);
        }
        self.fulltext_runtimes.insert(self.db_index, index, runtime);
        self.fulltext_invalidate_source_routes();

        if previous_storage != meta.active_storage {
            let mut cleanup = WriteBatch::new();
            self.delete_fulltext_storage_to_batch(&mut cleanup, &previous_storage);
            self.write_batch_if_not_empty(&cleanup);
        }
        self.fulltext_delete_vector_indexes(index, &old_meta);
        self.fulltext_refresh_index_inner(index, true, None)
    }

    pub(super) fn fulltext_recover_creating_index(&self, index: &str) -> Result<(), Error> {
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let (mut meta, expected_raw) = self.read_fulltext_meta_versioned(index)?;
        if !matches!(meta.state, FullTextIndexState::Creating) {
            return Ok(());
        }
        self.fulltext_create_vector_indexes(index, &meta)?;
        self.fulltext_runtimes.remove(self.db_index, index);
        self.ensure_fulltext_runtime(index)?;
        meta.state = if meta.index_options.skip_initial_scan {
            FullTextIndexState::Ready
        } else {
            FullTextIndexState::Backfilling
        };
        let mut batch = WriteBatch::new();
        self.fulltext_write_meta_cas(index, &expected_raw, &mut meta, &mut batch)?;
        self.fulltext_invalidate_source_routes();
        Ok(())
    }

    pub(super) fn ensure_fulltext_runtime(&self, index: &str) -> Result<(), Error> {
        if self.fulltext_runtimes.get(self.db_index, index).is_some() {
            return Ok(());
        }
        let meta = self.read_fulltext_meta_direct(index)?;
        self.fulltext_create_vector_indexes(index, &meta)?;
        let storage_name = self.fulltext_active_storage_name(index, &meta);
        let runtime_config = self.fulltext_runtime_config()?;
        let directory = KvTantivyDirectory::new(self.store.clone(), self.db_index, &storage_name);
        if directory.remove_stale_writer_lock()? {
            log::warn!(
                "removed stale fulltext writer lock db={} index={} storage={}",
                self.db_index,
                index,
                storage_name
            );
        }
        let runtime = FullTextRuntime::new(
            self.store.clone(),
            self.db_index,
            index,
            &storage_name,
            &meta,
            &runtime_config,
        )?;
        self.fulltext_runtimes.insert(self.db_index, index, runtime);
        Ok(())
    }

    pub(super) fn fulltext_mark_dirty(&self, index: &str) -> Result<(), Error> {
        let (mut meta, expected_meta_raw) = self.read_fulltext_meta_versioned(index)?;
        meta.state = FullTextIndexState::Dirty;
        let mut batch = WriteBatch::new();
        self.fulltext_write_meta_cas(index, &expected_meta_raw, &mut meta, &mut batch)?;
        Ok(())
    }

    pub(super) fn fulltext_dirty_repair_allowed(&self, index: &str) -> Result<bool, Error> {
        let now = current_fulltext_millis();
        let throttle_ms = self.fulltext_repair_throttle_ms()?;
        let marker = fulltext_repair_marker_key(self.db_index, index);
        if let Some(raw) = self.store.get_raw(&marker)
            && let Ok(value) = String::from_utf8(raw)
            && let Ok(previous) = value.parse::<u64>()
            && now.saturating_sub(previous) < throttle_ms
        {
            return Ok(false);
        }
        let mut batch = WriteBatch::new();
        batch.put(&marker, now.to_string().as_bytes());
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }
}
