use super::*;
impl Db {
    pub(crate) fn shutdown_fulltext_runtime(&self) {
        if let Err(err) = self.fulltext_maintenance_tick() {
            log::error!(
                "failed to checkpoint fulltext runtimes while shutting down DB {}: {err}",
                self.db_index
            );
        }
        self.fulltext_runtimes.remove_db(self.db_index);
        if let Err(err) = delete_fulltext_aggregate_cursors_for_db(self.db_index) {
            log::error!(
                "failed to clear fulltext cursors while shutting down DB {}: {err}",
                self.db_index
            );
        }
    }

    pub(super) fn fulltext_runtime_schema_needs_rebuild(&self, index: &str) -> Result<bool, Error> {
        if self.fulltext_runtimes.get(self.db_index, index).is_some() {
            return Ok(false);
        }
        let meta = self.read_fulltext_meta_direct(index)?;
        let storage_name = self.fulltext_active_storage_name(index, &meta);
        let directory = KvTantivyDirectory::new(self.store.clone(), self.db_index, &storage_name);
        if !Index::exists(&directory)? {
            return Ok(false);
        }
        let persisted = Index::open(directory)?;
        let schema = persisted.schema();
        let missing_expiration = schema.get_field(FULLTEXT_EXPIRES_AT_FIELD).is_err();
        let key_is_not_fast = schema
            .get_field(FULLTEXT_KEY_FIELD)
            .ok()
            .is_none_or(|field| !schema.get_field_entry(field).is_fast());
        let geo_is_not_fast = meta.schema.iter().enumerate().any(|(offset, field)| {
            matches!(field.kind, FullTextFieldKind::Geo)
                && !field.options.noindex
                && ["lon", "lat"].into_iter().any(|axis| {
                    schema
                        .get_field(&format!("{FULLTEXT_GEO_FIELD_PREFIX}{offset}_{axis}"))
                        .ok()
                        .is_none_or(|field| !schema.get_field_entry(field).is_fast())
                })
        });
        Ok(missing_expiration || key_is_not_fast || geo_is_not_fast)
    }

    pub(super) fn fulltext_refresh_index_inner(
        &self,
        index: &str,
        force: bool,
        external_deadline: Option<Instant>,
    ) -> Result<(), Error> {
        self.fulltext_refresh_index_mode(index, force, external_deadline, true, true)
    }

    fn fulltext_publish_index_inner(
        &self,
        index: &str,
        force: bool,
        external_deadline: Option<Instant>,
    ) -> Result<(), Error> {
        self.fulltext_refresh_index_mode(index, force, external_deadline, false, false)
    }

    pub(super) fn fulltext_refresh_index_mode(
        &self,
        index: &str,
        force: bool,
        external_deadline: Option<Instant>,
        durable_checkpoint: bool,
        repair_dirty: bool,
    ) -> Result<(), Error> {
        let (mut meta, expected_meta_raw) = self.read_fulltext_meta_versioned(index)?;
        if matches!(meta.state, FullTextIndexState::Dropping) {
            return Ok(());
        }
        if matches!(meta.state, FullTextIndexState::Dirty) {
            if repair_dirty && force && self.fulltext_dirty_repair_allowed(index)? {
                return self.fulltext_rebuild_index(index);
            }
            return Ok(());
        }
        self.ensure_fulltext_runtime(index)?;
        let Some(runtime) = self.fulltext_runtimes.get(self.db_index, index) else {
            return Ok(());
        };
        let policy = self.fulltext_effective_refresh_policy(&meta)?;
        let checkpoint_interval_ms = self.fulltext_checkpoint_interval_ms()?;
        let durable_checkpoint = if durable_checkpoint {
            let runtime_guard = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            force
                || checkpoint_interval_ms == 0
                || runtime_guard.last_checkpoint_at.elapsed()
                    >= Duration::from_millis(checkpoint_interval_ms)
        } else {
            false
        };
        {
            let runtime_guard = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            if !force && !runtime_guard.refresh_due(&policy) {
                return Ok(());
            }
        }

        let threshold = self.fulltext_outbox_compact_threshold()?;
        if self
            .fulltext_runtimes
            .take_outbox_compaction_due(self.db_index, index, threshold)
        {
            self.fulltext_compact_outbox_if_needed(index, threshold)?;
        }
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
            durable_checkpoint,
        );
        if let Err(err) = result {
            if self.fulltext_mark_dirty_if_incarnation(index, meta.incarnation)? {
                self.fulltext_runtimes.remove_if_incarnation(
                    self.db_index,
                    index,
                    meta.incarnation,
                );
            }
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
        if self.fulltext_refresh_progress(index)?.0 {
            return Ok(true);
        }
        let refresh_lock = self.fulltext_runtimes.refresh_lock(self.db_index, index);
        let _refresh_guard = refresh_lock
            .lock()
            .map_err(|_| Error::msg("ERR fulltext refresh lock poisoned"))?;
        loop {
            let before = self.fulltext_refresh_progress(index)?;
            if before.0 {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            let started = Instant::now();
            let result = self.fulltext_publish_index_inner(index, true, Some(deadline));
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
        let runtime = self
            .fulltext_runtimes
            .get(self.db_index, index)
            .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
        let published_seq = runtime.published_outbox_seq();
        let latest = self.fulltext_latest_outbox_seq(index).unwrap_or(0);
        let pending = latest.saturating_sub(published_seq).min(usize::MAX as u64) as usize;
        let complete = pending == 0 && runtime.backfill_complete;
        Ok((complete, pending, runtime.published_backfill_cursor.clone()))
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
            runtime.directory.checkpoint()?;
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
            self.delete_fulltext_storage_to_batch(&mut cleanup, &previous_storage)?;
            self.write_batch_if_not_empty(&cleanup);
        }
        self.fulltext_delete_vector_indexes(index, &old_meta);
        self.fulltext_refresh_index_inner(index, true, None)
    }

    pub(super) fn fulltext_recover_creating_index_inner(&self, index: &str) -> Result<(), Error> {
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
        if matches!(meta.state, FullTextIndexState::Backfilling)
            && let Some(runtime) = self.fulltext_runtimes.get(self.db_index, index)
        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            runtime.backfill_complete = false;
            runtime.published_backfill_cursor = None;
        }
        self.fulltext_invalidate_source_routes();
        Ok(())
    }

    pub(super) fn ensure_fulltext_runtime(&self, index: &str) -> Result<(), Error> {
        if self.fulltext_runtimes.get(self.db_index, index).is_some() {
            return Ok(());
        }
        self.fulltext_runtimes
            .get_or_try_insert(self.db_index, index, || {
                let meta = self.read_fulltext_meta_direct(index)?;
                self.fulltext_create_vector_indexes(index, &meta)?;
                let storage_name = self.fulltext_active_storage_name(index, &meta);
                let runtime_config = self.fulltext_runtime_config()?;
                let directory =
                    KvTantivyDirectory::new(self.store.clone(), self.db_index, &storage_name);
                if directory.remove_stale_writer_lock()? {
                    log::warn!(
                        "removed stale fulltext writer lock db={} index={} storage={}",
                        self.db_index,
                        index,
                        storage_name
                    );
                }
                FullTextRuntime::new(
                    self.store.clone(),
                    self.db_index,
                    index,
                    &storage_name,
                    &meta,
                    &runtime_config,
                )
            })?;
        Ok(())
    }

    pub(super) fn fulltext_mark_dirty_if_incarnation(
        &self,
        index: &str,
        incarnation: u64,
    ) -> Result<bool, Error> {
        let Some((mut meta, expected_meta_raw)) =
            self.read_fulltext_meta_versioned_optional(index)?
        else {
            return Ok(false);
        };
        if meta.incarnation != incarnation {
            return Ok(false);
        }
        meta.state = FullTextIndexState::Dirty;
        let mut batch = WriteBatch::new();
        self.fulltext_write_meta_cas(index, &expected_meta_raw, &mut meta, &mut batch)?;
        Ok(true)
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
        batch
            .put(&marker, now.to_string().as_bytes())
            .map_err(|error| Error::msg(error.to_string()))?;
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }
}
