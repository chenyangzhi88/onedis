impl Db {
    fn fulltext_refresh_index(&self, index: &str, force: bool) -> Result<(), Error> {
        let mut meta = self.read_fulltext_meta_direct(index)?;
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
        self.fulltext_compact_outbox_if_needed(index, meta.generation, threshold)?;
        let deadline = Instant::now() + Duration::from_millis(self.fulltext_refresh_timeout_ms()?);
        let result = self.fulltext_apply_pending(index, &mut meta, &runtime, &policy, deadline);
        if let Err(err) = result {
            self.fulltext_mark_dirty(index)?;
            self.fulltext_runtimes.remove(self.db_index, index);
            return Err(err);
        }
        Ok(())
    }

    fn fulltext_rebuild_index(&self, index: &str) -> Result<(), Error> {
        let mut meta = self.read_fulltext_meta_direct(index)?;
        meta.state = FullTextIndexState::Rebuilding;
        meta.generation = new_fulltext_sequence();
        meta.backfill_cursor = None;
        meta.last_indexed_outbox_seq = 0;

        let mut batch = WriteBatch::new();
        self.delete_fulltext_index_storage_to_batch(&mut batch, index);
        batch.put(
            &fulltext_meta_key(self.db_index, index),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);

        self.fulltext_delete_vector_indexes(index, &meta);
        self.fulltext_create_vector_indexes(index, &meta)?;
        self.fulltext_runtimes.remove(self.db_index, index);
        self.ensure_fulltext_runtime(index)?;
        self.fulltext_refresh_index(index, true)
    }

    fn ensure_fulltext_runtime(&self, index: &str) -> Result<(), Error> {
        if self.fulltext_runtimes.get(self.db_index, index).is_some() {
            return Ok(());
        }
        let meta = self.read_fulltext_meta_direct(index)?;
        self.fulltext_create_vector_indexes(index, &meta)?;
        let runtime = FullTextRuntime::new(self.store.clone(), self.db_index, index, &meta)?;
        self.fulltext_runtimes.insert(self.db_index, index, runtime);
        Ok(())
    }

    fn fulltext_mark_dirty(&self, index: &str) -> Result<(), Error> {
        let mut meta = self.read_fulltext_meta_direct(index)?;
        meta.state = FullTextIndexState::Dirty;
        let mut batch = WriteBatch::new();
        batch.put(
            &fulltext_meta_key(self.db_index, index),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        Ok(())
    }

    fn fulltext_dirty_repair_allowed(&self, index: &str) -> Result<bool, Error> {
        let now = current_fulltext_millis();
        let throttle_ms = self.fulltext_repair_throttle_ms()?;
        let marker = fulltext_repair_marker_key(self.db_index, index);
        if let Some(raw) = self.store.get_raw(&marker)
            && let Ok(value) = String::from_utf8(raw)
            && let Ok(previous) = value.parse::<u64>()
        {
            if now.saturating_sub(previous) < throttle_ms {
                return Ok(false);
            }
        }
        let mut batch = WriteBatch::new();
        batch.put(&marker, now.to_string().as_bytes());
        self.write_batch_if_not_empty(&batch);
        Ok(true)
    }
}
