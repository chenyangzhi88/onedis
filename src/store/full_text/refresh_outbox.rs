impl Db {
    fn fulltext_enqueue_mutation_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        source_type: FullTextSourceType,
        kind: FullTextMutationKind,
    ) -> Result<(), Error> {
        if self.store.is_transactional() {
            return Ok(());
        }
        for (index_name, meta) in self.fulltext_matching_metas_for_source(key, source_type)? {
            let seq = new_fulltext_sequence();
            let record = FullTextMutationRecord {
                generation: meta.generation,
                kind,
                key: key.to_string(),
            };
            batch.put(
                &fulltext_outbox_key(self.db_index, &index_name, seq),
                &encode_record(&record)?,
            );
        }
        Ok(())
    }

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

    fn fulltext_apply_pending(
        &self,
        index: &str,
        meta: &mut FullTextIndexMeta,
        runtime: &Arc<RwLock<FullTextRuntime>>,
        policy: &FullTextRefreshPolicy,
        deadline: Instant,
    ) -> Result<(), Error> {
        let mut changed = false;
        let mut indexed_docs = 0usize;
        let mut indexed_bytes = 0usize;
        let mut processed_outbox = Vec::new();
        let mut max_processed_seq = meta.last_indexed_outbox_seq;

        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            if matches!(
                meta.state,
                FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
            ) {
                let BackfillProgress {
                    finished,
                    cursor,
                    docs,
                    bytes,
                } = self.fulltext_apply_backfill_batch(
                    index,
                    &mut runtime,
                    meta,
                    policy,
                    deadline,
                )?;
                changed |= docs > 0;
                indexed_docs += docs;
                indexed_bytes += bytes;
                meta.backfill_cursor = cursor;
                if finished {
                    meta.state = FullTextIndexState::Ready;
                    meta.backfill_cursor = None;
                }
            }

            if indexed_docs < policy.max_docs
                && indexed_bytes < policy.max_bytes
                && Instant::now() < deadline
            {
                for (outbox_key, raw) in self
                    .store
                    .scan_prefix_raw(&fulltext_outbox_prefix(self.db_index, index))
                {
                    let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key)
                    else {
                        continue;
                    };
                    let record = decode_record::<FullTextMutationRecord>(&raw)?;
                    if record.generation == meta.generation {
                        match record.kind {
                            FullTextMutationKind::UpsertKey => {
                                if !matches!(meta.source_type, FullTextSourceType::Hash) {
                                    processed_outbox.push(outbox_key);
                                    continue;
                                }
                                let fields = self.hash_get_all(&record.key)?;
                                if fields.is_empty() {
                                    runtime.delete_hash(&record.key);
                                    self.fulltext_delete_vectors(index, meta, &record.key)?;
                                } else {
                                    indexed_bytes += runtime.upsert_hash(&record.key, &fields)?;
                                    self.fulltext_upsert_vectors(
                                        index,
                                        meta,
                                        &record.key,
                                        &fields,
                                    )?;
                                }
                            }
                            FullTextMutationKind::UpsertJson => {
                                if !matches!(meta.source_type, FullTextSourceType::Json) {
                                    processed_outbox.push(outbox_key);
                                    continue;
                                }
                                if let Some(fields) =
                                    self.fulltext_json_fields(&record.key, meta)?
                                {
                                    indexed_bytes += runtime.upsert_fields(&record.key, &fields)?;
                                    self.fulltext_upsert_vectors(
                                        index,
                                        meta,
                                        &record.key,
                                        &fields,
                                    )?;
                                } else {
                                    runtime.delete_hash(&record.key);
                                    self.fulltext_delete_vectors(index, meta, &record.key)?;
                                }
                            }
                            FullTextMutationKind::DeleteKey => {
                                runtime.delete_hash(&record.key);
                                self.fulltext_delete_vectors(index, meta, &record.key)?;
                            }
                        }
                        changed = true;
                        indexed_docs += 1;
                        max_processed_seq = max_processed_seq.max(seq);
                    }
                    processed_outbox.push(outbox_key);
                    if indexed_docs >= policy.max_docs
                        || indexed_bytes >= policy.max_bytes
                        || Instant::now() >= deadline
                    {
                        break;
                    }
                }
            }

            if changed {
                runtime.publish()?;
            } else {
                runtime.last_refresh_at = Instant::now();
            }
        }

        if changed
            || !processed_outbox.is_empty()
            || max_processed_seq != meta.last_indexed_outbox_seq
        {
            meta.last_indexed_outbox_seq = max_processed_seq;
            let mut batch = WriteBatch::new();
            for key in processed_outbox {
                batch.delete(&key);
            }
            batch.put(
                &fulltext_meta_key(self.db_index, index),
                &encode_record(meta)?,
            );
            self.write_batch_if_not_empty(&batch);
        }
        Ok(())
    }

    fn fulltext_apply_backfill_batch(
        &self,
        index: &str,
        runtime: &mut FullTextRuntime,
        meta: &FullTextIndexMeta,
        policy: &FullTextRefreshPolicy,
        deadline: Instant,
    ) -> Result<BackfillProgress, Error> {
        let mut docs = 0usize;
        let mut bytes = 0usize;
        let mut cursor = meta.backfill_cursor.clone();
        let mut seen = HashSet::new();
        let mut keys = Vec::new();
        for prefix in &meta.prefixes {
            for (raw_key, raw_value) in self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
            {
                if raw_key.len() < 2 {
                    continue;
                }
                let key = String::from_utf8_lossy(&raw_key[2..]).to_string();
                if !key.starts_with(prefix) || cursor.as_ref().is_some_and(|last| key <= *last) {
                    continue;
                }
                let matches_source = match meta.source_type {
                    FullTextSourceType::Hash => decode_hash_meta_checked(&raw_value).is_ok(),
                    FullTextSourceType::Json => Self::decode_json_meta(&raw_value).is_ok(),
                };
                if !matches_source || !seen.insert(key.clone()) {
                    continue;
                }
                keys.push(key);
            }
        }
        keys.sort();
        let finished = keys.len() <= policy.max_docs;
        for key in keys.into_iter().take(policy.max_docs) {
            if Instant::now() >= deadline {
                return Ok(BackfillProgress {
                    finished: false,
                    cursor,
                    docs,
                    bytes,
                });
            }
            match meta.source_type {
                FullTextSourceType::Hash => {
                    let fields = self.hash_get_all(&key)?;
                    if !fields.is_empty() {
                        bytes += runtime.upsert_hash(&key, &fields)?;
                        self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        docs += 1;
                    }
                }
                FullTextSourceType::Json => {
                    if let Some(fields) = self.fulltext_json_fields(&key, meta)? {
                        bytes += runtime.upsert_fields(&key, &fields)?;
                        self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        docs += 1;
                    }
                }
            }
            cursor = Some(key);
            if bytes >= policy.max_bytes {
                return Ok(BackfillProgress {
                    finished: false,
                    cursor,
                    docs,
                    bytes,
                });
            }
        }
        Ok(BackfillProgress {
            finished,
            cursor,
            docs,
            bytes,
        })
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

    fn fulltext_compact_outbox_if_needed(
        &self,
        index: &str,
        generation: u64,
        threshold: usize,
    ) -> Result<(), Error> {
        if threshold == 0 {
            return Ok(());
        }
        let entries = self
            .store
            .scan_prefix_raw(&fulltext_outbox_prefix(self.db_index, index));
        if entries.len() <= threshold {
            return Ok(());
        }
        let mut latest_by_key: HashMap<String, (u64, Vec<u8>)> = HashMap::new();
        let mut stale = Vec::new();
        for (outbox_key, raw) in entries {
            let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key) else {
                stale.push(outbox_key);
                continue;
            };
            let record = decode_record::<FullTextMutationRecord>(&raw)?;
            if record.generation != generation {
                stale.push(outbox_key);
                continue;
            }
            match latest_by_key.insert(record.key.clone(), (seq, outbox_key.clone())) {
                Some((previous_seq, previous_key)) if previous_seq < seq => {
                    stale.push(previous_key)
                }
                Some((previous_seq, previous_key)) => {
                    stale.push(outbox_key);
                    latest_by_key.insert(record.key, (previous_seq, previous_key));
                }
                None => {}
            }
        }
        if stale.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for key in stale {
            batch.delete(&key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(())
    }

}
