use super::*;
impl Db {
    pub fn fulltext_create(
        &self,
        index: &str,
        options: FullTextCreateOptions,
    ) -> Result<(), Error> {
        validate_fulltext_identifier(index, "index")?;
        validate_fulltext_create(&options)?;
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        if self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, index))
            .is_some()
            || self.read_fulltext_alias(index)?.is_some()
        {
            return Err(Error::msg("ERR fulltext index already exists"));
        }
        let generation = self.next_fulltext_sequence();
        let active_storage = fulltext_generation_storage_name(index, generation);
        let mut meta = FullTextIndexMeta {
            source_type: options.source_type,
            prefixes: options.prefixes,
            schema: options.schema,
            aliases: Vec::new(),
            index_options: options.index_options,
            state: FullTextIndexState::Creating,
            incarnation: generation,
            generation,
            revision: 1,
            active_storage,
            backfill_cursor: None,
            last_indexed_outbox_seq: 0,
            indexed_docs: 0,
            indexed_bytes: 0,
            refresh_policy: FullTextRefreshPolicy::default(),
        };
        let mut batch = WriteBatch::new();
        batch
            .put(
                &fulltext_meta_key(self.db_index, index),
                &encode_record(&meta)?,
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        if meta.index_options.temporary_seconds.is_some() {
            batch
                .put(
                    &fulltext_temporary_activity_key(self.db_index, index),
                    &current_fulltext_millis().to_be_bytes(),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let encoded_creating = encode_record(&meta)?;
        self.fulltext_compare_and_write(index, None, &batch)?;
        self.fulltext_invalidate_source_routes();

        let setup_result = (|| {
            self.fulltext_create_vector_indexes(index, &meta)?;
            self.fulltext_runtimes.remove(self.db_index, index);
            self.ensure_fulltext_runtime(index)?;
            Ok::<(), Error>(())
        })();
        if let Err(error) = setup_result {
            let _ = self.fulltext_purge_index_inner(index, &meta, &encoded_creating);
            return Err(error);
        }

        meta.state = if meta.index_options.skip_initial_scan {
            FullTextIndexState::Ready
        } else {
            FullTextIndexState::Backfilling
        };
        let mut ready_batch = WriteBatch::new();
        if let Err(error) =
            self.fulltext_write_meta_cas(index, &encoded_creating, &mut meta, &mut ready_batch)
        {
            let generation_is_still_published = self
                .read_fulltext_meta_direct(index)
                .is_ok_and(|current| current.generation == meta.generation);
            if !generation_is_still_published {
                self.fulltext_cleanup_generation(index, &meta);
                self.fulltext_runtimes.remove(self.db_index, index);
            }
            return Err(error);
        }
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

    pub async fn fulltext_create_async(
        &self,
        index: &str,
        options: FullTextCreateOptions,
    ) -> Result<(), Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_create(&index, options))
            .await
    }

    pub fn fulltext_list(&self) -> Result<Frame, Error> {
        let mut names = Vec::new();
        for (index, meta) in self.read_all_fulltext_metas()? {
            if self.fulltext_index_expired(&index, &meta) {
                self.fulltext_purge_index(&index, &meta)?;
            } else {
                names.push(index);
            }
        }
        names.sort();
        Ok(Frame::Array(
            names.into_iter().map(Frame::bulk_string).collect(),
        ))
    }

    pub async fn fulltext_list_async(&self) -> Result<Frame, Error> {
        self.run_blocking_store_task(|db| db.fulltext_list()).await
    }

    pub fn fulltext_drop_index(&self, index: &str, delete_documents: bool) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, &index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let (mut meta, expected_raw) = self.read_fulltext_meta_versioned(&index)?;
        meta.state = FullTextIndexState::Dropping;
        let mut dropping_batch = WriteBatch::new();
        let dropping_raw =
            self.fulltext_write_meta_cas(&index, &expected_raw, &mut meta, &mut dropping_batch)?;
        self.fulltext_invalidate_source_routes();
        self.fulltext_runtimes.remove(self.db_index, &index);
        if delete_documents {
            let mut cursor = None;
            loop {
                let (keys, has_more) =
                    self.fulltext_source_keys_page(&meta, cursor.as_deref(), 256)?;
                if let Some(last) = keys.last() {
                    cursor = Some(last.clone());
                }
                self.delete_keys_internal_batch(&keys)?;
                if !has_more {
                    break;
                }
                if cursor.is_none() {
                    return Err(Error::msg("ERR fulltext source scan made no progress"));
                }
            }
        }

        self.fulltext_purge_index_inner(&index, &meta, &dropping_raw)?;
        Ok(Frame::Ok)
    }

    pub(super) fn fulltext_purge_index(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<(), Error> {
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let (persisted, expected_raw) = self.read_fulltext_meta_versioned(index)?;
        let effective_meta = if persisted.generation == meta.generation {
            persisted
        } else {
            return Err(Error::msg("ERR fulltext index changed concurrently"));
        };
        self.fulltext_purge_index_inner(index, &effective_meta, &expected_raw)
    }

    pub(super) fn fulltext_purge_index_inner(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        expected_raw: &[u8],
    ) -> Result<(), Error> {
        let active_storage = self.fulltext_active_storage_name(index, meta);
        let mut batch = WriteBatch::new();
        batch
            .delete(&fulltext_meta_key(self.db_index, index))
            .map_err(|error| Error::msg(error.to_string()))?;
        batch
            .delete(&fulltext_temporary_activity_key(self.db_index, index))
            .map_err(|error| Error::msg(error.to_string()))?;
        batch
            .delete(&fulltext_outbox_latest_key(self.db_index, index))
            .map_err(|error| Error::msg(error.to_string()))?;
        for alias in self.fulltext_aliases_for_index(index)? {
            batch
                .delete(&fulltext_alias_key(self.db_index, &alias))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        self.delete_fulltext_index_storage_to_batch(&mut batch, index)?;
        if active_storage != index {
            self.delete_fulltext_storage_to_batch(&mut batch, &active_storage)?;
        }
        self.fulltext_compare_and_write(index, Some(expected_raw), &batch)?;
        self.fulltext_delete_vector_indexes(index, meta);
        self.fulltext_runtimes.remove(self.db_index, index);
        self.fulltext_runtimes
            .clear_outbox_pending(self.db_index, index);
        self.fulltext_invalidate_source_routes();
        if let Err(error) = delete_fulltext_aggregate_cursors_for_index(self.db_index, index) {
            log::warn!(
                "failed to clean aggregate cursors after dropping full-text index {index}: {error}"
            );
        }
        Ok(())
    }

    pub async fn fulltext_drop_index_async(
        &self,
        index: &str,
        delete_documents: bool,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_drop_index(&index, delete_documents))
            .await
    }

    pub fn fulltext_alter(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
    ) -> Result<Frame, Error> {
        self.fulltext_alter_with_options(index, fields, false)
    }

    pub fn fulltext_alter_with_options(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
        skip_initial_scan: bool,
    ) -> Result<Frame, Error> {
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid fulltext schema"));
        }
        let index = self.resolve_fulltext_index(index)?;
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, &index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        self.fulltext_refresh_index_inner(&index, true, None)?;
        let (old_meta, expected_raw) = self.read_fulltext_meta_versioned(&index)?;
        let old_storage = self.fulltext_active_storage_name(&index, &old_meta);
        let mut merged = old_meta.schema.clone();
        merged.extend(fields);
        let validation_options = FullTextCreateOptions {
            source_type: old_meta.source_type,
            prefixes: old_meta.prefixes.clone(),
            schema: merged.clone(),
            index_options: old_meta.index_options.clone(),
        };
        validate_fulltext_create(&validation_options)?;
        let mut meta = old_meta.clone();
        meta.schema = merged;
        meta.state = FullTextIndexState::Rebuilding;
        meta.generation = self.next_fulltext_sequence();
        meta.active_storage = fulltext_generation_storage_name(&index, meta.generation);
        meta.backfill_cursor = None;
        meta.last_indexed_outbox_seq = 0;
        meta.indexed_docs = 0;
        meta.indexed_bytes = 0;

        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            fulltext_vector_create_options(field)?;
        }

        let staged_storage = meta.active_storage.clone();
        let mut cleanup_batch = WriteBatch::new();
        self.delete_fulltext_storage_to_batch(&mut cleanup_batch, &staged_storage)?;
        self.write_batch_if_not_empty(&cleanup_batch);
        let stage_result = (|| {
            self.fulltext_create_vector_indexes(&index, &meta)?;
            let runtime_config = self.fulltext_runtime_config()?;
            let mut staged_runtime = FullTextRuntime::new(
                self.store.clone(),
                self.db_index,
                &index,
                &staged_storage,
                &meta,
                &runtime_config,
            )?;
            if skip_initial_scan {
                self.fulltext_build_generation_from_existing(
                    &index,
                    &old_meta,
                    &meta,
                    &mut staged_runtime,
                )?;
            } else {
                self.fulltext_build_generation(&index, &meta, &mut staged_runtime)?;
            }
            staged_runtime.directory.checkpoint()?;
            Ok::<FullTextRuntime, Error>(staged_runtime)
        })();
        let staged_runtime = match stage_result {
            Ok(runtime) => runtime,
            Err(error) => {
                self.fulltext_cleanup_generation(&index, &meta);
                return Err(error);
            }
        };
        meta.indexed_docs = staged_runtime.num_docs();
        meta.indexed_bytes = self.fulltext_file_bytes(&staged_storage) as u64;
        meta.state = FullTextIndexState::Ready;

        #[cfg(test)]
        if FULLTEXT_ALTER_FAIL_AFTER_SWAP.swap(false, AtomicOrdering::SeqCst) {
            self.fulltext_cleanup_generation(&index, &meta);
            return Err(Error::msg("ERR injected FT.ALTER runtime failure"));
        }

        let mut swap_batch = WriteBatch::new();
        if let Err(error) =
            self.fulltext_write_meta_cas(&index, &expected_raw, &mut meta, &mut swap_batch)
        {
            self.fulltext_cleanup_generation(&index, &meta);
            return Err(error);
        }
        self.fulltext_runtimes
            .insert(self.db_index, &index, staged_runtime);
        self.fulltext_invalidate_source_routes();

        if old_storage != staged_storage {
            let mut cleanup = WriteBatch::new();
            self.delete_fulltext_storage_to_batch(&mut cleanup, &old_storage)?;
            self.write_batch_if_not_empty(&cleanup);
        }
        self.fulltext_delete_vector_indexes(&index, &old_meta);
        self.fulltext_refresh_index_inner(&index, true, None)?;
        Ok(Frame::Ok)
    }

    pub(super) fn fulltext_cleanup_generation(&self, index: &str, meta: &FullTextIndexMeta) {
        let mut batch = WriteBatch::new();
        if let Err(error) = self.delete_fulltext_storage_to_batch(&mut batch, &meta.active_storage)
        {
            log::warn!(
                "failed to plan fulltext generation cleanup db={} index={index}: {error}",
                self.db_index
            );
            return;
        }
        self.write_batch_if_not_empty(&batch);
        self.fulltext_delete_vector_indexes(index, meta);
    }

    pub async fn fulltext_alter_async(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
    ) -> Result<Frame, Error> {
        self.fulltext_alter_with_options_async(index, fields, false)
            .await
    }

    pub async fn fulltext_alter_with_options_async(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
        skip_initial_scan: bool,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| {
            db.fulltext_alter_with_options(&index, fields, skip_initial_scan)
        })
        .await
    }

    pub fn fulltext_alias_add(&self, alias: &str, index: &str) -> Result<Frame, Error> {
        if self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, alias))
            .is_some()
            || self.read_fulltext_alias(alias)?.is_some()
        {
            return Err(Error::msg("ERR alias already exists"));
        }
        self.fulltext_alias_set(alias, index, false)
    }

    pub async fn fulltext_alias_add_async(&self, alias: &str, index: &str) -> Result<Frame, Error> {
        let alias = alias.to_string();
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_alias_add(&alias, &index))
            .await
    }

    pub fn fulltext_alias_update(&self, alias: &str, index: &str) -> Result<Frame, Error> {
        self.fulltext_alias_set(alias, index, true)
    }

    pub async fn fulltext_alias_update_async(
        &self,
        alias: &str,
        index: &str,
    ) -> Result<Frame, Error> {
        let alias = alias.to_string();
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_alias_update(&alias, &index))
            .await
    }

    pub fn fulltext_alias_del(&self, alias: &str) -> Result<Frame, Error> {
        validate_fulltext_identifier(alias, "alias")?;
        let alias_lock_name = format!("__alias__:{alias}");
        let alias_lock = self
            .fulltext_runtimes
            .lifecycle_lock(self.db_index, &alias_lock_name);
        let _alias_guard = alias_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let Some(existing) = self.read_fulltext_alias(alias)? else {
            return Err(Error::msg("ERR alias does not exist"));
        };
        let lifecycle_lock = self
            .fulltext_runtimes
            .lifecycle_lock(self.db_index, &existing.index);
        let _lifecycle_guard = lifecycle_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let mut batch = WriteBatch::new();
        let alias_key = fulltext_alias_key(self.db_index, alias);
        batch
            .delete(&alias_key)
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut conditions = vec![CompareCondition::exists_with(
            &alias_key,
            encode_record(&existing)?,
        )];
        if let Ok((mut meta, meta_raw)) = self.read_fulltext_meta_versioned(&existing.index) {
            meta.aliases.retain(|candidate| candidate != alias);
            meta.revision = meta.revision.saturating_add(1);
            batch
                .put(
                    &fulltext_meta_key(self.db_index, &existing.index),
                    &encode_record(&meta)?,
                )
                .map_err(|error| Error::msg(error.to_string()))?;
            conditions.push(CompareCondition::exists_with(
                fulltext_meta_key(self.db_index, &existing.index),
                meta_raw,
            ));
        }
        self.fulltext_compare_conditions(&conditions, &batch)?;
        self.fulltext_runtimes.remove_alias(self.db_index, alias);
        Ok(Frame::Ok)
    }

    pub async fn fulltext_alias_del_async(&self, alias: &str) -> Result<Frame, Error> {
        let alias = alias.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_alias_del(&alias))
            .await
    }

    pub fn fulltext_config_get(&self, name: &str) -> Result<Frame, Error> {
        let normalized = name.to_ascii_uppercase();
        let values = if normalized == "*" {
            fulltext_supported_config_names()
                .into_iter()
                .map(|name| {
                    Ok((
                        name.to_string(),
                        self.fulltext_config_value(name)?.unwrap_or_else(|| {
                            fulltext_default_config_value(name)
                                .unwrap_or_default()
                                .to_string()
                        }),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?
        } else {
            validate_fulltext_config_name(&normalized)?;
            vec![(
                normalized.clone(),
                self.fulltext_config_value(&normalized)?.unwrap_or_else(|| {
                    fulltext_default_config_value(&normalized)
                        .unwrap_or_default()
                        .to_string()
                }),
            )]
        };
        Ok(Frame::Array(
            values
                .into_iter()
                .map(|(name, value)| {
                    Frame::Array(vec![Frame::bulk_string(name), Frame::bulk_string(value)])
                })
                .collect(),
        ))
    }

    pub async fn fulltext_config_get_async(&self, name: &str) -> Result<Frame, Error> {
        let name = name.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_config_get(&name))
            .await
    }

    pub fn fulltext_config_set(&self, name: &str, value: &str) -> Result<Frame, Error> {
        let normalized = name.to_ascii_uppercase();
        validate_fulltext_config_value(&normalized, value)?;
        let mut batch = WriteBatch::new();
        batch
            .put(
                &fulltext_config_key(self.db_index, &normalized),
                value.as_bytes(),
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        self.write_batch_if_not_empty(&batch);
        self.fulltext_runtimes
            .set_config_value(self.db_index, &normalized, value.to_string());
        Ok(Frame::Ok)
    }

    pub async fn fulltext_config_set_async(&self, name: &str, value: &str) -> Result<Frame, Error> {
        let name = name.to_string();
        let value = value.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_config_set(&name, &value))
            .await
    }
}
