impl Db {
    pub fn fulltext_create(
        &self,
        index: &str,
        options: FullTextCreateOptions,
    ) -> Result<(), Error> {
        validate_fulltext_create(&options)?;
        if self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, index))
            .is_some()
            || self.read_fulltext_alias(index)?.is_some()
        {
            return Err(Error::msg("ERR fulltext index already exists"));
        }
        let state = if options.index_options.skip_initial_scan {
            FullTextIndexState::Ready
        } else {
            FullTextIndexState::Backfilling
        };
        let meta = FullTextIndexMeta {
            source_type: options.source_type,
            prefixes: options.prefixes,
            schema: options.schema,
            aliases: Vec::new(),
            index_options: options.index_options,
            state,
            generation: new_fulltext_sequence(),
            backfill_cursor: None,
            last_indexed_outbox_seq: 0,
            refresh_policy: FullTextRefreshPolicy::default(),
        };
        let mut batch = WriteBatch::new();
        batch.put(
            &fulltext_meta_key(self.db_index, index),
            &encode_record(&meta)?,
        );
        if meta.index_options.temporary_seconds.is_some() {
            batch.put(
                &fulltext_temporary_activity_key(self.db_index, index),
                &current_fulltext_millis().to_be_bytes(),
            );
        }
        self.write_batch_if_not_empty(&batch);
        self.fulltext_create_vector_indexes(index, &meta)?;
        self.fulltext_runtimes.remove(self.db_index, index);
        self.ensure_fulltext_runtime(index)?;
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
        let meta = self.read_fulltext_meta_direct(&index)?;
        if delete_documents && matches!(meta.source_type, FullTextSourceType::Hash) {
            for key in self.fulltext_matching_hash_keys(&meta)? {
                self.delete_key(&key);
            }
        }

        self.fulltext_purge_index(&index, &meta)?;
        Ok(Frame::Ok)
    }

    fn fulltext_purge_index(&self, index: &str, meta: &FullTextIndexMeta) -> Result<(), Error> {
        let active_storage = self.fulltext_active_storage_name(index, meta);
        let mut batch = WriteBatch::new();
        batch.delete(&fulltext_meta_key(self.db_index, index));
        batch.delete(&fulltext_temporary_activity_key(self.db_index, index));
        for alias in self.fulltext_aliases_for_index(index)? {
            batch.delete(&fulltext_alias_key(self.db_index, &alias));
        }
        self.delete_fulltext_index_storage_to_batch(&mut batch, index);
        if active_storage != index {
            self.delete_fulltext_storage_to_batch(&mut batch, &active_storage);
        }
        self.write_batch_if_not_empty(&batch);
        self.fulltext_delete_vector_indexes(index, meta);
        self.fulltext_runtimes.remove(self.db_index, index);
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
        self.run_blocking_store_task(move |db| {
            db.fulltext_drop_index(&index, delete_documents)
        })
        .await
    }

    pub fn fulltext_alter(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
    ) -> Result<Frame, Error> {
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid fulltext schema"));
        }
        let index = self.resolve_fulltext_index(index)?;
        let mut meta = self.read_fulltext_meta_direct(&index)?;
        let old_meta = meta.clone();
        let old_storage = self.fulltext_active_storage_name(&index, &old_meta);
        let added_fields = fields.clone();
        let mut merged = meta.schema.clone();
        merged.extend(fields);
        let validation_options = FullTextCreateOptions {
            source_type: meta.source_type,
            prefixes: meta.prefixes.clone(),
            schema: merged.clone(),
            index_options: meta.index_options.clone(),
        };
        validate_fulltext_create(&validation_options)?;
        meta.schema = merged;
        meta.state = if meta.index_options.skip_initial_scan {
            FullTextIndexState::Ready
        } else {
            FullTextIndexState::Rebuilding
        };
        meta.generation = new_fulltext_sequence();
        meta.backfill_cursor = None;
        meta.last_indexed_outbox_seq = 0;

        for field in added_fields
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            fulltext_vector_create_options(field)?;
        }

        let staged_storage = fulltext_generation_storage_name(&index, meta.generation);
        let mut cleanup_batch = WriteBatch::new();
        self.delete_fulltext_storage_to_batch(&mut cleanup_batch, &staged_storage);
        self.write_batch_if_not_empty(&cleanup_batch);
        let staged_runtime = match FullTextRuntime::new(
            self.store.clone(),
            self.db_index,
            &index,
            &staged_storage,
            &meta,
        ) {
            Ok(runtime) => runtime,
            Err(error) => {
                let mut batch = WriteBatch::new();
                self.delete_fulltext_storage_to_batch(&mut batch, &staged_storage);
                self.write_batch_if_not_empty(&batch);
                return Err(error);
            }
        };
        drop(staged_runtime);

        let mut created_vector_indexes = Vec::new();
        for field in added_fields
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let internal = fulltext_vector_index_name(&index, field.attribute_name());
            if self.store.get_raw(&self.mk(&internal)).is_some() {
                self.fulltext_cleanup_alter_stage(&staged_storage, &created_vector_indexes);
                return Err(Error::msg(
                    "ERR vector index for added fulltext field already exists",
                ));
            }
            if let Err(error) =
                self.vector_create(&internal, fulltext_vector_create_options(field)?)
            {
                self.fulltext_cleanup_alter_stage(&staged_storage, &created_vector_indexes);
                return Err(error);
            }
            created_vector_indexes.push(internal);
        }

        let mut batch = WriteBatch::new();
        batch.put(
            &fulltext_meta_key(self.db_index, &index),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.fulltext_runtimes.remove(self.db_index, &index);
        #[cfg(test)]
        let runtime_result = if FULLTEXT_ALTER_FAIL_AFTER_SWAP.swap(false, AtomicOrdering::SeqCst) {
            Err(Error::msg("ERR injected FT.ALTER runtime failure"))
        } else {
            self.ensure_fulltext_runtime(&index)
        };
        #[cfg(not(test))]
        let runtime_result = self.ensure_fulltext_runtime(&index);
        if let Err(error) = runtime_result {
            let mut rollback = WriteBatch::new();
            rollback.put(
                &fulltext_meta_key(self.db_index, &index),
                &encode_record(&old_meta)?,
            );
            self.write_batch_if_not_empty(&rollback);
            self.fulltext_runtimes.remove(self.db_index, &index);
            self.fulltext_cleanup_alter_stage(&staged_storage, &created_vector_indexes);
            if let Err(rollback_error) = self.ensure_fulltext_runtime(&index) {
                return Err(Error::msg(format!(
                    "{error}; FT.ALTER rollback failed: {rollback_error}"
                )));
            }
            return Err(error);
        }

        if old_storage != staged_storage {
            let mut cleanup = WriteBatch::new();
            self.delete_fulltext_storage_to_batch(&mut cleanup, &old_storage);
            self.write_batch_if_not_empty(&cleanup);
        }
        Ok(Frame::Ok)
    }

    fn fulltext_cleanup_alter_stage(
        &self,
        staged_storage: &str,
        created_vector_indexes: &[String],
    ) {
        let mut batch = WriteBatch::new();
        self.delete_fulltext_storage_to_batch(&mut batch, staged_storage);
        self.write_batch_if_not_empty(&batch);
        for vector_index in created_vector_indexes {
            self.delete_key(vector_index);
        }
    }

    pub async fn fulltext_alter_async(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_alter(&index, fields))
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
        let Some(existing) = self.read_fulltext_alias(alias)? else {
            return Err(Error::msg("ERR alias does not exist"));
        };
        let mut batch = WriteBatch::new();
        batch.delete(&fulltext_alias_key(self.db_index, alias));
        if let Ok(mut meta) = self.read_fulltext_meta_direct(&existing.index) {
            meta.aliases.retain(|candidate| candidate != alias);
            batch.put(
                &fulltext_meta_key(self.db_index, &existing.index),
                &encode_record(&meta)?,
            );
        }
        self.write_batch_if_not_empty(&batch);
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
        batch.put(
            &fulltext_config_key(self.db_index, &normalized),
            value.as_bytes(),
        );
        self.write_batch_if_not_empty(&batch);
        Ok(Frame::Ok)
    }

    pub async fn fulltext_config_set_async(&self, name: &str, value: &str) -> Result<Frame, Error> {
        let name = name.to_string();
        let value = value.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_config_set(&name, &value))
            .await
    }
}
