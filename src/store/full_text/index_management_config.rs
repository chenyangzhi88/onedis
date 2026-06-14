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
        self.fulltext_create(index, options)
    }

    pub fn fulltext_list(&self) -> Result<Frame, Error> {
        let mut names = self
            .read_all_fulltext_metas()?
            .into_iter()
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        names.sort();
        Ok(Frame::Array(
            names.into_iter().map(Frame::bulk_string).collect(),
        ))
    }

    pub async fn fulltext_list_async(&self) -> Result<Frame, Error> {
        self.fulltext_list()
    }

    pub fn fulltext_drop_index(&self, index: &str, delete_documents: bool) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        if delete_documents && matches!(meta.source_type, FullTextSourceType::Hash) {
            for key in self.fulltext_matching_hash_keys(&meta)? {
                self.delete_key(&key);
            }
        }

        let mut batch = WriteBatch::new();
        batch.delete(&fulltext_meta_key(self.db_index, &index));
        for alias in self.fulltext_aliases_for_index(&index)? {
            batch.delete(&fulltext_alias_key(self.db_index, &alias));
        }
        self.delete_fulltext_index_storage_to_batch(&mut batch, &index);
        self.write_batch_if_not_empty(&batch);
        self.fulltext_delete_vector_indexes(&index, &meta);
        self.fulltext_runtimes.remove(self.db_index, &index);
        Ok(Frame::Ok)
    }

    pub async fn fulltext_drop_index_async(
        &self,
        index: &str,
        delete_documents: bool,
    ) -> Result<Frame, Error> {
        self.fulltext_drop_index(index, delete_documents)
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

        let mut batch = WriteBatch::new();
        self.delete_fulltext_index_storage_to_batch(&mut batch, &index);
        batch.put(
            &fulltext_meta_key(self.db_index, &index),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.fulltext_delete_vector_indexes(&index, &old_meta);
        self.fulltext_create_vector_indexes(&index, &meta)?;
        self.fulltext_runtimes.remove(self.db_index, &index);
        self.ensure_fulltext_runtime(&index)?;
        Ok(Frame::Ok)
    }

    pub async fn fulltext_alter_async(
        &self,
        index: &str,
        fields: Vec<FullTextFieldSchema>,
    ) -> Result<Frame, Error> {
        self.fulltext_alter(index, fields)
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
        self.fulltext_alias_add(alias, index)
    }

    pub fn fulltext_alias_update(&self, alias: &str, index: &str) -> Result<Frame, Error> {
        self.fulltext_alias_set(alias, index, true)
    }

    pub async fn fulltext_alias_update_async(
        &self,
        alias: &str,
        index: &str,
    ) -> Result<Frame, Error> {
        self.fulltext_alias_update(alias, index)
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
        self.fulltext_alias_del(alias)
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
        self.fulltext_config_get(name)
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
        self.fulltext_config_set(name, value)
    }


}
