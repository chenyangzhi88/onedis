use super::*;

pub(super) type FullTextVectorMutationBatches = HashMap<String, Vec<(String, Option<Vec<f32>>)>>;
pub(in crate::store::db) type FullTextCommittedOutbox = HashMap<String, (u64, u64)>;
impl Db {
    pub(super) fn fulltext_request_refresh_for_source(
        &self,
        _key: &str,
        _source_type: FullTextSourceType,
    ) -> Result<(), Error> {
        // The mutation itself already appended a durable outbox record in the
        // same write batch. Consistent searches only publish pending records
        // into the process-local Tantivy overlay; the maintenance worker owns
        // durable KV checkpoints and outbox retirement. Keep tokenization,
        // segment IO and merge work off the source-write path.
        // The normal write helpers publish committed outbox markers directly
        // from their WriteBatch. External writers (currently the TTL sweeper)
        // use `fulltext_observe_external_source_commit` below.
        Ok(())
    }

    pub(in crate::store) fn fulltext_observe_committed_outbox_batch(&self, batch: &WriteBatch) {
        let committed = self.fulltext_collect_committed_outbox_batch(batch);
        self.fulltext_publish_committed_outbox(committed);
    }

    pub(in crate::store::db) fn fulltext_collect_committed_outbox_batch(
        &self,
        batch: &WriteBatch,
    ) -> FullTextCommittedOutbox {
        let mut committed = FullTextCommittedOutbox::new();
        for (write_type, key, value) in batch.iter() {
            if !write_type.is_put_like() {
                continue;
            }
            if let Some((index, seq)) = fulltext_index_and_seq_from_outbox_key(self.db_index, key) {
                let entry = committed.entry(index).or_default();
                entry.0 = entry.0.max(seq);
                entry.1 = entry.1.saturating_add(1);
                continue;
            }
            if value.len() != std::mem::size_of::<u64>() {
                continue;
            }
            let Some(index) = fulltext_index_from_outbox_latest_key(self.db_index, key) else {
                continue;
            };
            let Some(seq) = value.try_into().ok().map(u64::from_be_bytes) else {
                continue;
            };
            let entry = committed.entry(index).or_default();
            entry.0 = entry.0.max(seq);
        }
        committed
    }

    pub(in crate::store::db) fn fulltext_publish_committed_outbox(
        &self,
        committed: FullTextCommittedOutbox,
    ) {
        for (index, (seq, count)) in committed {
            self.fulltext_runtimes
                .note_latest_outbox_seq(self.db_index, &index, seq);
            if self
                .fulltext_runtimes
                .outbox_pending(self.db_index, &index)
                .is_some()
            {
                self.fulltext_runtimes
                    .add_outbox_pending(self.db_index, &index, count);
            } else {
                self.fulltext_runtimes
                    .set_outbox_pending(self.db_index, &index, count);
            }
            self.fulltext_runtimes
                .note_outbox_mutations(self.db_index, &index, count as usize);
        }
    }

    pub(crate) fn fulltext_observe_external_source_commit(
        &self,
        key: &str,
        source_type: FullTextSourceType,
    ) -> Result<(), Error> {
        for (index, _) in self.fulltext_matching_metas_for_source(key, source_type)? {
            let Some(seq) = self
                .store
                .get_raw(&fulltext_outbox_latest_key(self.db_index, &index))?
                .and_then(|raw| raw.try_into().ok())
                .map(u64::from_be_bytes)
            else {
                continue;
            };
            let previous = self
                .fulltext_runtimes
                .latest_outbox_seq(self.db_index, &index);
            if previous.is_some_and(|previous| previous >= seq) {
                continue;
            }
            self.fulltext_runtimes
                .note_latest_outbox_seq(self.db_index, &index, seq);
            if self
                .fulltext_runtimes
                .outbox_pending(self.db_index, &index)
                .is_some()
            {
                self.fulltext_runtimes
                    .add_outbox_pending(self.db_index, &index, 1);
            } else {
                self.fulltext_runtimes
                    .set_outbox_pending(self.db_index, &index, 1);
            }
            self.fulltext_runtimes
                .note_outbox_mutations(self.db_index, &index, 1);
        }
        Ok(())
    }

    pub(super) fn fulltext_alias_set(
        &self,
        alias: &str,
        index: &str,
        update: bool,
    ) -> Result<Frame, Error> {
        validate_fulltext_identifier(alias, "alias")?;
        let alias_lock_name = format!("__alias__:{alias}");
        let alias_lock = self
            .fulltext_runtimes
            .lifecycle_lock(self.db_index, &alias_lock_name);
        let _alias_guard = alias_lock
            .write()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        if self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, alias))?
            .is_some()
        {
            return Err(Error::msg("ERR alias conflicts with index name"));
        }
        let index = self.resolve_fulltext_index(index)?;
        let previous = self.read_fulltext_alias(alias)?;
        if !update && previous.is_some() {
            return Err(Error::msg("ERR alias already exists"));
        }

        let mut lock_names = vec![index.clone()];
        if let Some(previous) = &previous
            && previous.index != index
        {
            lock_names.push(previous.index.clone());
        }
        lock_names.sort();
        lock_names.dedup();
        let lifecycle_locks = lock_names
            .iter()
            .map(|name| self.fulltext_runtimes.lifecycle_lock(self.db_index, name))
            .collect::<Vec<_>>();
        let _lifecycle_guards = lifecycle_locks
            .iter()
            .map(|lock| {
                lock.write()
                    .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut batch = WriteBatch::new();
        let mut conditions = Vec::new();
        if let Some(previous) = &previous
            && previous.index != index
            && let Ok((mut old_meta, old_raw)) = self.read_fulltext_meta_versioned(&previous.index)
        {
            old_meta.aliases.retain(|candidate| candidate != alias);
            old_meta.revision = old_meta.revision.saturating_add(1);
            batch.put(
                &fulltext_meta_key(self.db_index, &previous.index),
                &encode_record(&old_meta)?,
            )?;
            conditions.push(CompareCondition::exists_with(
                fulltext_meta_key(self.db_index, &previous.index),
                old_raw,
            ));
        }

        let (mut meta, meta_raw) = self.read_fulltext_meta_versioned(&index)?;
        if !meta.aliases.iter().any(|candidate| candidate == alias) {
            meta.aliases.push(alias.to_string());
            meta.aliases.sort();
        }
        meta.revision = meta.revision.saturating_add(1);
        batch.put(
            &fulltext_alias_key(self.db_index, alias),
            &encode_record(&FullTextAliasMeta {
                index: index.clone(),
            })?,
        )?;
        batch.put(
            &fulltext_meta_key(self.db_index, &index),
            &encode_record(&meta)?,
        )?;
        conditions.push(CompareCondition::exists_with(
            fulltext_meta_key(self.db_index, &index),
            meta_raw,
        ));
        let alias_key = fulltext_alias_key(self.db_index, alias);
        conditions.push(match previous {
            Some(existing) => CompareCondition::exists_with(&alias_key, encode_record(&existing)?),
            None => CompareCondition::absent(&alias_key),
        });
        self.fulltext_compare_conditions(&conditions, &batch)?;
        self.fulltext_runtimes
            .set_alias_target(self.db_index, alias, &index);
        Ok(Frame::Ok)
    }

    pub(super) fn fulltext_json_fields(
        &self,
        key: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<Option<Vec<(String, String)>>, Error> {
        let Some(root) = self.fulltext_json_root(key)? else {
            return Ok(None);
        };
        Ok(Some(self.fulltext_json_fields_from_root(&root, meta)?))
    }

    pub(super) fn fulltext_source_expire_ms(&self, key: &str) -> Result<u64, Error> {
        Ok(self
            .store
            .get_raw(&self.mk(key))?
            .as_deref()
            .and_then(decode_meta_header)
            .map_or(0, |header| header.expire_ms))
    }

    pub(super) fn fulltext_json_root(&self, key: &str) -> Result<Option<serde_json::Value>, Error> {
        self.expire_if_needed(key)?;
        if self.store.get_raw(&self.mk(key))?.is_none() {
            return Ok(None);
        }
        let Some(raw) = self.json_get(key, "$")? else {
            return Ok(None);
        };
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|_| Error::msg("ERR failed to decode JSON value"))
    }

    pub(super) fn fulltext_json_fields_from_root(
        &self,
        root: &serde_json::Value,
        meta: &FullTextIndexMeta,
    ) -> Result<Vec<(String, String)>, Error> {
        let mut fields = Vec::new();
        for field in &meta.schema {
            if field.options.noindex && !field.options.index_missing {
                continue;
            }
            let values = fulltext_json_values_from_root(root, &field.name)?;
            if values.is_empty() {
                continue;
            }
            let attribute = field.attribute_name().to_string();
            match field.kind {
                FullTextFieldKind::Text => {
                    for value in &values {
                        for text in json_index_strings(value) {
                            fields.push((attribute.clone(), text));
                        }
                    }
                }
                FullTextFieldKind::Tag => {
                    for value in &values {
                        for tag in json_index_tag_values(value) {
                            fields.push((attribute.clone(), tag));
                        }
                    }
                }
                FullTextFieldKind::Numeric => {
                    for value in &values {
                        for number in json_index_numeric_values(value) {
                            fields.push((attribute.clone(), number));
                        }
                    }
                }
                FullTextFieldKind::Geo | FullTextFieldKind::GeoShape => {
                    for value in &values {
                        for text in json_index_strings(value) {
                            fields.push((attribute.clone(), text));
                        }
                    }
                }
                FullTextFieldKind::Vector => {}
            }
        }
        for field in [
            meta.index_options.language_field.as_deref(),
            meta.index_options.score_field.as_deref(),
            meta.index_options.payload_field.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if fields.iter().any(|(name, _)| name == field) {
                continue;
            }
            if let Some(value) = fulltext_json_values_from_root(root, field)?
                .into_iter()
                .next()
                .and_then(|value| match value {
                    serde_json::Value::String(value) => Some(value),
                    serde_json::Value::Number(value) => Some(value.to_string()),
                    serde_json::Value::Bool(value) => Some(value.to_string()),
                    _ => None,
                })
            {
                fields.push((field.to_string(), value));
            }
        }
        Ok(fields)
    }

    pub(super) fn fulltext_json_return_fields_from_root(
        &self,
        root: &serde_json::Value,
        meta: &FullTextIndexMeta,
        dialect: u8,
    ) -> Result<Vec<(String, String)>, Error> {
        let mut fields = Vec::new();
        for field in &meta.schema {
            if field.options.noindex {
                continue;
            }
            let values = fulltext_json_values_from_root(root, &field.name)?;
            if values.is_empty() {
                continue;
            }
            let value = if dialect >= 3 && values.len() > 1 {
                serde_json::Value::Array(values)
            } else {
                values.into_iter().next().unwrap_or(serde_json::Value::Null)
            };
            fields.push((
                field.attribute_name().to_string(),
                serde_json::to_string(&value)
                    .map_err(|_| Error::msg("ERR failed to encode JSON value"))?,
            ));
        }
        Ok(fields)
    }

    pub(super) fn fulltext_create_vector_indexes(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<(), Error> {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let internal =
                fulltext_vector_index_name(index, meta.generation, field.attribute_name());
            let flat = field
                .options
                .vector
                .as_ref()
                .is_some_and(|options| options.algorithm == FullTextVectorAlgorithm::Flat);
            match self.vector_create_internal(
                &internal,
                fulltext_vector_create_options(field)?,
                flat,
            ) {
                Ok(()) => {}
                Err(err) if err.to_string() == "ERR vector index already exists" => {
                    self.vector_set_internal_algorithm(&internal, flat)?;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub(super) fn fulltext_delete_vector_indexes(&self, index: &str, meta: &FullTextIndexMeta) {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let _ = self.vector_drop(&fulltext_vector_index_name(
                index,
                meta.generation,
                field.attribute_name(),
            ));
        }
    }

    pub(super) fn fulltext_collect_vector_mutations(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
        json_root: Option<&serde_json::Value>,
        batches: &mut FullTextVectorMutationBatches,
    ) -> Result<(), Error> {
        if !meta
            .schema
            .iter()
            .any(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            return Ok(());
        }
        let hash_fields = matches!(meta.source_type, FullTextSourceType::Hash)
            .then(|| self.hash_get_all_bytes(key))
            .transpose()?;
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let vector = match meta.source_type {
                FullTextSourceType::Hash => hash_fields
                    .as_ref()
                    .expect("HASH fields loaded above")
                    .iter()
                    .find(|(name, _)| name == &field.name || name == field.attribute_name())
                    .map(|(_, value)| parse_fulltext_vector_value(value, field))
                    .transpose()?,
                FullTextSourceType::Json => json_root
                    .map(|root| fulltext_json_values_from_root(root, &field.name))
                    .transpose()?
                    .unwrap_or_default()
                    .into_iter()
                    .next()
                    .map(|value| parse_fulltext_vector_json_value(&value))
                    .transpose()?,
            };
            let internal =
                fulltext_vector_index_name(index, meta.generation, field.attribute_name());
            batches
                .entry(internal)
                .or_default()
                .push((key.to_string(), vector));
        }
        Ok(())
    }

    pub(super) fn fulltext_apply_vector_mutations(
        &self,
        batches: FullTextVectorMutationBatches,
    ) -> Result<(), Error> {
        for (index, mutations) in batches {
            self.vector_apply_internal_batch(&index, mutations)?;
        }
        Ok(())
    }

    pub(super) fn fulltext_collect_vector_deletions(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
        batches: &mut FullTextVectorMutationBatches,
    ) {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            batches
                .entry(fulltext_vector_index_name(
                    index,
                    meta.generation,
                    field.attribute_name(),
                ))
                .or_default()
                .push((key.to_string(), None));
        }
    }

    pub(super) fn fulltext_upsert_vectors(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
        _fields: &[(String, String)],
        json_root: Option<&serde_json::Value>,
    ) -> Result<(), Error> {
        let mut batches = FullTextVectorMutationBatches::new();
        self.fulltext_collect_vector_mutations(index, meta, key, json_root, &mut batches)?;
        self.fulltext_apply_vector_mutations(batches)
    }
}

pub(super) fn fulltext_json_values_from_root(
    root: &serde_json::Value,
    path: &str,
) -> Result<Vec<serde_json::Value>, Error> {
    let tokens = parse_fulltext_json_path(path)?;
    Ok(fulltext_json_path_values(root, &tokens))
}
