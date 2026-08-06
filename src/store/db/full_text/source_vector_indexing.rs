use super::*;
impl Db {
    pub(super) fn fulltext_request_refresh_for_source(
        &self,
        key: &str,
        source_type: FullTextSourceType,
    ) -> Result<(), Error> {
        // The mutation itself already appended a durable outbox record in the
        // same write batch. Searches synchronously catch up that outbox before
        // evaluating a query, and the maintenance worker drains it in the
        // background for INFO/idle indexes. Keep Tantivy indexing off this
        // path, but retain bounded outbox compaction without scanning the
        // queue on every source write.
        let matching_metas = self.fulltext_matching_metas_for_source(key, source_type)?;
        if matching_metas.is_empty() {
            return Ok(());
        }
        let threshold = self.fulltext_outbox_compact_threshold()?;
        for (index, meta) in matching_metas {
            if self
                .fulltext_runtimes
                .outbox_pending(self.db_index, &index)
                .is_none()
            {
                // The just-committed mutation is already visible to this
                // recovery scan, so do not increment it a second time.
                self.fulltext_pending_outbox_count(&index);
            } else {
                self.fulltext_runtimes
                    .add_outbox_pending(self.db_index, &index, 1);
            }
            if self
                .fulltext_runtimes
                .note_outbox_mutation(self.db_index, &index, threshold)
            {
                self.fulltext_compact_outbox_if_needed(&index, threshold)?;
            }
            let _ = meta;
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
            .get_raw(&fulltext_meta_key(self.db_index, alias))
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
            );
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
        );
        batch.put(
            &fulltext_meta_key(self.db_index, &index),
            &encode_record(&meta)?,
        );
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

    pub(super) fn fulltext_json_root(&self, key: &str) -> Result<Option<serde_json::Value>, Error> {
        self.expire_if_needed(key);
        if self.store.get_raw(&self.mk(key)).is_none() {
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
            if field.options.noindex {
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
            match self.vector_create(&internal, fulltext_vector_create_options(field)?) {
                Ok(()) => {}
                Err(err) if err.to_string() == "ERR vector index already exists" => {}
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
            self.delete_key(&fulltext_vector_index_name(
                index,
                meta.generation,
                field.attribute_name(),
            ));
        }
    }

    pub(super) fn fulltext_upsert_vectors(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
        fields: &[(String, String)],
        json_root: Option<&serde_json::Value>,
    ) -> Result<(), Error> {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let vector = match meta.source_type {
                FullTextSourceType::Hash => fields
                    .iter()
                    .find(|(name, _)| name == &field.name || name == field.attribute_name())
                    .map(|(_, value)| parse_fulltext_vector_text(value))
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
            if let Some(vector) = vector {
                self.vector_add(&internal, key, vector, None)?;
            } else {
                let ids = [key.to_string()];
                self.vector_del(&internal, &ids)?;
            }
        }
        Ok(())
    }

    pub(super) fn fulltext_delete_vectors(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
    ) -> Result<(), Error> {
        let ids = [key.to_string()];
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            self.vector_del(
                &fulltext_vector_index_name(index, meta.generation, field.attribute_name()),
                &ids,
            )?;
        }
        Ok(())
    }
}

pub(super) fn fulltext_json_values_from_root(
    root: &serde_json::Value,
    path: &str,
) -> Result<Vec<serde_json::Value>, Error> {
    let tokens = parse_fulltext_json_path(path)?;
    Ok(fulltext_json_path_values(root, &tokens))
}
