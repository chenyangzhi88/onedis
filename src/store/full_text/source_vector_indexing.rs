impl Db {
    fn fulltext_request_refresh_for_source(
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
        let threshold = self.fulltext_outbox_compact_threshold()?;
        for (index, meta) in self.fulltext_matching_metas_for_source(key, source_type)? {
            if self.fulltext_runtimes.note_outbox_mutation(
                self.db_index,
                &index,
                threshold,
            ) {
                self.fulltext_compact_outbox_if_needed(&index, meta.generation, threshold)?;
            }
        }
        Ok(())
    }

    fn fulltext_alias_set(&self, alias: &str, index: &str, update: bool) -> Result<Frame, Error> {
        if alias.is_empty() {
            return Err(Error::msg("ERR invalid alias"));
        }
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

        let mut batch = WriteBatch::new();
        if let Some(previous) = previous
            && previous.index != index
            && let Ok(mut old_meta) = self.read_fulltext_meta_direct(&previous.index)
        {
            old_meta.aliases.retain(|candidate| candidate != alias);
            batch.put(
                &fulltext_meta_key(self.db_index, &previous.index),
                &encode_record(&old_meta)?,
            );
        }

        let mut meta = self.read_fulltext_meta_direct(&index)?;
        if !meta.aliases.iter().any(|candidate| candidate == alias) {
            meta.aliases.push(alias.to_string());
            meta.aliases.sort();
        }
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
        self.write_batch_if_not_empty(&batch);
        Ok(Frame::Ok)
    }

    fn fulltext_json_fields(
        &self,
        key: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<Option<Vec<(String, String)>>, Error> {
        self.expire_if_needed(key);
        if self.store.get_raw(&self.mk(key)).is_none() {
            return Ok(None);
        }
        let mut fields = Vec::new();
        for field in &meta.schema {
            if field.options.noindex {
                continue;
            }
            let values = self.fulltext_json_values(key, &field.name)?;
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
            if let Some(value) = self
                .fulltext_json_values(key, field)?
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
        Ok(Some(fields))
    }

    fn fulltext_json_return_fields(
        &self,
        key: &str,
        meta: &FullTextIndexMeta,
        dialect: u8,
    ) -> Result<Vec<(String, String)>, Error> {
        let mut fields = Vec::new();
        for field in &meta.schema {
            if field.options.noindex {
                continue;
            }
            let values = self.fulltext_json_values(key, &field.name)?;
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

    fn fulltext_json_values(&self, key: &str, path: &str) -> Result<Vec<serde_json::Value>, Error> {
        let Some(raw) = self.json_get(key, "$")? else {
            return Ok(Vec::new());
        };
        let root: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|_| Error::msg("ERR failed to decode JSON value"))?;
        let tokens = parse_fulltext_json_path(path)?;
        Ok(fulltext_json_path_values(&root, &tokens))
    }

    fn fulltext_create_vector_indexes(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<(), Error> {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            let internal = fulltext_vector_index_name(index, field.attribute_name());
            match self.vector_create(&internal, fulltext_vector_create_options(field)?) {
                Ok(()) => {}
                Err(err) if err.to_string() == "ERR vector index already exists" => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn fulltext_delete_vector_indexes(&self, index: &str, meta: &FullTextIndexMeta) {
        for field in meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
        {
            self.delete_key(&fulltext_vector_index_name(index, field.attribute_name()));
        }
    }

    fn fulltext_upsert_vectors(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        key: &str,
        fields: &[(String, String)],
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
                FullTextSourceType::Json => self
                    .fulltext_json_values(key, &field.name)?
                    .into_iter()
                    .next()
                    .map(|value| parse_fulltext_vector_json_value(&value))
                    .transpose()?,
            };
            let internal = fulltext_vector_index_name(index, field.attribute_name());
            if let Some(vector) = vector {
                self.vector_add(&internal, key, vector, None)?;
            } else {
                let ids = [key.to_string()];
                self.vector_del(&internal, &ids)?;
            }
        }
        Ok(())
    }

    fn fulltext_delete_vectors(
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
                &fulltext_vector_index_name(index, field.attribute_name()),
                &ids,
            )?;
        }
        Ok(())
    }


}
