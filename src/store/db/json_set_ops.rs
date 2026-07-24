use super::*;

pub(in crate::store::db) struct JsonIndexedSetRequest<'a> {
    key: &'a str,
    expire_ms: u64,
    version: u64,
    tokens: &'a [JsonPathToken],
    new_value: JsonValue,
    condition: SetCondition,
    cas_condition: CompareCondition,
}

impl Db {
    pub fn json_set(
        &self,
        key: &str,
        path: &str,
        json: &str,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let tokens = parse_json_path(path)?;
        let new_value: JsonValue =
            serde_json::from_str(json).map_err(|_| Error::msg("ERR invalid JSON value"))?;

        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            if !tokens.is_empty() || condition == SetCondition::Xx {
                return Ok(false);
            }
            self.write_json_value(key, &new_value, 0, self.next_persisted_version())?;
            return Ok(true);
        };

        let (expire_ms, version, indexed) = Self::decode_json_meta(&raw)?;
        if !indexed {
            let mut document = Self::decode_legacy_json_document(&raw)?;
            if tokens.is_empty() {
                if condition == SetCondition::Nx {
                    return Ok(false);
                }
                self.write_json_value(key, &new_value, expire_ms, version)?;
                return Ok(true);
            }
            let target_exists = json_get_path(&document, &tokens).is_some();
            let condition_matches = match condition {
                SetCondition::Always => true,
                SetCondition::Nx => !target_exists,
                SetCondition::Xx => target_exists,
            };
            if !condition_matches {
                return Ok(false);
            }
            if json_set_path(&mut document, &tokens, new_value).is_none() {
                return Ok(false);
            }
            self.write_json_value(key, &document, expire_ms, version)?;
            return Ok(true);
        }

        if tokens.is_empty() {
            if condition == SetCondition::Nx {
                return Ok(false);
            }
            self.write_json_value(key, &new_value, expire_ms, version)?;
            return Ok(true);
        }

        self.json_set_indexed(key, expire_ms, version, &tokens, new_value, condition)
    }

    pub(in crate::store::db) fn json_set_indexed(
        &self,
        key: &str,
        expire_ms: u64,
        version: u64,
        tokens: &[JsonPathToken],
        new_value: JsonValue,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let target_exists = self.json_node_exists(key, version, tokens);
        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !target_exists,
            SetCondition::Xx => target_exists,
        };
        if !condition_matches {
            return Ok(false);
        }

        let Some((last, parent_tokens)) = tokens.split_last() else {
            return Ok(false);
        };
        let Some(mut parent_node) = self.read_json_node(key, version, parent_tokens)? else {
            return Ok(false);
        };

        let mut batch = WriteBatch::new();
        match (last, &mut parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(fields)) => {
                if !fields.iter().any(|existing| existing == field) {
                    fields.push(field.clone());
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                let mut path = tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut path,
                    &new_value,
                )?;
                batch.put(
                    &json_node_key(self.db_index, key, version, parent_tokens),
                    &encode_json_node(&parent_node),
                );
            }
            (JsonPathToken::Index(index), JsonNode::Array(len)) => {
                if *index >= *len {
                    return Ok(false);
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                let mut path = tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut path,
                    &new_value,
                )?;
            }
            _ => return Ok(false),
        }
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version);
        if let Err(err) = self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key) {
            log::error!("failed to enqueue fulltext JSON upsert for {key}: {err}");
        }
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_json_refresh(key)?;
        Ok(true)
    }

    pub(in crate::store::db) async fn json_set_indexed_async(
        &self,
        request: JsonIndexedSetRequest<'_>,
    ) -> Result<Option<bool>, Error> {
        let JsonIndexedSetRequest {
            key,
            expire_ms,
            version,
            tokens,
            new_value,
            condition,
            cas_condition,
        } = request;
        let target_exists = self.json_node_exists_async(key, version, tokens).await;
        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !target_exists,
            SetCondition::Xx => target_exists,
        };
        if !condition_matches {
            return Ok(Some(false));
        }

        let Some((last, parent_tokens)) = tokens.split_last() else {
            return Ok(Some(false));
        };
        let Some(mut parent_node) = self
            .read_json_node_async(key, version, parent_tokens)
            .await?
        else {
            return Ok(Some(false));
        };

        let mut batch = WriteBatch::new();
        match (last, &mut parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(fields)) => {
                if !fields.iter().any(|existing| existing == field) {
                    fields.push(field.clone());
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                let mut path = tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut path,
                    &new_value,
                )?;
                batch.put(
                    &json_node_key(self.db_index, key, version, parent_tokens),
                    &encode_json_node(&parent_node),
                );
            }
            (JsonPathToken::Index(index), JsonNode::Array(len)) => {
                if *index >= *len {
                    return Ok(Some(false));
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                let mut path = tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut path,
                    &new_value,
                )?;
            }
            _ => return Ok(Some(false)),
        }
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version);
        if let Err(err) = self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key) {
            log::error!("failed to enqueue fulltext JSON upsert for {key}: {err}");
        }
        if self
            .compare_and_write_batch_if_not_empty_async(&[cas_condition], &batch)
            .await?
        {
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_json_refresh(key)?;
            return Ok(Some(true));
        }
        Ok(None)
    }

    pub async fn json_set_async(
        &self,
        key: &str,
        path: &str,
        json: &str,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let tokens = parse_json_path(path)?;
        let new_value: JsonValue =
            serde_json::from_str(json).map_err(|_| Error::msg("ERR invalid JSON value"))?;

        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let cas_condition = CompareCondition::from_observed(&observed);
            let Some(raw) = observed.value().map(|value| value.to_vec()) else {
                if !tokens.is_empty() || condition == SetCondition::Xx {
                    return Ok(false);
                }
                if self
                    .write_json_value_cas_async(
                        key,
                        &new_value,
                        0,
                        self.next_persisted_version_async().await,
                        cas_condition,
                    )
                    .await?
                {
                    return Ok(true);
                }
                continue;
            };

            let (expire_ms, version, indexed) = Self::decode_json_meta(&raw)?;
            if !indexed {
                let mut document = Self::decode_legacy_json_document(&raw)?;
                if tokens.is_empty() {
                    if condition == SetCondition::Nx {
                        return Ok(false);
                    }
                    if self
                        .write_json_value_cas_async(
                            key,
                            &new_value,
                            expire_ms,
                            version,
                            cas_condition,
                        )
                        .await?
                    {
                        return Ok(true);
                    }
                    continue;
                }
                let target_exists = json_get_path(&document, &tokens).is_some();
                let condition_matches = match condition {
                    SetCondition::Always => true,
                    SetCondition::Nx => !target_exists,
                    SetCondition::Xx => target_exists,
                };
                if !condition_matches {
                    return Ok(false);
                }
                if json_set_path(&mut document, &tokens, new_value.clone()).is_none() {
                    return Ok(false);
                }
                if self
                    .write_json_value_cas_async(key, &document, expire_ms, version, cas_condition)
                    .await?
                {
                    return Ok(true);
                }
                continue;
            }

            if tokens.is_empty() {
                if condition == SetCondition::Nx {
                    return Ok(false);
                }
                if self
                    .write_json_value_cas_async(key, &new_value, expire_ms, version, cas_condition)
                    .await?
                {
                    return Ok(true);
                }
                continue;
            }

            match self
                .json_set_indexed_async(JsonIndexedSetRequest {
                    key,
                    expire_ms,
                    version,
                    tokens: &tokens,
                    new_value: new_value.clone(),
                    condition,
                    cas_condition,
                })
                .await?
            {
                Some(result) => return Ok(result),
                None => continue,
            }
        }

        Err(Error::msg("ERR json write conflict"))
    }
}
