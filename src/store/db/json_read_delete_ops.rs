use super::*;

impl Db {
    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<String>, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };
        let (_, version, indexed) = Self::decode_json_meta(&raw)?;
        let value = if indexed {
            self.read_json_value_at_path(key, version, &tokens)?
        } else {
            let document = Self::decode_legacy_json_document(&raw)?;
            json_get_path(&document, &tokens).cloned()
        };
        let Some(value) = value else {
            return Ok(None);
        };
        serde_json::to_string(&value)
            .map(Some)
            .map_err(|_| Error::msg("ERR failed to encode JSON value"))
    }

    pub async fn json_get_async(&self, key: &str, path: &str) -> Result<Option<String>, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed_async(key).await;
        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };
        let (_, version, indexed) = Self::decode_json_meta(&raw)?;
        let value = if indexed {
            self.read_json_value_at_path_async(key, version, &tokens)
                .await?
        } else {
            let document = Self::decode_legacy_json_document(&raw)?;
            json_get_path(&document, &tokens).cloned()
        };
        let Some(value) = value else {
            return Ok(None);
        };
        serde_json::to_string(&value)
            .map(Some)
            .map_err(|_| Error::msg("ERR failed to encode JSON value"))
    }

    pub fn json_del(&self, key: &str, path: &str) -> Result<i64, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(0);
        };
        let (expire_ms, version, indexed) = Self::decode_json_meta(&raw)?;
        if tokens.is_empty() {
            return Ok(i64::from(self.delete_key_internal(key, true)));
        }
        if indexed {
            return self.json_del_indexed(key, expire_ms, version, &tokens);
        }
        let mut document = Self::decode_legacy_json_document(&raw)?;
        if !json_del_path(&mut document, &tokens) {
            return Ok(0);
        }
        self.write_json_value(key, &document, expire_ms, version)?;
        Ok(1)
    }

    pub async fn json_del_async(&self, key: &str, path: &str) -> Result<i64, Error> {
        let tokens = parse_json_path(path)?;
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let cas_condition = CompareCondition::from_observed(&observed);
            let Some(raw) = observed.value().map(|value| value.to_vec()) else {
                return Ok(0);
            };
            let (expire_ms, version, indexed) = Self::decode_json_meta(&raw)?;
            if tokens.is_empty() {
                return Ok(i64::from(self.delete_key_internal_async(key, true).await));
            }
            if indexed {
                match self
                    .json_del_indexed_async(key, expire_ms, version, &tokens, cas_condition)
                    .await?
                {
                    Some(deleted) => return Ok(deleted),
                    None => continue,
                }
            }
            let mut document = Self::decode_legacy_json_document(&raw)?;
            if !json_del_path(&mut document, &tokens) {
                return Ok(0);
            }
            if self
                .write_json_value_cas_async(key, &document, expire_ms, version, cas_condition)
                .await?
            {
                return Ok(1);
            }
        }
        Err(Error::msg("ERR json write conflict"))
    }

    pub fn json_type(&self, key: &str, path: &str) -> Result<Option<&'static str>, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };
        let (_, version, indexed) = Self::decode_json_meta(&raw)?;
        if indexed {
            return self.json_type_indexed(key, version, &tokens);
        }
        let document = Self::decode_legacy_json_document(&raw)?;
        Ok(json_get_path(&document, &tokens).map(json_type_name))
    }

    pub async fn json_type_async(
        &self,
        key: &str,
        path: &str,
    ) -> Result<Option<&'static str>, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed_async(key).await;
        let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
            return Ok(None);
        };
        let (_, version, indexed) = Self::decode_json_meta(&raw)?;
        if indexed {
            return self.json_type_indexed_async(key, version, &tokens).await;
        }
        let document = Self::decode_legacy_json_document(&raw)?;
        Ok(json_get_path(&document, &tokens).map(json_type_name))
    }

    pub(in crate::store::db) fn json_del_indexed(
        &self,
        key: &str,
        expire_ms: u64,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<i64, Error> {
        if !self.json_node_exists(key, version, tokens) {
            return Ok(0);
        }
        let Some((last, parent_tokens)) = tokens.split_last() else {
            return Ok(0);
        };
        let Some(mut parent_node) = self.read_json_node(key, version, parent_tokens)? else {
            return Ok(0);
        };

        let mut batch = WriteBatch::new();
        match (last, &mut parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(fields)) => {
                let Some(pos) = fields.iter().position(|existing| existing == field) else {
                    return Ok(0);
                };
                fields.remove(pos);
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                batch.put(
                    &json_node_key(self.db_index, key, version, parent_tokens),
                    &encode_json_node(&parent_node),
                );
            }
            (JsonPathToken::Index(_), JsonNode::Array(_)) => {
                let Some(mut parent_value) =
                    self.read_json_value_at_path(key, version, parent_tokens)?
                else {
                    return Ok(0);
                };
                if !json_del_path(&mut parent_value, std::slice::from_ref(last)) {
                    return Ok(0);
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    parent_tokens,
                );
                let mut parent_path = parent_tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut parent_path,
                    &parent_value,
                )?;
            }
            _ => return Ok(0),
        }
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version);
        if let Err(err) = self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key) {
            log::error!("failed to enqueue fulltext JSON upsert for {key}: {err}");
        }
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_json_refresh(key)?;
        Ok(1)
    }

    pub(in crate::store::db) async fn json_del_indexed_async(
        &self,
        key: &str,
        expire_ms: u64,
        version: u64,
        tokens: &[JsonPathToken],
        cas_condition: CompareCondition,
    ) -> Result<Option<i64>, Error> {
        if !self.json_node_exists_async(key, version, tokens).await {
            return Ok(Some(0));
        }
        let Some((last, parent_tokens)) = tokens.split_last() else {
            return Ok(Some(0));
        };
        let Some(mut parent_node) = self
            .read_json_node_async(key, version, parent_tokens)
            .await?
        else {
            return Ok(Some(0));
        };

        let mut batch = WriteBatch::new();
        match (last, &mut parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(fields)) => {
                let Some(pos) = fields.iter().position(|existing| existing == field) else {
                    return Ok(Some(0));
                };
                fields.remove(pos);
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    tokens,
                );
                batch.put(
                    &json_node_key(self.db_index, key, version, parent_tokens),
                    &encode_json_node(&parent_node),
                );
            }
            (JsonPathToken::Index(_), JsonNode::Array(_)) => {
                let Some(mut parent_value) = self
                    .read_json_value_at_path_async(key, version, parent_tokens)
                    .await?
                else {
                    return Ok(Some(0));
                };
                if !json_del_path(&mut parent_value, std::slice::from_ref(last)) {
                    return Ok(Some(0));
                }
                delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    parent_tokens,
                );
                let mut parent_path = parent_tokens.to_vec();
                write_json_subtree_to_batch(
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &mut parent_path,
                    &parent_value,
                )?;
            }
            _ => return Ok(Some(0)),
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
            return Ok(Some(1));
        }
        Ok(None)
    }
}
