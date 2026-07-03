use super::*;

impl Db {
    pub(in crate::store::db) fn decode_json_meta(raw: &[u8]) -> Result<(u64, u64, bool), Error> {
        let Some(header) = decode_meta_header(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        if header.type_tag != TYPE_JSON {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some((expire_ms, version, Structure::Json(json))) = decode_entry(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        Ok((expire_ms, version, json == JSON_INDEXED_MARKER))
    }

    pub(in crate::store::db) fn decode_legacy_json_document(
        raw: &[u8],
    ) -> Result<JsonValue, Error> {
        let Some((_, _, Structure::Json(json))) = decode_entry(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        if json == JSON_INDEXED_MARKER {
            return Err(Error::msg("Type parsing error"));
        }
        serde_json::from_str(&json).map_err(|_| Error::msg("Type parsing error"))
    }

    pub(in crate::store::db) fn read_json_node(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonNode>, Error> {
        let Some(raw) = self
            .store
            .get_raw(&json_node_key(self.db_index, key, version, tokens))
        else {
            return Ok(None);
        };
        decode_json_node(&raw)
            .map(Some)
            .ok_or_else(|| Error::msg("Type parsing error"))
    }

    pub(in crate::store::db) async fn read_json_node_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonNode>, Error> {
        let Some(raw) = self
            .store
            .get_raw_async(&json_node_key(self.db_index, key, version, tokens))
            .await
        else {
            return Ok(None);
        };
        decode_json_node(&raw)
            .map(Some)
            .ok_or_else(|| Error::msg("Type parsing error"))
    }

    pub(in crate::store::db) fn json_node_exists(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> bool {
        self.store
            .contains_key(&json_node_key(self.db_index, key, version, tokens))
    }

    pub(in crate::store::db) async fn json_node_exists_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> bool {
        self.store
            .get_raw_async(&json_node_key(self.db_index, key, version, tokens))
            .await
            .is_some()
    }

    pub(in crate::store::db) fn read_json_value_at_path(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(node) = self.read_json_node(key, version, tokens)? else {
            return Ok(None);
        };
        match node {
            JsonNode::Scalar(raw) => json_scalar_to_value(&raw).map(Some),
            JsonNode::Object(fields) => {
                let mut object = serde_json::Map::new();
                for field in fields {
                    let mut child_tokens = tokens.to_vec();
                    child_tokens.push(JsonPathToken::Field(field.clone()));
                    if let Some(child) =
                        self.read_json_value_at_path(key, version, &child_tokens)?
                    {
                        object.insert(field, child);
                    }
                }
                Ok(Some(JsonValue::Object(object)))
            }
            JsonNode::Array(len) => {
                let mut array = Vec::with_capacity(len);
                for index in 0..len {
                    let mut child_tokens = tokens.to_vec();
                    child_tokens.push(JsonPathToken::Index(index));
                    let Some(child) = self.read_json_value_at_path(key, version, &child_tokens)?
                    else {
                        return Err(Error::msg("Type parsing error"));
                    };
                    array.push(child);
                }
                Ok(Some(JsonValue::Array(array)))
            }
        }
    }

    pub(in crate::store::db) async fn read_json_value_at_path_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(node) = self.read_json_node_async(key, version, tokens).await? else {
            return Ok(None);
        };
        match node {
            JsonNode::Scalar(raw) => json_scalar_to_value(&raw).map(Some),
            JsonNode::Object(fields) => {
                let mut object = serde_json::Map::new();
                for field in fields {
                    let mut child_tokens = tokens.to_vec();
                    child_tokens.push(JsonPathToken::Field(field.clone()));
                    if let Some(child) =
                        Box::pin(self.read_json_value_at_path_async(key, version, &child_tokens))
                            .await?
                    {
                        object.insert(field, child);
                    }
                }
                Ok(Some(JsonValue::Object(object)))
            }
            JsonNode::Array(len) => {
                let mut array = Vec::with_capacity(len);
                for index in 0..len {
                    let mut child_tokens = tokens.to_vec();
                    child_tokens.push(JsonPathToken::Index(index));
                    let Some(child) =
                        Box::pin(self.read_json_value_at_path_async(key, version, &child_tokens))
                            .await?
                    else {
                        return Err(Error::msg("Type parsing error"));
                    };
                    array.push(child);
                }
                Ok(Some(JsonValue::Array(array)))
            }
        }
    }

    pub(in crate::store::db) fn json_type_indexed(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<&'static str>, Error> {
        let Some(node) = self.read_json_node(key, version, tokens)? else {
            return Ok(None);
        };
        Ok(Some(match node {
            JsonNode::Scalar(raw) => json_type_name(&json_scalar_to_value(&raw)?),
            JsonNode::Object(_) => "object",
            JsonNode::Array(_) => "array",
        }))
    }

    pub(in crate::store::db) async fn json_type_indexed_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<&'static str>, Error> {
        let Some(node) = self.read_json_node_async(key, version, tokens).await? else {
            return Ok(None);
        };
        Ok(Some(match node {
            JsonNode::Scalar(raw) => json_type_name(&json_scalar_to_value(&raw)?),
            JsonNode::Object(_) => "object",
            JsonNode::Array(_) => "array",
        }))
    }

    pub(in crate::store::db) fn touch_json_meta_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        expire_ms: u64,
        version: u64,
    ) {
        batch.put(
            &self.mk(key),
            &encode_entry(
                &Structure::Json(JSON_INDEXED_MARKER.to_string()),
                expire_ms,
                version,
            ),
        );
    }

    pub(in crate::store::db) fn write_json_value(
        &self,
        key: &str,
        value: &JsonValue,
        expire_ms: u64,
        version: u64,
    ) -> Result<(), Error> {
        let version = if version == 0 {
            self.next_persisted_version()
        } else {
            version
        };
        self.changes.fetch_add(1, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        delete_json_subtree_to_batch(&self.store, &mut batch, self.db_index, key, version, &[]);
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version);
        let mut path = Vec::new();
        write_json_subtree_to_batch(&mut batch, self.db_index, key, version, &mut path, value)?;
        if let Err(err) = self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key) {
            log::error!("failed to enqueue fulltext JSON upsert for {key}: {err}");
        }
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
        self.fulltext_request_json_refresh(key)?;
        Ok(())
    }

    pub(in crate::store::db) async fn write_json_value_cas_async(
        &self,
        key: &str,
        value: &JsonValue,
        expire_ms: u64,
        version: u64,
        cas_condition: CompareCondition,
    ) -> Result<bool, Error> {
        let version = if version == 0 {
            self.next_persisted_version_async().await
        } else {
            version
        };
        let mut batch = WriteBatch::new();
        delete_json_subtree_to_batch(&self.store, &mut batch, self.db_index, key, version, &[]);
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version);
        let mut path = Vec::new();
        write_json_subtree_to_batch(&mut batch, self.db_index, key, version, &mut path, value)?;
        if let Err(err) = self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key) {
            log::error!("failed to enqueue fulltext JSON upsert for {key}: {err}");
        }
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        if self
            .compare_and_write_batch_if_not_empty_async(&[cas_condition], &batch)
            .await?
        {
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_json_refresh(key)?;
            return Ok(true);
        }
        Ok(false)
    }
}
