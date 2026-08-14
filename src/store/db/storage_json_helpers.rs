use super::*;

pub(in crate::store::db) struct JsonObservedTarget {
    pub(in crate::store::db) parent_tokens: Vec<JsonPathToken>,
    pub(in crate::store::db) target_tokens: Vec<JsonPathToken>,
    pub(in crate::store::db) parent_node: JsonNode,
    pub(in crate::store::db) target_node: Option<JsonNode>,
    pub(in crate::store::db) conditions: Vec<CompareCondition>,
}

impl Db {
    pub(in crate::store::db) fn decode_json_meta(raw: &[u8]) -> Result<(u64, u64), Error> {
        let Some(header) = decode_meta_header(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        if header.type_tag != TYPE_JSON {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some((expire_ms, version, Structure::Json(json))) = decode_entry(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        if json != JSON_INDEXED_MARKER {
            return Err(Error::msg("Type parsing error"));
        }
        Ok((expire_ms, version))
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

    /// Convert user-visible array ranks into stable physical element ids while walking the path.
    /// Object field names already are physical path components.
    pub(in crate::store::db) fn resolve_json_storage_path(
        &self,
        key: &str,
        version: u64,
        query_tokens: &[JsonPathToken],
    ) -> Result<Option<Vec<JsonPathToken>>, Error> {
        let mut storage_tokens = Vec::with_capacity(query_tokens.len());
        for token in query_tokens {
            match token {
                JsonPathToken::Field(field) => {
                    storage_tokens.push(JsonPathToken::Field(field.clone()));
                }
                JsonPathToken::Index(index) => {
                    let Some(JsonNode::Array(element_ids)) =
                        self.read_json_node(key, version, &storage_tokens)?
                    else {
                        return Ok(None);
                    };
                    let Some(element_id) = element_ids.get(*index).copied() else {
                        return Ok(None);
                    };
                    let physical_id = usize::try_from(element_id)
                        .map_err(|_| Error::msg("Type parsing error"))?;
                    storage_tokens.push(JsonPathToken::Index(physical_id));
                }
            }
        }
        Ok(Some(storage_tokens))
    }

    pub(in crate::store::db) async fn resolve_json_storage_path_async(
        &self,
        key: &str,
        version: u64,
        query_tokens: &[JsonPathToken],
    ) -> Result<Option<Vec<JsonPathToken>>, Error> {
        let mut storage_tokens = Vec::with_capacity(query_tokens.len());
        for token in query_tokens {
            match token {
                JsonPathToken::Field(field) => {
                    storage_tokens.push(JsonPathToken::Field(field.clone()));
                }
                JsonPathToken::Index(index) => {
                    let Some(JsonNode::Array(element_ids)) = self
                        .read_json_node_async(key, version, &storage_tokens)
                        .await?
                    else {
                        return Ok(None);
                    };
                    let Some(element_id) = element_ids.get(*index).copied() else {
                        return Ok(None);
                    };
                    let physical_id = usize::try_from(element_id)
                        .map_err(|_| Error::msg("Type parsing error"))?;
                    storage_tokens.push(JsonPathToken::Index(physical_id));
                }
            }
        }
        Ok(Some(storage_tokens))
    }

    pub(in crate::store::db) fn resolve_json_storage_target(
        &self,
        key: &str,
        version: u64,
        query_tokens: &[JsonPathToken],
    ) -> Result<Option<(Vec<JsonPathToken>, Vec<JsonPathToken>, JsonNode)>, Error> {
        let Some((last, query_parent)) = query_tokens.split_last() else {
            return Ok(None);
        };
        let Some(storage_parent) = self.resolve_json_storage_path(key, version, query_parent)?
        else {
            return Ok(None);
        };
        let Some(parent_node) = self.read_json_node(key, version, &storage_parent)? else {
            return Ok(None);
        };
        let mut storage_target = storage_parent.clone();
        match (last, &parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(_)) => {
                storage_target.push(JsonPathToken::Field(field.clone()));
            }
            (JsonPathToken::Index(index), JsonNode::Array(element_ids)) => {
                let Some(element_id) = element_ids.get(*index).copied() else {
                    return Ok(None);
                };
                storage_target.push(JsonPathToken::Index(
                    usize::try_from(element_id).map_err(|_| Error::msg("Type parsing error"))?,
                ));
            }
            _ => return Ok(None),
        }
        Ok(Some((storage_parent, storage_target, parent_node)))
    }

    /// Resolve a user path and observe every structural node used by that resolution. Array
    /// directory changes, ancestor replacement, and same-target writes therefore invalidate the
    /// CAS, while writes to independent object fields do not touch any shared structural node.
    pub(in crate::store::db) async fn observe_json_storage_target_async(
        &self,
        key: &str,
        version: u64,
        query_tokens: &[JsonPathToken],
    ) -> Result<Option<JsonObservedTarget>, Error> {
        let Some((last, query_parent)) = query_tokens.split_last() else {
            return Ok(None);
        };
        let mut parent_tokens = Vec::with_capacity(query_parent.len());
        let mut conditions = Vec::with_capacity(query_tokens.len() + 1);
        for token in query_parent {
            let node_key = json_node_key(self.db_index, key, version, &parent_tokens);
            let observed = self.store.get_raw_observed_async(&node_key).await;
            let Some(raw) = observed.value() else {
                return Ok(None);
            };
            let node = decode_json_node(raw).ok_or_else(|| Error::msg("Type parsing error"))?;
            conditions.push(CompareCondition::from_observed(&observed));
            match (token, node) {
                (JsonPathToken::Field(field), JsonNode::Object(_)) => {
                    parent_tokens.push(JsonPathToken::Field(field.clone()));
                }
                (JsonPathToken::Index(index), JsonNode::Array(element_ids)) => {
                    let Some(element_id) = element_ids.get(*index).copied() else {
                        return Ok(None);
                    };
                    parent_tokens.push(JsonPathToken::Index(
                        usize::try_from(element_id)
                            .map_err(|_| Error::msg("Type parsing error"))?,
                    ));
                }
                _ => return Ok(None),
            }
        }

        let parent_key = json_node_key(self.db_index, key, version, &parent_tokens);
        let parent_observed = self.store.get_raw_observed_async(&parent_key).await;
        let Some(parent_raw) = parent_observed.value() else {
            return Ok(None);
        };
        let parent_node =
            decode_json_node(parent_raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        conditions.push(CompareCondition::from_observed(&parent_observed));

        let mut target_tokens = parent_tokens.clone();
        match (last, &parent_node) {
            (JsonPathToken::Field(field), JsonNode::Object(_)) => {
                target_tokens.push(JsonPathToken::Field(field.clone()));
            }
            (JsonPathToken::Index(index), JsonNode::Array(element_ids)) => {
                let Some(element_id) = element_ids.get(*index).copied() else {
                    return Ok(None);
                };
                target_tokens.push(JsonPathToken::Index(
                    usize::try_from(element_id).map_err(|_| Error::msg("Type parsing error"))?,
                ));
            }
            _ => return Ok(None),
        }
        let target_key = json_node_key(self.db_index, key, version, &target_tokens);
        let target_observed = self.store.get_raw_observed_async(&target_key).await;
        let target_node = target_observed
            .value()
            .map(|raw| decode_json_node(raw).ok_or_else(|| Error::msg("Type parsing error")))
            .transpose()?;
        conditions.push(CompareCondition::from_observed(&target_observed));

        Ok(Some(JsonObservedTarget {
            parent_tokens,
            target_tokens,
            parent_node,
            target_node,
            conditions,
        }))
    }

    pub(in crate::store::db) fn read_json_value_at_path(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(storage_tokens) = self.resolve_json_storage_path(key, version, tokens)? else {
            return Ok(None);
        };
        self.read_json_value_at_storage_path(key, version, &storage_tokens)
    }

    pub(in crate::store::db) fn read_json_value_at_storage_path(
        &self,
        key: &str,
        version: u64,
        storage_tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(node) = self.read_json_node(key, version, storage_tokens)? else {
            return Ok(None);
        };
        match node {
            JsonNode::Scalar(raw) => json_scalar_to_value(&raw).map(Some),
            JsonNode::Object(_) | JsonNode::Array(_) => {
                let prefix = json_node_key(self.db_index, key, version, storage_tokens);
                let snapshot =
                    JsonNodeSnapshot::from_entries(&prefix, self.store.scan_prefix_raw(&prefix))?;
                snapshot.value_at(&mut Vec::new())
            }
        }
    }

    pub(in crate::store::db) async fn read_json_value_at_path_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(storage_tokens) = self
            .resolve_json_storage_path_async(key, version, tokens)
            .await?
        else {
            return Ok(None);
        };
        self.read_json_value_at_storage_path_async(key, version, &storage_tokens)
            .await
    }

    pub(in crate::store::db) async fn read_json_value_at_storage_path_async(
        &self,
        key: &str,
        version: u64,
        storage_tokens: &[JsonPathToken],
    ) -> Result<Option<JsonValue>, Error> {
        let Some(node) = self
            .read_json_node_async(key, version, storage_tokens)
            .await?
        else {
            return Ok(None);
        };
        match node {
            JsonNode::Scalar(raw) => json_scalar_to_value(&raw).map(Some),
            JsonNode::Object(_) | JsonNode::Array(_) => {
                let prefix = json_node_key(self.db_index, key, version, storage_tokens);
                let entries = self.store.scan_prefix_raw_async(&prefix).await;
                let snapshot = JsonNodeSnapshot::from_entries(&prefix, entries)?;
                snapshot.value_at(&mut Vec::new())
            }
        }
    }

    pub(in crate::store::db) fn json_type_indexed(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<Option<&'static str>, Error> {
        let Some(storage_tokens) = self.resolve_json_storage_path(key, version, tokens)? else {
            return Ok(None);
        };
        let Some(node) = self.read_json_node(key, version, &storage_tokens)? else {
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
        let Some(storage_tokens) = self
            .resolve_json_storage_path_async(key, version, tokens)
            .await?
        else {
            return Ok(None);
        };
        let Some(node) = self
            .read_json_node_async(key, version, &storage_tokens)
            .await?
        else {
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
    ) -> Result<(), Error> {
        batch
            .put(
                &self.mk(key),
                &encode_entry(
                    &Structure::Json(JSON_INDEXED_MARKER.to_string()),
                    expire_ms,
                    version,
                ),
            )
            .map_err(|error| Error::msg(error.to_string()))
    }

    pub(in crate::store::db) fn write_json_value(
        &self,
        key: &str,
        value: &JsonValue,
        expire_ms: u64,
        version: u64,
    ) -> Result<(), Error> {
        let version = if version == 0 {
            self.next_version()
        } else {
            version
        };
        self.changes.fetch_add(1, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version)?;
        let mut path = Vec::new();
        write_json_subtree_to_batch(&mut batch, self.db_index, key, version, &mut path, value)?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
        if expire_ms > 0 {
            self.ttl_manager
                .try_add_to_batch(&mut batch, expire_ms, self.db_index, key)
                .map_err(|error| Error::msg(error.to_string()))?;
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
            self.next_version_async().await
        } else {
            version
        };
        let mut batch = WriteBatch::new();
        self.touch_json_meta_to_batch(&mut batch, key, expire_ms, version)?;
        let mut path = Vec::new();
        write_json_subtree_to_batch(&mut batch, self.db_index, key, version, &mut path, value)?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
        if expire_ms > 0 {
            self.ttl_manager
                .try_add_to_batch(&mut batch, expire_ms, self.db_index, key)
                .map_err(|error| Error::msg(error.to_string()))?;
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
