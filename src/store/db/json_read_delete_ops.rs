use super::*;

impl Db {
    pub(crate) async fn json_get_batch_async(
        &self,
        commands: &[(&str, &str)],
    ) -> Vec<Result<Option<String>, Error>> {
        let mut key_positions = HashMap::new();
        let mut keys = Vec::new();
        for (key, _) in commands {
            if !key_positions.contains_key(key) {
                key_positions.insert(*key, keys.len());
                keys.push(*key);
            }
        }
        let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let metas = self.store.multi_get_raw_async(&meta_keys).await;
        let now = now_ms();
        let mut pair_positions = HashMap::new();
        let mut pairs = Vec::new();
        for pair in commands {
            if !pair_positions.contains_key(pair) {
                pair_positions.insert(*pair, pairs.len());
                pairs.push(*pair);
            }
        }
        let mut results = Vec::with_capacity(pairs.len());
        for (key, path) in pairs {
            let result = async {
                let tokens = parse_json_path(path)?;
                let Some(raw) = metas[key_positions[key]].as_deref() else {
                    return Ok(None);
                };
                let (expire_ms, version) = Self::decode_json_meta(raw)?;
                if expire_ms > 0 && now >= expire_ms {
                    return Ok(None);
                }
                let value = self
                    .read_json_value_at_path_async(key, version, &tokens)
                    .await?;
                value
                    .map(|value| {
                        serde_json::to_string(&value)
                            .map_err(|_| Error::msg("ERR failed to encode JSON value"))
                    })
                    .transpose()
            }
            .await;
            results.push(result.map_err(|error: Error| error.to_string()));
        }
        commands
            .iter()
            .map(|pair| match &results[pair_positions[pair]] {
                Ok(value) => Ok(value.clone()),
                Err(message) => Err(Error::msg(message.clone())),
            })
            .collect()
    }

    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<String>, Error> {
        let tokens = parse_json_path(path)?;
        self.expire_if_needed(key);
        let Some(raw) = self.store.get_raw(&self.mk(key)) else {
            return Ok(None);
        };
        let (_, version) = Self::decode_json_meta(&raw)?;
        let value = self.read_json_value_at_path(key, version, &tokens)?;
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
        let (_, version) = Self::decode_json_meta(&raw)?;
        let value = self
            .read_json_value_at_path_async(key, version, &tokens)
            .await?;
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
        let (_, version) = Self::decode_json_meta(&raw)?;
        if tokens.is_empty() {
            return Ok(i64::from(self.delete_key_internal(key, true)));
        }
        self.json_del_indexed(key, version, &tokens)
    }

    pub async fn json_del_async(&self, key: &str, path: &str) -> Result<i64, Error> {
        let tokens = parse_json_path(path)?;
        if tokens.is_empty() {
            let _write_guard = self.set_write_lock(key).lock().await;
            self.expire_if_needed_async(key).await;
            let Some(raw) = self.store.get_raw_async(&self.mk(key)).await else {
                return Ok(0);
            };
            Self::decode_json_meta(&raw)?;
            return Ok(i64::from(self.delete_key_internal_async(key, true).await));
        }
        let _structural_guard = self.set_write_lock(key).read().await;
        let parent_lock_route = format!("json:{:?}", &tokens[..tokens.len() - 1]);
        let mut conflict_guard = None;
        for attempt in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let cas_condition = CompareCondition::from_observed(&observed);
            let Some(raw) = observed.value().map(|value| value.to_vec()) else {
                return Ok(0);
            };
            let (_, version) = Self::decode_json_meta(&raw)?;
            match self
                .json_del_indexed_async(key, version, &tokens, cas_condition)
                .await?
            {
                Some(deleted) => return Ok(deleted),
                None => {
                    if attempt == 2 {
                        conflict_guard = Some(
                            self.hash_field_write_lock(key, &parent_lock_route)
                                .lock()
                                .await,
                        );
                    }
                    let _fallback_active = conflict_guard.is_some();
                    tokio::task::yield_now().await;
                }
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
        let (_, version) = Self::decode_json_meta(&raw)?;
        self.json_type_indexed(key, version, &tokens)
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
        let (_, version) = Self::decode_json_meta(&raw)?;
        self.json_type_indexed_async(key, version, &tokens).await
    }

    pub(in crate::store::db) fn json_del_indexed(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
    ) -> Result<i64, Error> {
        let Some((parent_tokens, storage_tokens, mut parent_node)) =
            self.resolve_json_storage_target(key, version, tokens)?
        else {
            return Ok(0);
        };
        if !self.json_node_exists(key, version, &storage_tokens) {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        delete_json_subtree_to_batch(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            version,
            &storage_tokens,
        )?;
        match (&tokens[tokens.len() - 1], &mut parent_node) {
            (JsonPathToken::Field(_), JsonNode::Object(generation)) => {
                *generation = generation.wrapping_add(1);
            }
            (JsonPathToken::Index(index), JsonNode::Array(element_ids)) => {
                element_ids.remove(*index);
            }
            _ => return Ok(0),
        }
        batch
            .put(
                &json_node_key(self.db_index, key, version, &parent_tokens),
                &encode_json_node(&parent_node),
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
        self.write_batch_if_not_empty(&batch);
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.fulltext_request_json_refresh(key)?;
        Ok(1)
    }

    pub(in crate::store::db) async fn json_del_indexed_async(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
        cas_condition: CompareCondition,
    ) -> Result<Option<i64>, Error> {
        let Some(mut observed_target) = self
            .observe_json_storage_target_async(key, version, tokens)
            .await?
        else {
            return Ok(Some(0));
        };
        if observed_target.target_node.is_none() {
            return Ok(Some(0));
        }

        let mut batch = WriteBatch::new();
        let Some(subtree_conditions) = observe_and_delete_json_subtree_to_batch_async(
            &self.store,
            &mut batch,
            self.db_index,
            key,
            version,
            &observed_target.target_tokens,
        )
        .await?
        else {
            return Ok(None);
        };
        observed_target.conditions.extend(subtree_conditions);
        match (&tokens[tokens.len() - 1], &mut observed_target.parent_node) {
            (JsonPathToken::Field(_), JsonNode::Object(generation)) => {
                *generation = generation.wrapping_add(1);
            }
            (JsonPathToken::Index(index), JsonNode::Array(element_ids)) => {
                element_ids.remove(*index);
            }
            _ => return Ok(Some(0)),
        }
        batch
            .put(
                &json_node_key(self.db_index, key, version, &observed_target.parent_tokens),
                &encode_json_node(&observed_target.parent_node),
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
        observed_target.conditions.push(cas_condition);
        if self
            .compare_and_write_batch_if_not_empty_async(&observed_target.conditions, &batch)
            .await?
        {
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_json_refresh(key)?;
            return Ok(Some(1));
        }
        Ok(None)
    }
}
