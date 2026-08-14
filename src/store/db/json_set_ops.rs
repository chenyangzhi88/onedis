use super::*;

pub(in crate::store::db) struct JsonIndexedSetRequest<'a> {
    key: &'a str,
    version: u64,
    tokens: &'a [JsonPathToken],
    new_value: JsonValue,
    condition: SetCondition,
    meta_condition: CompareCondition,
}

impl Db {
    /// Apply unconditional root JSON.SET commands as one ordered storage batch. Only the last
    /// valid root value for each key is materialized; every command still receives its own reply.
    pub(crate) async fn json_set_root_batch_async(
        &self,
        commands: &[(&str, &str)],
    ) -> Vec<Result<(), Error>> {
        if commands.is_empty() {
            return Vec::new();
        }
        let mut replies = commands
            .iter()
            .map(|(_, json)| {
                serde_json::from_str::<JsonValue>(json)
                    .map_err(|_| Error::msg("ERR invalid JSON value"))
            })
            .collect::<Vec<_>>();
        let valid_keys = commands
            .iter()
            .zip(&replies)
            .filter_map(|((key, _), value)| value.as_ref().ok().map(|_| *key))
            .collect::<Vec<_>>();
        if valid_keys.is_empty() {
            return replies
                .into_iter()
                .map(|result| result.map(|_| ()))
                .collect();
        }
        let shards = unique_key_write_lock_shards(
            self.db_index,
            valid_keys.iter().map(|key| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;
        let mut keys = Vec::<&str>::new();
        let mut positions = HashMap::<&str, usize>::new();
        for key in valid_keys {
            if !positions.contains_key(key) {
                positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        for key in &keys {
            self.expire_if_needed_async(key).await;
        }
        let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let old_values = self.store.multi_get_raw_async(&raw_keys).await;
        let mut versions = Vec::with_capacity(keys.len());
        let mut expires = Vec::with_capacity(keys.len());
        let mut eligible = Vec::with_capacity(keys.len());
        for old_raw in &old_values {
            let header = old_raw.as_deref().and_then(decode_meta_header);
            let key_is_json = old_raw.is_none()
                || header.is_some_and(|header| header.type_tag == TYPE_JSON)
                    && old_raw
                        .as_deref()
                        .is_some_and(|raw| Self::decode_json_meta(raw).is_ok());
            eligible.push(key_is_json);
            versions.push(self.next_version());
            expires.push(header.map(|header| header.expire_ms).unwrap_or(0));
        }
        let mut final_command_indexes = vec![None; keys.len()];
        for (command_index, (key, _)) in commands.iter().enumerate() {
            let position = positions[key];
            if replies[command_index].is_ok() && eligible[position] {
                final_command_indexes[position] = Some(command_index);
            } else if replies[command_index].is_ok() {
                replies[command_index] = Err(Error::msg(WRONG_TYPE_ERROR));
            }
        }

        let mut batch = WriteBatch::new();
        let mut committed_keys = Vec::with_capacity(keys.len());
        for (position, key) in keys.iter().enumerate() {
            let Some(command_index) = final_command_indexes[position] else {
                continue;
            };
            let value = replies[command_index]
                .as_ref()
                .expect("final JSON batch command was parsed successfully");
            let mut key_batch = WriteBatch::new();
            if let Err(error) = self.touch_json_meta_to_batch(
                &mut key_batch,
                key,
                expires[position],
                versions[position],
            ) {
                let message = error.to_string();
                for (index, (command_key, _)) in commands.iter().enumerate() {
                    if command_key == key && replies[index].is_ok() {
                        replies[index] = Err(Error::msg(message.clone()));
                    }
                }
                continue;
            }
            let mut path = Vec::new();
            if let Err(error) = write_json_subtree_to_batch(
                &mut key_batch,
                self.db_index,
                key,
                versions[position],
                &mut path,
                value,
            ) {
                let message = error.to_string();
                for (index, (command_key, _)) in commands.iter().enumerate() {
                    if command_key == key && replies[index].is_ok() {
                        replies[index] = Err(Error::msg(message.clone()));
                    }
                }
                continue;
            }
            if let Err(error) = self.fulltext_enqueue_json_upsert_to_batch(&mut key_batch, key) {
                let message = error.to_string();
                for (index, (command_key, _)) in commands.iter().enumerate() {
                    if command_key == key && replies[index].is_ok() {
                        replies[index] = Err(Error::msg(message.clone()));
                    }
                }
                continue;
            }
            if expires[position] > 0 {
                if let Err(error) = self.ttl_manager.try_add_to_batch(
                    &mut key_batch,
                    expires[position],
                    self.db_index,
                    key,
                ) {
                    let message = error.to_string();
                    for (index, (command_key, _)) in commands.iter().enumerate() {
                        if command_key == key && replies[index].is_ok() {
                            replies[index] = Err(Error::msg(message.clone()));
                        }
                    }
                    continue;
                }
            }
            if batch.count() == 0 {
                batch = key_batch;
                committed_keys.push(*key);
            } else if let Err(error) = batch.append_batch(key_batch) {
                let message = error.to_string();
                for (index, (command_key, _)) in commands.iter().enumerate() {
                    if command_key == key && replies[index].is_ok() {
                        replies[index] = Err(Error::msg(message.clone()));
                    }
                }
            } else {
                committed_keys.push(*key);
            }
        }
        self.write_batch_with_logical_keys_if_not_empty_async(&batch, &committed_keys)
            .await;
        self.changes.fetch_add(
            replies.iter().filter(|value| value.is_ok()).count() as u64,
            Ordering::Relaxed,
        );
        for key in keys {
            if let Err(error) = self.fulltext_request_json_refresh(key) {
                log::error!("failed to refresh fulltext JSON source {key}: {error}");
            }
        }
        replies
            .into_iter()
            .map(|result| result.map(|_| ()))
            .collect()
    }

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
            self.write_json_value(key, &new_value, 0, self.next_version())?;
            return Ok(true);
        };

        let (expire_ms, version) = Self::decode_json_meta(&raw)?;

        if tokens.is_empty() {
            if condition == SetCondition::Nx {
                return Ok(false);
            }
            self.write_json_value(key, &new_value, expire_ms, self.next_version())?;
            return Ok(true);
        }

        self.json_set_indexed(key, version, &tokens, new_value, condition)
    }

    pub(in crate::store::db) fn json_set_indexed(
        &self,
        key: &str,
        version: u64,
        tokens: &[JsonPathToken],
        new_value: JsonValue,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        let Some((parent_tokens, storage_tokens, mut parent_node)) =
            self.resolve_json_storage_target(key, version, tokens)?
        else {
            return Ok(false);
        };
        let target_exists = self.json_node_exists(key, version, &storage_tokens);
        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !target_exists,
            SetCondition::Xx => target_exists,
        };
        if !condition_matches {
            return Ok(false);
        }

        let mut batch = WriteBatch::new();
        if target_exists {
            match self.read_json_node(key, version, &storage_tokens)? {
                Some(JsonNode::Scalar(_)) => batch
                    .delete(&json_node_key(self.db_index, key, version, &storage_tokens))
                    .map_err(|error| Error::msg(error.to_string()))?,
                Some(_) => delete_json_subtree_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    version,
                    &storage_tokens,
                )?,
                None => return Ok(false),
            }
        } else if let JsonNode::Object(generation) = &mut parent_node {
            *generation = generation.wrapping_add(1);
            batch
                .put(
                    &json_node_key(self.db_index, key, version, &parent_tokens),
                    &encode_json_node(&parent_node),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let mut path = storage_tokens;
        write_json_subtree_to_batch(
            &mut batch,
            self.db_index,
            key,
            version,
            &mut path,
            &new_value,
        )?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
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
            version,
            tokens,
            new_value,
            condition,
            meta_condition,
        } = request;
        let Some(mut observed_target) = self
            .observe_json_storage_target_async(key, version, tokens)
            .await?
        else {
            return Ok(Some(false));
        };
        let target_exists = observed_target.target_node.is_some();
        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !target_exists,
            SetCondition::Xx => target_exists,
        };
        if !condition_matches {
            return Ok(Some(false));
        }

        let mut batch = WriteBatch::new();
        if target_exists {
            if matches!(observed_target.target_node, Some(JsonNode::Scalar(_))) {
                batch
                    .delete(&json_node_key(
                        self.db_index,
                        key,
                        version,
                        &observed_target.target_tokens,
                    ))
                    .map_err(|error| Error::msg(error.to_string()))?;
            } else {
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
            }
        } else if let JsonNode::Object(generation) = &mut observed_target.parent_node {
            *generation = generation.wrapping_add(1);
            batch
                .put(
                    &json_node_key(self.db_index, key, version, &observed_target.parent_tokens),
                    &encode_json_node(&observed_target.parent_node),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let mut path = observed_target.target_tokens;
        write_json_subtree_to_batch(
            &mut batch,
            self.db_index,
            key,
            version,
            &mut path,
            &new_value,
        )?;
        self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
        observed_target.conditions.push(meta_condition);
        if self
            .compare_and_write_batch_if_not_empty_async(&observed_target.conditions, &batch)
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

        if tokens.is_empty() {
            let _write_guard = self.set_write_lock(key).lock().await;
            return self.json_set_root_async(key, new_value, condition).await;
        }

        let _structural_guard = self.set_write_lock(key).read().await;
        let parent_lock_route = format!("json:{:?}", &tokens[..tokens.len() - 1]);
        let mut conflict_guard = None;

        for attempt in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let meta_condition = CompareCondition::from_observed(&observed);
            let Some(raw) = observed.value().map(|value| value.to_vec()) else {
                return Ok(false);
            };

            let (_, version) = Self::decode_json_meta(&raw)?;

            match self
                .json_set_indexed_async(JsonIndexedSetRequest {
                    key,
                    version,
                    tokens: &tokens,
                    new_value: new_value.clone(),
                    condition,
                    meta_condition,
                })
                .await?
            {
                Some(result) => return Ok(result),
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

    async fn json_set_root_async(
        &self,
        key: &str,
        new_value: JsonValue,
        condition: SetCondition,
    ) -> Result<bool, Error> {
        for _ in 0..64 {
            self.expire_if_needed_async(key).await;
            let key_bytes = self.mk(key);
            let observed = self.store.get_raw_observed_async(&key_bytes).await;
            let meta_condition = CompareCondition::from_observed(&observed);
            let expire_ms = match observed.value() {
                Some(raw) => {
                    let (expire_ms, _) = Self::decode_json_meta(raw)?;
                    if condition == SetCondition::Nx {
                        return Ok(false);
                    }
                    expire_ms
                }
                None => {
                    if condition == SetCondition::Xx {
                        return Ok(false);
                    }
                    0
                }
            };
            if self
                .write_json_value_cas_async(
                    key,
                    &new_value,
                    expire_ms,
                    self.next_version_async().await,
                    meta_condition,
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(Error::msg("ERR json write conflict"))
    }
}
