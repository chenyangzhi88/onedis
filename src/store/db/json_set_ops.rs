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
    fn try_json_set_packed(
        &self,
        key: &str,
        tokens: &[JsonPathToken],
        new_value: &JsonValue,
        condition: SetCondition,
    ) -> Result<Option<bool>, Error> {
        let key_bytes = self.mk(key);
        for _ in 0..SMALL_INLINE_CAS_ATTEMPTS {
            let observed = self.store.get_raw_observed(&key_bytes);
            let Some(raw) = observed.value() else {
                return Ok(None);
            };
            let Some(mut document) = decode_packed_json(raw) else {
                return Ok(None);
            };
            let target_exists = json_value_at_path(&document, tokens).is_some();
            let condition_matches = match condition {
                SetCondition::Always => true,
                SetCondition::Nx => !target_exists,
                SetCondition::Xx => target_exists,
            };
            if !condition_matches {
                return Ok(Some(false));
            }
            if !set_json_value_at_path(&mut document, tokens, new_value.clone()) {
                return Ok(Some(false));
            }
            let encoded_bytes = serde_json::to_vec(&document)?.len();
            validate_json_value_limits(&document, encoded_bytes)?;
            let header = decode_meta_header(raw).ok_or_else(|| Error::msg("Type parsing error"))?;
            let mut batch = WriteBatch::new();
            self.write_json_document_to_batch(
                &mut batch,
                key,
                &document,
                header.expire_ms,
                self.next_version(),
            )?;
            self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
            if self.compare_and_write_batch_if_not_empty(
                &[CompareCondition::from_observed(&observed)],
                &batch,
            )? {
                self.changes.fetch_add(1, Ordering::Relaxed);
                self.fulltext_request_json_refresh(key)?;
                return Ok(Some(true));
            }
        }
        self.promote_packed_json(key)?;
        Ok(None)
    }

    /// Applies every JSON.MSET entry to an in-memory view first, then publishes
    /// all affected documents with one conditional kv-engine batch. A missing
    /// path, wrong type, invalid value, or write conflict leaves every key
    /// unchanged.
    pub(crate) async fn json_mset_atomic_async(
        &self,
        entries: &[(&str, &str, &str)],
    ) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut parsed = Vec::with_capacity(entries.len());
        for (key, path, raw_value) in entries {
            let tokens = parse_json_path(path)?;
            let value: JsonValue = serde_json::from_str(raw_value)
                .map_err(|_| Error::msg("ERR invalid JSON value"))?;
            validate_json_value_limits(&value, raw_value.len())?;
            parsed.push((*key, tokens, value));
        }

        let shards = unique_key_write_lock_shards(
            self.db_index,
            parsed.iter().map(|(key, _, _)| key.as_bytes()),
        );
        let _write_guards = self.lock_write_shards(&shards).await;

        let mut keys = Vec::<&str>::new();
        let mut positions = HashMap::<&str, usize>::new();
        for (key, _, _) in &parsed {
            if !positions.contains_key(key) {
                positions.insert(key, keys.len());
                keys.push(key);
            }
        }
        for key in &keys {
            self.expire_if_needed_async(key).await;
        }

        let raw_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let observations = self.store.multi_get_raw_observed_async(&raw_keys).await;
        let mut expires = Vec::with_capacity(keys.len());
        let mut documents = Vec::with_capacity(keys.len());
        for (key, observation) in keys.iter().zip(&observations) {
            let Some(raw) = observation.value() else {
                expires.push(0);
                documents.push(None);
                continue;
            };
            let (expire_ms, version) = Self::decode_json_meta(raw)?;
            let document = self
                .read_json_value_at_path_async(key, version, &[])
                .await?
                .ok_or_else(|| Error::msg("Type parsing error"))?;
            expires.push(expire_ms);
            documents.push(Some(document));
        }

        for (key, tokens, value) in parsed {
            let position = positions[key];
            if tokens.is_empty() {
                documents[position] = Some(value);
                continue;
            }
            let document = documents[position]
                .as_mut()
                .ok_or_else(|| Error::msg("ERR path does not exist"))?;
            if !set_json_value_at_path(document, &tokens, value) {
                return Err(Error::msg("ERR path does not exist"));
            }
        }

        let mut batch = WriteBatch::new();
        for (position, key) in keys.iter().enumerate() {
            let document = documents[position]
                .as_ref()
                .ok_or_else(|| Error::msg("ERR path does not exist"))?;
            let encoded_bytes = serde_json::to_vec(document)?.len();
            validate_json_value_limits(document, encoded_bytes)?;
            let version = self.next_version_async().await;
            self.write_json_document_to_batch(
                &mut batch,
                key,
                document,
                expires[position],
                version,
            )?;
            self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
            if expires[position] > 0 {
                self.ttl_manager.try_add_to_batch(
                    &mut batch,
                    expires[position],
                    self.db_index,
                    key,
                )?;
            }
        }
        let conditions = observations
            .iter()
            .map(CompareCondition::from_observed)
            .collect::<Vec<_>>();
        if !self
            .compare_and_write_batch_if_not_empty_async(&conditions, &batch)
            .await?
        {
            return Err(Error::msg("ERR json write conflict"));
        }
        self.changes
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        for key in keys {
            self.fulltext_request_json_refresh(key)?;
        }
        Ok(())
    }

    /// Atomically read, transform and replace one JSON subtree.  The exclusive
    /// structure lock coordinates with JSON.SET's shared subtree path, while
    /// the observed metadata and node conditions protect against storage-level
    /// races.  Non-root updates only rewrite the selected subtree.
    pub(crate) async fn json_update_value_async<R, F>(
        &self,
        key: &str,
        path: &str,
        update: F,
    ) -> Result<Option<R>, Error>
    where
        F: FnOnce(&mut JsonValue) -> Result<R, Error>,
    {
        let tokens = parse_json_path(path)?;
        let _structure_guard = self.set_write_lock(key).lock().await;
        self.expire_if_needed_async(key).await;

        let key_bytes = self.mk(key);
        let observed = self.store.get_raw_observed_async(&key_bytes).await;
        let Some(raw) = observed.value() else {
            return Ok(None);
        };
        let (expire_ms, version) = Self::decode_json_meta(raw)?;
        if version == 0 {
            let mut document =
                decode_packed_json(raw).ok_or_else(|| Error::msg("Type parsing error"))?;
            let Some(value) = json_value_at_path_mut(&mut document, &tokens) else {
                return Ok(None);
            };
            let result = update(value)?;
            validate_json_value_limits(&document, serde_json::to_vec(&document)?.len())?;
            let mut batch = WriteBatch::new();
            self.write_json_document_to_batch(
                &mut batch,
                key,
                &document,
                expire_ms,
                self.next_version_async().await,
            )?;
            self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
            if !self
                .compare_and_write_batch_if_not_empty_async(
                    &[CompareCondition::from_observed(&observed)],
                    &batch,
                )
                .await?
            {
                return Err(Error::msg("ERR json write conflict"));
            }
            self.changes.fetch_add(1, Ordering::Relaxed);
            self.fulltext_request_json_refresh(key)?;
            return Ok(Some(result));
        }
        let Some(mut value) = self
            .read_json_value_at_path_async(key, version, &tokens)
            .await?
        else {
            return Ok(None);
        };
        let result = update(&mut value)?;
        validate_json_value_limits(&value, serde_json::to_vec(&value)?.len())?;
        let meta_condition = CompareCondition::from_observed(&observed);

        let committed = if tokens.is_empty() {
            self.write_json_value_cas_async(
                key,
                &value,
                expire_ms,
                self.next_version_async().await,
                meta_condition,
            )
            .await?
        } else {
            self.json_set_indexed_async(JsonIndexedSetRequest {
                key,
                version,
                tokens: &tokens,
                new_value: value,
                condition: SetCondition::Always,
                meta_condition,
            })
            .await?
            .unwrap_or(false)
        };
        if !committed {
            return Err(Error::msg("ERR json write conflict"));
        }
        Ok(Some(result))
    }

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
                let value = serde_json::from_str::<JsonValue>(json)
                    .map_err(|_| Error::msg("ERR invalid JSON value"))?;
                validate_json_value_limits(&value, json.len())?;
                Ok(value)
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
            if let Err(error) = self.write_json_document_to_batch(
                &mut key_batch,
                key,
                value,
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
        validate_json_value_limits(&new_value, json.len())?;

        self.expire_if_needed(key);
        if let Some(result) = self.try_json_set_packed(key, &tokens, &new_value, condition)? {
            return Ok(result);
        }
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
        validate_json_value_limits(&new_value, json.len())?;

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

            if version == 0 {
                let mut document =
                    decode_packed_json(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
                let target_exists = json_value_at_path(&document, &tokens).is_some();
                let condition_matches = match condition {
                    SetCondition::Always => true,
                    SetCondition::Nx => !target_exists,
                    SetCondition::Xx => target_exists,
                };
                if !condition_matches {
                    return Ok(false);
                }
                if !set_json_value_at_path(&mut document, &tokens, new_value.clone()) {
                    return Ok(false);
                }
                let encoded_bytes = serde_json::to_vec(&document)?.len();
                validate_json_value_limits(&document, encoded_bytes)?;
                let expire_ms = decode_meta_header(&raw)
                    .ok_or_else(|| Error::msg("Type parsing error"))?
                    .expire_ms;
                let mut batch = WriteBatch::new();
                self.write_json_document_to_batch(
                    &mut batch,
                    key,
                    &document,
                    expire_ms,
                    self.next_version_async().await,
                )?;
                self.fulltext_enqueue_json_upsert_to_batch(&mut batch, key)?;
                if self
                    .compare_and_write_batch_if_not_empty_async(&[meta_condition], &batch)
                    .await?
                {
                    self.changes.fetch_add(1, Ordering::Relaxed);
                    self.fulltext_request_json_refresh(key)?;
                    return Ok(true);
                }
                if attempt + 1 == SMALL_INLINE_CAS_ATTEMPTS {
                    self.promote_packed_json_async(key).await?;
                }
                continue;
            }

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

fn set_json_value_at_path(
    root: &mut JsonValue,
    tokens: &[JsonPathToken],
    new_value: JsonValue,
) -> bool {
    let Some((last, parents)) = tokens.split_last() else {
        *root = new_value;
        return true;
    };
    let mut current = root;
    for token in parents {
        current = match (token, current) {
            (JsonPathToken::Field(field), JsonValue::Object(object)) => {
                let Some(next) = object.get_mut(field) else {
                    return false;
                };
                next
            }
            (JsonPathToken::Index(index), JsonValue::Array(array)) => {
                let Some(next) = array.get_mut(*index) else {
                    return false;
                };
                next
            }
            _ => return false,
        };
    }
    match (last, current) {
        (JsonPathToken::Field(field), JsonValue::Object(object)) => {
            object.insert(field.clone(), new_value);
            true
        }
        (JsonPathToken::Index(index), JsonValue::Array(array)) if *index < array.len() => {
            array[*index] = new_value;
            true
        }
        _ => false,
    }
}
