impl Db {
    pub fn flushdb(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            batch.delete_range(&prefix, &end);
        } else {
            for (key, _) in self.store.scan_prefix_raw(&prefix) {
                batch.delete(&key);
            }
        }
        self.ttl_manager
            .remove_db_to_batch(&mut batch, self.db_index);
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }

    pub async fn flushdb_async(&self) {
        let prefix = db_prefix(self.db_index);
        let mut batch = WriteBatch::new();
        if let Some(end) = db_prefix_exclusive_upper_bound(self.db_index) {
            batch.delete_range(&prefix, &end);
        } else {
            for (key, _) in self.store.scan_prefix_raw_async(&prefix).await {
                batch.delete(&key);
            }
        }
        self.ttl_manager
            .remove_db_to_batch_async(&mut batch, self.db_index)
            .await;
        if batch.count() > 0 {
            self.write_batch_if_not_empty(&batch);
        }
        self.fulltext_clear_runtimes_for_db();
    }

    // ========================================================================
    // 核心 KV 操作方法
    // ========================================================================

    /**
     * 插入键值（重置过期时间）
     *
     * 用于 SET 等命令，插入后过期时间清零。
     */
    pub fn insert(&self, key: String, value: Structure) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        if let Structure::String(value) = value {
            self.write_string(&key, value.as_bytes(), 0);
            return;
        }
        let version = self.next_persisted_version();
        self.write_structure(&key, &value, 0, version);
    }

    pub fn insert_string(&self, key: String, value: String, ttl_ms: Option<u64>) {
        self.insert_string_bytes(key, value.into_bytes(), ttl_ms);
    }

    pub async fn insert_string_async(&self, key: String, value: String, ttl_ms: Option<u64>) {
        self.insert_string_bytes_async(key, value.into_bytes(), ttl_ms)
            .await;
    }

    pub fn insert_string_bytes(&self, key: String, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let expire_ms = ttl_ms.map_or(0, |ttl| now_ms().saturating_add(ttl));
        self.write_string(&key, &value, expire_ms);
    }

    pub async fn insert_string_bytes_async(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let expire_ms = ttl_ms.map_or(0, |ttl| now_ms().saturating_add(ttl));
        let old_raw = if self.version_counter.current() == 0 {
            None
        } else {
            self.store.get_raw_async(&self.mk(&key)).await
        };
        self.write_string_async(&key, &value, expire_ms, old_raw.as_deref())
            .await;
    }

    pub fn set_string_bytes(
        &self,
        key: String,
        value: Vec<u8>,
        expiration: SetExpiration,
        condition: SetCondition,
        return_old: bool,
    ) -> Result<SetOutcome, Error> {
        self.expire_if_needed(&key);
        let old_raw = self.store.get_raw(&self.mk(&key));
        let old_header = old_raw.as_deref().and_then(decode_meta_header);
        let exists = old_header.is_some();

        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !exists,
            SetCondition::Xx => exists,
        };
        if !condition_matches {
            return Ok(SetOutcome::NotSet);
        }

        let old_value = if return_old {
            match old_raw.as_deref() {
                Some(raw) => {
                    let Some(header) = old_header else {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    };
                    if header.type_tag != TYPE_STRING {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    Some(decode_string_bytes(raw).ok_or_else(|| Error::msg("Type parsing error"))?)
                }
                None => None,
            }
        } else {
            None
        };

        let expire_ms = match expiration {
            SetExpiration::Clear => 0,
            SetExpiration::KeepTtl => old_header.map_or(0, |header| header.expire_ms),
            SetExpiration::At(expire_ms) => expire_ms,
        };

        self.changes.fetch_add(1, Ordering::Relaxed);
        if expire_ms > 0 && now_ms() >= expire_ms {
            self.remove_internal(&key, false);
        } else {
            self.write_string(&key, &value, expire_ms);
        }

        Ok(SetOutcome::Set { old_value })
    }

    pub async fn set_string_bytes_async(
        &self,
        key: String,
        value: Vec<u8>,
        expiration: SetExpiration,
        condition: SetCondition,
        return_old: bool,
    ) -> Result<SetOutcome, Error> {
        self.expire_if_needed_async(&key).await;
        let old_raw = self.store.get_raw_async(&self.mk(&key)).await;
        let old_header = old_raw.as_deref().and_then(decode_meta_header);
        let exists = old_header.is_some();

        let condition_matches = match condition {
            SetCondition::Always => true,
            SetCondition::Nx => !exists,
            SetCondition::Xx => exists,
        };
        if !condition_matches {
            return Ok(SetOutcome::NotSet);
        }

        let old_value = if return_old {
            match old_raw.as_deref() {
                Some(raw) => {
                    let Some(header) = old_header else {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    };
                    if header.type_tag != TYPE_STRING {
                        return Err(Error::msg(WRONG_TYPE_ERROR));
                    }
                    Some(decode_string_bytes(raw).ok_or_else(|| Error::msg("Type parsing error"))?)
                }
                None => None,
            }
        } else {
            None
        };

        let expire_ms = match expiration {
            SetExpiration::Clear => 0,
            SetExpiration::KeepTtl => old_header.map_or(0, |header| header.expire_ms),
            SetExpiration::At(expire_ms) => expire_ms,
        };

        self.changes.fetch_add(1, Ordering::Relaxed);
        if expire_ms > 0 && now_ms() >= expire_ms {
            self.remove_internal_async(&key, false).await;
        } else {
            self.write_string_async(&key, &value, expire_ms, old_raw.as_deref())
                .await;
        }

        Ok(SetOutcome::Set { old_value })
    }

    pub fn insert_string_ref(&self, key: &str, value: &str) {
        self.insert_string_bytes_ref(key, value.as_bytes());
    }

    pub fn insert_string_bytes_ref(&self, key: &str, value: &[u8]) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        self.write_string(key, value, 0);
    }

    pub async fn insert_string_bytes_refs_async(&self, key_vals: &[(&str, &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        if self.version_counter.current() == 0 {
            for (key, value) in key_vals {
                self.write_string_to_batch_with_old_raw(&mut batch, key, value, 0, None);
            }
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| self.mk(key))
                .collect::<Vec<_>>();
            let old_raws = self.store.multi_get_raw_async(&keys).await;
            for ((key, value), old_raw) in key_vals.iter().zip(old_raws) {
                self.write_string_to_batch_with_old_raw(
                    &mut batch,
                    key,
                    value,
                    0,
                    old_raw.as_deref(),
                );
            }
        }
        self.write_batch_if_not_empty_async(&batch).await;
    }

    pub async fn insert_string_bytes_refs_without_watch_publish_async(
        &self,
        key_vals: &[(&str, &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        if self.version_counter.current() == 0 {
            for (key, value) in key_vals {
                self.write_string_to_batch_with_old_raw(&mut batch, key, value, 0, None);
            }
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| self.mk(key))
                .collect::<Vec<_>>();
            let old_raws = self.store.multi_get_raw_async(&keys).await;
            for ((key, value), old_raw) in key_vals.iter().zip(old_raws) {
                self.write_string_to_batch_with_old_raw(
                    &mut batch,
                    key,
                    value,
                    0,
                    old_raw.as_deref(),
                );
            }
        }
        self.write_batch_if_not_empty_without_watch_publish_async(&batch)
            .await;
    }

    pub async fn insert_string_byte_keys_async(&self, key_vals: &[(&[u8], &[u8])]) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        if self.version_counter.current() == 0 {
            for (key, value) in key_vals {
                self.write_string_byte_key_to_batch_with_old_raw(&mut batch, key, value, 0, None);
            }
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| main_key_bytes(self.db_index, key))
                .collect::<Vec<_>>();
            let old_raws = self.store.multi_get_raw_async(&keys).await;
            for ((key, value), old_raw) in key_vals.iter().zip(old_raws) {
                self.write_string_byte_key_to_batch_with_old_raw(
                    &mut batch,
                    key,
                    value,
                    0,
                    old_raw.as_deref(),
                );
            }
        }
        self.write_batch_if_not_empty_async(&batch).await;
    }

    pub async fn insert_string_byte_keys_without_watch_publish_async(
        &self,
        key_vals: &[(&[u8], &[u8])],
    ) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        if self.version_counter.current() == 0 {
            for (key, value) in key_vals {
                self.write_string_byte_key_to_batch_with_old_raw(&mut batch, key, value, 0, None);
            }
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| main_key_bytes(self.db_index, key))
                .collect::<Vec<_>>();
            let old_raws = self.store.multi_get_raw_async(&keys).await;
            for ((key, value), old_raw) in key_vals.iter().zip(old_raws) {
                self.write_string_byte_key_to_batch_with_old_raw(
                    &mut batch,
                    key,
                    value,
                    0,
                    old_raw.as_deref(),
                );
            }
        }
        self.write_batch_if_not_empty_without_watch_publish_async(&batch)
            .await;
    }

    pub fn insert_strings(&self, key_vals: Vec<(String, String)>) {
        self.insert_string_bytes_many(
            key_vals
                .into_iter()
                .map(|(key, value)| (key, value.into_bytes()))
                .collect(),
        );
    }

    pub fn insert_string_bytes_many(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let old_raws = if self.version_counter.current() == 0 {
            vec![None; key_vals.len()]
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| self.mk(key))
                .collect::<Vec<_>>();
            self.store.multi_get_raw(&keys)
        };
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.into_iter().zip(old_raws) {
            self.write_string_to_batch_with_old_raw(
                &mut batch,
                &key,
                &value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_batch_if_not_empty(&batch);
    }

    pub async fn insert_string_bytes_many_async(&self, key_vals: Vec<(String, Vec<u8>)>) {
        if key_vals.is_empty() {
            return;
        }
        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let old_raws = if self.version_counter.current() == 0 {
            vec![None; key_vals.len()]
        } else {
            let keys = key_vals
                .iter()
                .map(|(key, _)| self.mk(key))
                .collect::<Vec<_>>();
            self.store.multi_get_raw_async(&keys).await
        };
        let mut batch = WriteBatch::new();
        for ((key, value), old_raw) in key_vals.into_iter().zip(old_raws) {
            self.write_string_to_batch_with_old_raw(
                &mut batch,
                &key,
                &value,
                0,
                old_raw.as_deref(),
            );
        }
        self.write_batch_if_not_empty_async(&batch).await;
    }

    pub fn insert_string_bytes_many_nx(&self, key_vals: Vec<(String, Vec<u8>)>) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        for (key, _) in &key_vals {
            self.expire_if_needed(key);
            if self.store.contains_key(&self.mk(key)) {
                return false;
            }
        }

        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch(&mut batch, &key, &value, 0);
        }
        self.write_batch_if_not_empty(&batch);
        true
    }

    pub async fn insert_string_bytes_many_nx_async(
        &self,
        key_vals: Vec<(String, Vec<u8>)>,
    ) -> bool {
        if key_vals.is_empty() {
            return false;
        }
        for (key, _) in &key_vals {
            self.expire_if_needed_async(key).await;
        }
        let keys = key_vals
            .iter()
            .map(|(key, _)| self.mk(key))
            .collect::<Vec<_>>();
        if self
            .store
            .multi_get_raw_async(&keys)
            .await
            .iter()
            .any(Option::is_some)
        {
            return false;
        }

        self.changes
            .fetch_add(key_vals.len() as u64, Ordering::Relaxed);
        let mut batch = WriteBatch::new();
        for (key, value) in key_vals {
            self.write_string_to_batch(&mut batch, &key, &value, 0);
        }
        self.write_batch_if_not_empty_async(&batch).await;
        true
    }

    /**
     * 更新键值（保留原有过期时间）
     *
     * 用于 INCR、LPUSH 等修改数据但不改变 TTL 的命令。
     * 替代原来的 get_mut() 就地修改模式。
     */
    pub fn update(&self, key: String, value: Structure) {
        self.changes.fetch_add(1, Ordering::Relaxed);
        let (expire_ms, version) = self.get_expire_and_version(&key);
        self.write_structure(&key, &value, expire_ms, version);
    }

    /**
     * 获取键值（返回 owned Structure）
     *
     * 自动进行惰性过期检测。
     */
    pub fn get(&self, key: &str) -> Option<Structure> {
        self.expire_if_needed(key);
        let raw = self.store.get_raw(&self.mk(key))?.clone();
        if let Some(meta) = decode_list_meta(&raw) {
            return Some(Structure::List(self.read_list_items(key, meta.version)));
        }
        if let Some(meta) = decode_stream_meta(&raw) {
            return Some(Structure::Stream(
                self.read_stream_entries(key, meta.version),
            ));
        }
        if let Some(meta) = decode_set_meta(&raw) {
            return Some(Structure::Set(self.read_set_members(key, meta.version)));
        }
        if let Some(meta) = decode_hash_meta(&raw) {
            return Some(Structure::Hash(self.read_hash_fields(key, meta.version)));
        }
        let (_, version, structure) = decode_entry(&raw)?;
        match structure {
            Structure::Hash(_) => Some(Structure::Hash(self.read_hash_fields(key, version))),
            Structure::SortedSet(_) => {
                Some(Structure::SortedSet(self.read_zset_members(key, version)))
            }
            Structure::Set(_) => Some(Structure::Set(self.read_set_members(key, version))),
            Structure::List(_) => Some(Structure::List(self.read_list_items(key, version))),
            Structure::Stream(_) => Some(Structure::Stream(self.read_stream_entries(key, version))),
            Structure::Json(json) if json == JSON_INDEXED_MARKER => self
                .read_json_value_at_path(key, version, &[])
                .ok()
                .flatten()
                .and_then(|value| serde_json::to_string(&value).ok())
                .map(Structure::Json),
            other => Some(other),
        }
    }

    pub fn get_string(&self, key: &str) -> Result<Option<String>, Error> {
        match self.get_string_bytes(key)? {
            Some(value) => String::from_utf8(value)
                .map(Some)
                .map_err(|_| Error::msg("Type parsing error")),
            None => Ok(None),
        }
    }

    pub async fn get_string_async(&self, key: &str) -> Result<Option<String>, Error> {
        match self.get_string_bytes_async(key).await? {
            Some(value) => String::from_utf8(value)
                .map(Some)
                .map_err(|_| Error::msg("Type parsing error")),
            None => Ok(None),
        }
    }

    pub fn get_string_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw(key) else {
            return Ok(None);
        };
        if let Some(value) = decode_string_bytes(&raw) {
            Ok(Some(value))
        } else {
            Err(Error::msg("Type parsing error"))
        }
    }

    pub async fn get_string_bytes_async(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let Some(raw) = self.read_live_raw_async(key).await else {
            return Ok(None);
        };
        if let Some(value) = decode_string_bytes(&raw) {
            Ok(Some(value))
        } else {
            Err(Error::msg("Type parsing error"))
        }
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

    fn json_set_indexed(
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

    async fn json_set_indexed_async(
        &self,
        key: &str,
        expire_ms: u64,
        version: u64,
        tokens: &[JsonPathToken],
        new_value: JsonValue,
        condition: SetCondition,
        cas_condition: CompareCondition,
    ) -> Result<Option<bool>, Error> {
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
            let cas_condition = CompareCondition::from_observed(key_bytes, &observed);
            let Some(raw) = observed.value.as_ref().map(|value| value.to_vec()) else {
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
                .json_set_indexed_async(
                    key,
                    expire_ms,
                    version,
                    &tokens,
                    new_value.clone(),
                    condition,
                    cas_condition,
                )
                .await?
            {
                Some(result) => return Ok(result),
                None => continue,
            }
        }

        Err(Error::msg("ERR json write conflict"))
    }

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
            let cas_condition = CompareCondition::from_observed(key_bytes, &observed);
            let Some(raw) = observed.value.as_ref().map(|value| value.to_vec()) else {
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

    fn json_del_indexed(
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

    async fn json_del_indexed_async(
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
