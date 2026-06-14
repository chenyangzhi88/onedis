impl Db {
    fn logical_keys(&self) -> Vec<String> {
        let prefix = db_prefix(self.db_index);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(k, _)| {
                // 去掉 2 字节 db 前缀后转为字符串；
                // 命名空间子键（hash/list/set/zset）包含 0xFF/0x00 等非 UTF-8 字节，
                // 会被 String::from_utf8 过滤掉，只保留主键。
                String::from_utf8(k[prefix.len()..].to_vec()).ok()
            })
            .collect()
    }

    async fn logical_keys_async(&self) -> Vec<String> {
        let prefix = db_prefix(self.db_index);
        self.store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k[prefix.len()..].to_vec()).ok())
            .collect()
    }

    pub async fn scan_string_prefix_async(
        &self,
        key_prefix: &str,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let prefix = main_key(self.db_index, key_prefix);
        let mut rows = Vec::new();
        for (raw_key, _) in self.store.scan_prefix_raw_async(&prefix).await {
            if rows.len() >= limit {
                break;
            }
            let Some(key_bytes) = raw_key.strip_prefix(&db_prefix(self.db_index)) else {
                continue;
            };
            let Ok(key) = String::from_utf8(key_bytes.to_vec()) else {
                continue;
            };
            if let Ok(Some(value)) = self.get_string_bytes_async(&key).await {
                rows.push((key, value));
            }
        }
        rows
    }

    fn read_hash_fields(&self, key: &str, version: u64) -> HashMap<String, String> {
        let mut hash = HashMap::new();

        for (field, value) in self.hash_entries_raw(key, version) {
            if let (Ok(field), Ok(value)) = (String::from_utf8(field), String::from_utf8(value)) {
                hash.insert(field, value);
            }
        }

        hash
    }

    fn read_set_members(&self, key: &str, version: u64) -> HashSet<String> {
        self.set_members_raw(key, version)
            .into_iter()
            .filter_map(|member| String::from_utf8(member).ok())
            .collect()
    }

    fn read_zset_members(&self, key: &str, version: u64) -> BTreeMap<String, f64> {
        self.zset_members_raw(key, version)
            .into_iter()
            .filter_map(|(member, value)| {
                match (String::from_utf8(member), decode_zset_score(&value)) {
                    (Ok(member), Some(score)) => Some((member, score)),
                    _ => None,
                }
            })
            .collect()
    }

    fn decode_rank_score(&self, key: &str, version: u64, rank_key: &[u8]) -> Option<f64> {
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let suffix = rank_key.strip_prefix(prefix.as_slice())?;
        if suffix.len() < 9 {
            return None;
        }
        let score_bytes: [u8; 8] = suffix[0..8].try_into().ok()?;
        Some(decode_sorted_f64(score_bytes))
    }

    fn decode_rank_member(&self, key: &str, version: u64, rank_key: &[u8]) -> Option<String> {
        let prefix = zset_rank_prefix(self.db_index, key, version);
        let suffix = rank_key.strip_prefix(prefix.as_slice())?;
        if suffix.len() < 9 || suffix[8] != 0x00 {
            return None;
        }
        String::from_utf8(suffix[9..].to_vec()).ok()
    }

    fn read_list_items(&self, key: &str, version: u64) -> Vec<String> {
        let prefix = list_item_prefix(self.db_index, key, version);
        let mut items: Vec<(i64, String)> = Vec::new();

        for (key_bytes, value_bytes) in self.store.scan_prefix_raw(&prefix) {
            let index_bytes = &key_bytes[prefix.len()..];
            if index_bytes.len() != 8 {
                continue;
            }

            let index = match <[u8; 8]>::try_from(index_bytes) {
                Ok(bytes) => i64::from_be_bytes(bytes),
                Err(_) => continue,
            };

            if let Ok(value) = String::from_utf8(value_bytes) {
                items.push((index, value));
            }
        }

        items.sort_by_key(|(index, _)| *index);
        items.into_iter().map(|(_, value)| value).collect()
    }

    fn read_stream_entries(&self, key: &str, version: u64) -> Vec<StreamEntry> {
        self.stream_entries_between(
            key,
            version,
            StreamId { ms: 0, seq: 0 },
            StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            },
        )
    }

    fn load_live_raw_for_db_with_backend(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Option<Vec<u8>> {
        let key_bytes = main_key(db_index, key);
        if let Some(raw) = store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 && now_ms() >= expire_ms {
                let mut batch = WriteBatch::new();
                Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, &raw);
                store.write_batch(&batch);
                return None;
            }
            return Some(raw);
        }
        None
    }

    fn copy_structure_between_dbs_to_batch(
        store: &KvStore,
        batch: &mut WriteBatch,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        raw: &[u8],
        version_counter: &VersionCounter,
    ) {
        let Some(header) = decode_meta_header(raw) else {
            return;
        };
        let target_version = Self::next_persisted_version_for_store(store, version_counter);

        if let Some(meta) = decode_list_meta(raw) {
            batch.put(
                &main_key(target_db_index, target_key),
                &encode_list_meta(meta.expire_ms, target_version, meta.head, meta.tail),
            );
            let source_prefix = list_item_prefix(source_db_index, source_key, meta.version);
            let target_prefix = list_item_prefix(target_db_index, target_key, target_version);
            for (item_key, value) in store.scan_prefix_raw(&source_prefix) {
                if let Some(suffix) = item_key.strip_prefix(source_prefix.as_slice()) {
                    let mut new_key = target_prefix.clone();
                    new_key.extend_from_slice(suffix);
                    batch.put(&new_key, &value);
                }
            }
            return;
        }

        if let Some(meta) = decode_stream_meta(raw) {
            batch.put(
                &main_key(target_db_index, target_key),
                &encode_stream_meta(StreamMeta {
                    version: target_version,
                    ..meta
                }),
            );
            for (source_ns, target_ns) in [
                (
                    stream_entry_prefix(source_db_index, source_key, meta.version),
                    stream_entry_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_group_prefix(source_db_index, source_key, meta.version),
                    stream_group_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_pel_prefix(source_db_index, source_key, meta.version),
                    stream_pel_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_consumer_prefix(source_db_index, source_key, meta.version),
                    stream_consumer_prefix(target_db_index, target_key, target_version),
                ),
            ] {
                for (source_key_bytes, value) in store.scan_prefix_raw(&source_ns) {
                    if let Some(suffix) = source_key_bytes.strip_prefix(source_ns.as_slice()) {
                        let mut new_key = target_ns.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
            }
            return;
        }

        match header.type_tag {
            TYPE_HASH => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_hash_meta(header.expire_ms, target_version),
                );
                let source_prefix = hash_field_prefix(source_db_index, source_key, header.version);
                let target_prefix = hash_field_prefix(target_db_index, target_key, target_version);
                for (field_key, value) in store.scan_prefix_raw(&source_prefix) {
                    if let Some(suffix) = field_key.strip_prefix(source_prefix.as_slice()) {
                        let mut new_key = target_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
                let source_expire_prefix =
                    hash_field_expire_prefix(source_db_index, source_key, header.version);
                let target_expire_prefix =
                    hash_field_expire_prefix(target_db_index, target_key, target_version);
                for (field_key, value) in store.scan_prefix_raw(&source_expire_prefix) {
                    if let Some(suffix) = field_key.strip_prefix(source_expire_prefix.as_slice()) {
                        let mut new_key = target_expire_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
            }
            TYPE_SORTED_SET => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_zset_meta(header.expire_ms, target_version),
                );
                let source_member_prefix =
                    zset_member_prefix(source_db_index, source_key, header.version);
                let target_member_prefix =
                    zset_member_prefix(target_db_index, target_key, target_version);
                for (member_key, value) in store.scan_prefix_raw(&source_member_prefix) {
                    if let Some(suffix) = member_key.strip_prefix(source_member_prefix.as_slice()) {
                        let mut new_key = target_member_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }

                let source_rank_prefix =
                    zset_rank_prefix(source_db_index, source_key, header.version);
                let target_rank_prefix =
                    zset_rank_prefix(target_db_index, target_key, target_version);
                for (rank_key, value) in store.scan_prefix_raw(&source_rank_prefix) {
                    if let Some(suffix) = rank_key.strip_prefix(source_rank_prefix.as_slice()) {
                        let mut new_key = target_rank_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
            }
            TYPE_SET => {
                let Some(meta) = decode_set_meta(raw) else {
                    return;
                };
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_set_meta(meta.expire_ms, target_version, meta.len),
                );
                let source_prefix = set_member_prefix(source_db_index, source_key, meta.version);
                let target_prefix = set_member_prefix(target_db_index, target_key, target_version);
                for (member_key, value) in store.scan_prefix_raw(&source_prefix) {
                    if let Some(suffix) = member_key.strip_prefix(source_prefix.as_slice()) {
                        let mut new_key = target_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
                for (source_prefix, target_prefix) in [
                    (
                        set_slot_prefix(source_db_index, source_key, meta.version),
                        set_slot_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        set_member_slot_prefix(source_db_index, source_key, meta.version),
                        set_member_slot_prefix(target_db_index, target_key, target_version),
                    ),
                ] {
                    for (source_key_bytes, value) in store.scan_prefix_raw(&source_prefix) {
                        if let Some(suffix) =
                            source_key_bytes.strip_prefix(source_prefix.as_slice())
                        {
                            let mut new_key = target_prefix.clone();
                            new_key.extend_from_slice(suffix);
                            batch.put(&new_key, &value);
                        }
                    }
                }
            }
            TYPE_JSON => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                let source_prefix = json_node_prefix(source_db_index, source_key, header.version);
                let target_prefix = json_node_prefix(target_db_index, target_key, target_version);
                for (node_key, value) in store.scan_prefix_raw(&source_prefix) {
                    if let Some(suffix) = node_key.strip_prefix(source_prefix.as_slice()) {
                        let mut new_key = target_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
            }
            TYPE_LIST => {
                // Should have been handled above via decode_list_meta, but handle for safety
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                let source_prefix = list_item_prefix(source_db_index, source_key, header.version);
                let target_prefix = list_item_prefix(target_db_index, target_key, target_version);
                for (item_key, value) in store.scan_prefix_raw(&source_prefix) {
                    if let Some(suffix) = item_key.strip_prefix(source_prefix.as_slice()) {
                        let mut new_key = target_prefix.clone();
                        new_key.extend_from_slice(suffix);
                        batch.put(&new_key, &value);
                    }
                }
            }
            TYPE_STREAM => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                for (source_prefix, target_prefix) in [
                    (
                        stream_entry_prefix(source_db_index, source_key, header.version),
                        stream_entry_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_group_prefix(source_db_index, source_key, header.version),
                        stream_group_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_pel_prefix(source_db_index, source_key, header.version),
                        stream_pel_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_consumer_prefix(source_db_index, source_key, header.version),
                        stream_consumer_prefix(target_db_index, target_key, target_version),
                    ),
                ] {
                    for (entry_key, value) in store.scan_prefix_raw(&source_prefix) {
                        if let Some(suffix) = entry_key.strip_prefix(source_prefix.as_slice()) {
                            let mut new_key = target_prefix.clone();
                            new_key.extend_from_slice(suffix);
                            batch.put(&new_key, &value);
                        }
                    }
                }
            }
            _ => {
                // String / Json / Vector — no sub-keys
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
            }
        }
    }

    async fn copy_prefixed_namespace_to_batch(
        store: &KvStore,
        batch: &mut WriteBatch,
        source_prefix: Vec<u8>,
        target_prefix: Vec<u8>,
    ) {
        for (source_key_bytes, value) in store.scan_prefix_raw_async(&source_prefix).await {
            if let Some(suffix) = source_key_bytes.strip_prefix(source_prefix.as_slice()) {
                let mut new_key = target_prefix.clone();
                new_key.extend_from_slice(suffix);
                batch.put(&new_key, &value);
            }
        }
    }

    async fn copy_structure_between_dbs_to_batch_async(
        store: &KvStore,
        batch: &mut WriteBatch,
        source_db_index: u16,
        source_key: &str,
        target_db_index: u16,
        target_key: &str,
        raw: &[u8],
        version_counter: &VersionCounter,
    ) {
        let Some(header) = decode_meta_header(raw) else {
            return;
        };
        let target_version = Self::next_persisted_version_for_store(store, version_counter);

        if let Some(meta) = decode_list_meta(raw) {
            batch.put(
                &main_key(target_db_index, target_key),
                &encode_list_meta(meta.expire_ms, target_version, meta.head, meta.tail),
            );
            Self::copy_prefixed_namespace_to_batch(
                store,
                batch,
                list_item_prefix(source_db_index, source_key, meta.version),
                list_item_prefix(target_db_index, target_key, target_version),
            )
            .await;
            return;
        }

        if let Some(meta) = decode_stream_meta(raw) {
            batch.put(
                &main_key(target_db_index, target_key),
                &encode_stream_meta(StreamMeta {
                    version: target_version,
                    ..meta
                }),
            );
            for (source_ns, target_ns) in [
                (
                    stream_entry_prefix(source_db_index, source_key, meta.version),
                    stream_entry_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_group_prefix(source_db_index, source_key, meta.version),
                    stream_group_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_pel_prefix(source_db_index, source_key, meta.version),
                    stream_pel_prefix(target_db_index, target_key, target_version),
                ),
                (
                    stream_consumer_prefix(source_db_index, source_key, meta.version),
                    stream_consumer_prefix(target_db_index, target_key, target_version),
                ),
            ] {
                Self::copy_prefixed_namespace_to_batch(store, batch, source_ns, target_ns).await;
            }
            return;
        }

        match header.type_tag {
            TYPE_HASH => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_hash_meta(header.expire_ms, target_version),
                );
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    hash_field_prefix(source_db_index, source_key, header.version),
                    hash_field_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    hash_field_expire_prefix(source_db_index, source_key, header.version),
                    hash_field_expire_prefix(target_db_index, target_key, target_version),
                )
                .await;
            }
            TYPE_SORTED_SET => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_zset_meta(header.expire_ms, target_version),
                );
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    zset_member_prefix(source_db_index, source_key, header.version),
                    zset_member_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    zset_rank_prefix(source_db_index, source_key, header.version),
                    zset_rank_prefix(target_db_index, target_key, target_version),
                )
                .await;
            }
            TYPE_SET => {
                let Some(meta) = decode_set_meta(raw) else {
                    return;
                };
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_set_meta(meta.expire_ms, target_version, meta.len),
                );
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    set_member_prefix(source_db_index, source_key, meta.version),
                    set_member_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    set_slot_prefix(source_db_index, source_key, meta.version),
                    set_slot_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    set_member_slot_prefix(source_db_index, source_key, meta.version),
                    set_member_slot_prefix(target_db_index, target_key, target_version),
                )
                .await;
            }
            TYPE_JSON => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    json_node_prefix(source_db_index, source_key, header.version),
                    json_node_prefix(target_db_index, target_key, target_version),
                )
                .await;
            }
            TYPE_LIST => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                Self::copy_prefixed_namespace_to_batch(
                    store,
                    batch,
                    list_item_prefix(source_db_index, source_key, header.version),
                    list_item_prefix(target_db_index, target_key, target_version),
                )
                .await;
            }
            TYPE_STREAM => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                for (source_prefix, target_prefix) in [
                    (
                        stream_entry_prefix(source_db_index, source_key, header.version),
                        stream_entry_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_group_prefix(source_db_index, source_key, header.version),
                        stream_group_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_pel_prefix(source_db_index, source_key, header.version),
                        stream_pel_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        stream_consumer_prefix(source_db_index, source_key, header.version),
                        stream_consumer_prefix(target_db_index, target_key, target_version),
                    ),
                ] {
                    Self::copy_prefixed_namespace_to_batch(
                        store,
                        batch,
                        source_prefix,
                        target_prefix,
                    )
                    .await;
                }
            }
            _ => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
            }
        }
    }

    fn delete_structure_for_db_to_batch(
        batch: &mut WriteBatch,
        db_index: u16,
        key: &str,
        raw: &[u8],
    ) {
        let key_bytes = main_key(db_index, key);
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(raw) {
            delete_sub_keys_to_batch(batch, db_index, key, header.version, header.type_tag);
        }
    }

    fn write_structure(&self, key: &str, value: &Structure, expire_ms: u64, version: u64) {
        let mut batch = WriteBatch::new();
        if version > 0 {}
        // Clean up old version's sub-keys if overwriting
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes) {
            if let Some(old_header) = decode_meta_header(&raw) {
                if old_header.expire_ms > 0 && old_header.expire_ms != expire_ms {
                    self.ttl_manager.remove_known_to_batch(
                        &mut batch,
                        old_header.expire_ms,
                        self.db_index,
                        key,
                    );
                }
                if old_header.version != version {
                    delete_sub_keys_to_batch(
                        &mut batch,
                        self.db_index,
                        key,
                        old_header.version,
                        old_header.type_tag,
                    );
                }
            }
        }
        Self::write_structure_to_batch(&mut batch, self.db_index, key, value, expire_ms, version);
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(&mut batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager
                .remove_to_batch(&mut batch, self.db_index, key);
        }
        self.write_batch_if_not_empty(&batch);
    }

    fn decode_json_meta(raw: &[u8]) -> Result<(u64, u64, bool), Error> {
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

    fn decode_legacy_json_document(raw: &[u8]) -> Result<JsonValue, Error> {
        let Some((_, _, Structure::Json(json))) = decode_entry(raw) else {
            return Err(Error::msg("Type parsing error"));
        };
        if json == JSON_INDEXED_MARKER {
            return Err(Error::msg("Type parsing error"));
        }
        serde_json::from_str(&json).map_err(|_| Error::msg("Type parsing error"))
    }

    fn read_json_node(
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

    async fn read_json_node_async(
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

    fn json_node_exists(&self, key: &str, version: u64, tokens: &[JsonPathToken]) -> bool {
        self.store
            .contains_key(&json_node_key(self.db_index, key, version, tokens))
    }

    async fn json_node_exists_async(
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

    fn read_json_value_at_path(
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

    async fn read_json_value_at_path_async(
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

    fn json_type_indexed(
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

    async fn json_type_indexed_async(
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

    fn touch_json_meta_to_batch(
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

    fn write_json_value(
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

    async fn write_json_value_cas_async(
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

    fn write_string(&self, key: &str, value: &[u8], expire_ms: u64) {
        let mut batch = WriteBatch::new();
        self.write_string_to_batch(&mut batch, key, value, expire_ms);
        self.write_batch_if_not_empty(&batch);
    }

    async fn write_string_async(
        &self,
        key: &str,
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let mut batch = WriteBatch::new();
        self.write_string_to_batch_with_old_raw(&mut batch, key, value, expire_ms, old_raw);
        self.write_batch_if_not_empty_async(&batch).await;
    }

    fn write_string_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
    ) {
        let old_raw = if self.version_counter.current() == 0 {
            None
        } else {
            self.store.get_raw(&self.mk(key))
        };
        self.write_string_to_batch_with_old_raw(batch, key, value, expire_ms, old_raw.as_deref());
    }

    fn write_string_to_batch_with_old_raw(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let key_bytes = self.mk(key);
        self.cleanup_old_complex_subkeys_for_string_overwrite(batch, key, old_raw);
        batch.put(&key_bytes, &encode_raw_string(value, expire_ms));
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager.remove_to_batch(batch, self.db_index, key);
        }
    }

    fn write_string_byte_key_to_batch_with_old_raw(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let key_bytes = main_key_bytes(self.db_index, key);
        self.cleanup_old_complex_subkeys_for_string_byte_key_overwrite(batch, key, old_raw);
        batch.put(&key_bytes, &encode_raw_string(value, expire_ms));
        if expire_ms > 0
            && let Ok(key) = std::str::from_utf8(key)
        {
            self.ttl_manager
                .add_to_batch(batch, expire_ms, self.db_index, key);
        } else if let Some(header) = old_raw.and_then(decode_meta_header)
            && header.expire_ms > 0
            && let Ok(key) = std::str::from_utf8(key)
        {
            self.ttl_manager
                .remove_known_to_batch(batch, header.expire_ms, self.db_index, key);
        }
    }

    fn cleanup_old_complex_subkeys_for_string_overwrite(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        old_raw: Option<&[u8]>,
    ) {
        let Some(raw) = old_raw else {
            return;
        };
        let Some(header) = decode_meta_header(raw) else {
            return;
        };
        if header.type_tag != TYPE_STRING && header.version > 0 {
            delete_sub_keys_to_batch(batch, self.db_index, key, header.version, header.type_tag);
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(batch, key) {
                        log::error!(
                            "failed to enqueue fulltext delete for overwritten {key}: {err}"
                        );
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(batch, key) {
                        log::error!(
                            "failed to enqueue fulltext JSON delete for overwritten {key}: {err}"
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn cleanup_old_complex_subkeys_for_string_byte_key_overwrite(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        old_raw: Option<&[u8]>,
    ) {
        let Some(raw) = old_raw else {
            return;
        };
        let Some(header) = decode_meta_header(raw) else {
            return;
        };
        if header.type_tag != TYPE_STRING && header.version > 0 {
            delete_sub_keys_to_batch_bytes(
                batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
        }
    }

    fn write_structure_to_batch(
        batch: &mut WriteBatch,
        db_index: u16,
        key: &str,
        value: &Structure,
        expire_ms: u64,
        version: u64,
    ) {
        match value {
            Structure::String(value) => {
                batch.put(
                    &main_key(db_index, key),
                    &encode_raw_string(value.as_bytes(), expire_ms),
                );
            }
            Structure::Hash(hash) => {
                batch.put(
                    &main_key(db_index, key),
                    &encode_hash_meta(expire_ms, version),
                );

                for (field, value) in hash {
                    batch.put(
                        &hash_field_key(db_index, key, version, field),
                        value.as_bytes(),
                    );
                }
            }
            Structure::SortedSet(set) => {
                batch.put(
                    &main_key(db_index, key),
                    &encode_zset_meta(expire_ms, version),
                );

                for (member, score) in set {
                    batch.put(
                        &zset_member_key(db_index, key, version, member),
                        &score.to_be_bytes(),
                    );
                    batch.put(
                        &zset_rank_key(db_index, key, version, *score, member),
                        INDEX_MARKER_VALUE,
                    );
                }
            }
            Structure::Set(set) => {
                batch.put(
                    &main_key(db_index, key),
                    &encode_set_meta(expire_ms, version, set.len()),
                );

                for (slot, member) in set.iter().enumerate() {
                    batch.put(
                        &set_member_key(db_index, key, version, member),
                        INDEX_MARKER_VALUE,
                    );
                    batch.put(
                        &set_slot_key(db_index, key, version, slot as u64),
                        member.as_bytes(),
                    );
                    batch.put(
                        &set_member_slot_key(db_index, key, version, member.as_bytes()),
                        &(slot as u64).to_be_bytes(),
                    );
                }
            }
            Structure::List(list) => {
                batch.put(
                    &main_key(db_index, key),
                    &encode_list_meta(expire_ms, version, 0, list.len() as i64),
                );

                for (index, value) in list.iter().enumerate() {
                    batch.put(
                        &list_item_key(db_index, key, version, index as i64),
                        value.as_bytes(),
                    );
                }
            }
            Structure::Stream(entries) => {
                let mut last_id = StreamId { ms: 0, seq: 0 };
                let mut encoded_entries = Vec::new();
                for entry in entries {
                    if let Some(id) = StreamId::parse(&entry.id)
                        && id > last_id
                    {
                        encoded_entries.push((id, entry.fields.clone()));
                        last_id = id;
                    }
                }
                batch.put(
                    &main_key(db_index, key),
                    &encode_stream_meta(StreamMeta {
                        expire_ms,
                        version,
                        last_id,
                        length: encoded_entries.len() as u64,
                        entries_added: encoded_entries.len() as u64,
                    }),
                );
                for (id, fields) in encoded_entries {
                    batch.put(
                        &stream_entry_key(db_index, key, version, id),
                        &encode_stream_entry(&fields),
                    );
                }
            }
            _ => {
                let encoded = encode_entry(value, expire_ms, version);
                batch.put(&main_key(db_index, key), &encoded);
            }
        }
    }

    fn remove_internal(&self, key: &str, count_change: bool) -> Option<Structure> {
        let key_bytes = self.mk(key);
        let raw = self.store.get_raw(&key_bytes)?.clone();

        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
        }

        if let Some(meta) = decode_list_meta(&raw) {
            let list = self.read_list_items(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
            self.write_batch_if_not_empty(&batch);
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::List(list));
        }

        if let Some(meta) = decode_stream_meta(&raw) {
            let entries = self.read_stream_entries(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_STREAM);
            self.write_batch_if_not_empty(&batch);
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::Stream(entries));
        }

        let (_, version, structure) = decode_entry(&raw)?;
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }

        let type_tag = structure_type_tag(&structure);
        let result = match &structure {
            Structure::Hash(_) => {
                let hash = self.read_hash_fields(key, version);
                Some(Structure::Hash(hash))
            }
            Structure::SortedSet(_) => {
                let set = self.read_zset_members(key, version);
                Some(Structure::SortedSet(set))
            }
            Structure::Set(_) => {
                let set = self.read_set_members(key, version);
                Some(Structure::Set(set))
            }
            Structure::List(_) => {
                let list = self.read_list_items(key, version);
                Some(Structure::List(list))
            }
            Structure::Stream(_) => {
                let entries = self.read_stream_entries(key, version);
                Some(Structure::Stream(entries))
            }
            _ => Some(structure),
        };

        delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, type_tag);
        if type_tag == TYPE_JSON {
            delete_json_nodes_to_batch(&self.store, &mut batch, self.db_index, key, version);
        }
        self.write_batch_if_not_empty(&batch);
        result
    }

    async fn remove_internal_async(&self, key: &str, count_change: bool) -> Option<Structure> {
        let key_bytes = self.mk(key);
        let raw = self.store.get_raw(&key_bytes)?.clone();

        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
        }

        if let Some(meta) = decode_list_meta(&raw) {
            let list = self.read_list_items(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_LIST);
            self.write_batch_if_not_empty_async(&batch).await;
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::List(list));
        }

        if let Some(meta) = decode_stream_meta(&raw) {
            let entries = self.read_stream_entries(key, meta.version);
            delete_sub_keys_to_batch(&mut batch, self.db_index, key, meta.version, TYPE_STREAM);
            self.write_batch_if_not_empty_async(&batch).await;
            if count_change {
                self.changes.fetch_add(1, Ordering::Relaxed);
            }
            return Some(Structure::Stream(entries));
        }

        let (_, version, structure) = decode_entry(&raw)?;
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }

        let type_tag = structure_type_tag(&structure);
        let result = match &structure {
            Structure::Hash(_) => {
                let hash = self.read_hash_fields(key, version);
                Some(Structure::Hash(hash))
            }
            Structure::SortedSet(_) => {
                let set = self.read_zset_members(key, version);
                Some(Structure::SortedSet(set))
            }
            Structure::Set(_) => {
                let set = self.read_set_members(key, version);
                Some(Structure::Set(set))
            }
            Structure::List(_) => {
                let list = self.read_list_items(key, version);
                Some(Structure::List(list))
            }
            Structure::Stream(_) => {
                let entries = self.read_stream_entries(key, version);
                Some(Structure::Stream(entries))
            }
            _ => Some(structure),
        };

        delete_sub_keys_to_batch(&mut batch, self.db_index, key, version, type_tag);
        if type_tag == TYPE_JSON {
            delete_json_nodes_to_batch(&self.store, &mut batch, self.db_index, key, version);
        }
        self.write_batch_if_not_empty_async(&batch).await;
        result
    }

    fn delete_key_internal(&self, key: &str, count_change: bool) -> bool {
        let key_bytes = self.mk(key);
        let Some(raw) = self.store.get_raw(&key_bytes).map(|raw| raw.clone()) else {
            return false;
        };
        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            if header.type_tag == TYPE_JSON {
                delete_json_nodes_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                );
            }
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext delete for {key}: {err}");
                        return false;
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext JSON delete for {key}: {err}");
                        return false;
                    }
                }
                _ => {}
            }
        }
        self.write_batch_if_not_empty(&batch);
        if let Some(header) = decode_meta_header(&raw) {
            let refresh = match header.type_tag {
                TYPE_HASH => self.fulltext_request_refresh(key),
                TYPE_JSON => self.fulltext_request_json_refresh(key),
                _ => Ok(()),
            };
            if let Err(err) = refresh {
                log::error!("failed to refresh fulltext delete for {key}: {err}");
            }
        }
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    async fn delete_key_internal_async(&self, key: &str, count_change: bool) -> bool {
        let key_bytes = self.mk(key);
        let Some(raw) = self.store.get_raw(&key_bytes).map(|raw| raw.clone()) else {
            return false;
        };
        let mut batch = WriteBatch::new();
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(&raw) {
            self.ttl_manager.remove_known_to_batch(
                &mut batch,
                header.expire_ms,
                self.db_index,
                key,
            );
            delete_sub_keys_to_batch(
                &mut batch,
                self.db_index,
                key,
                header.version,
                header.type_tag,
            );
            if header.type_tag == TYPE_JSON {
                delete_json_nodes_to_batch(
                    &self.store,
                    &mut batch,
                    self.db_index,
                    key,
                    header.version,
                );
            }
            match header.type_tag {
                TYPE_HASH => {
                    if let Err(err) = self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext delete for {key}: {err}");
                        return false;
                    }
                }
                TYPE_JSON => {
                    if let Err(err) = self.fulltext_enqueue_json_delete_to_batch(&mut batch, key) {
                        log::error!("failed to enqueue fulltext JSON delete for {key}: {err}");
                        return false;
                    }
                }
                _ => {}
            }
        }
        self.write_batch_if_not_empty_async(&batch).await;
        if let Some(header) = decode_meta_header(&raw) {
            let refresh = match header.type_tag {
                TYPE_HASH => self.fulltext_request_refresh(key),
                TYPE_JSON => self.fulltext_request_json_refresh(key),
                _ => Ok(()),
            };
            if let Err(err) = refresh {
                log::error!("failed to refresh fulltext delete for {key}: {err}");
            }
        }
        if count_change {
            self.changes.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

}
