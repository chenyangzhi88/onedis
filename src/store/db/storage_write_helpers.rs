use super::*;

impl Db {
    pub(in crate::store::db) fn write_structure(
        &self,
        key: &str,
        value: &Structure,
        expire_ms: u64,
        version: u64,
    ) {
        let mut batch = WriteBatch::new();
        let version = if version == 0 && !matches!(value, Structure::String(_)) {
            self.next_version()
        } else {
            version
        };
        let key_bytes = self.mk(key);
        if let Some(raw) = self.store.get_raw(&key_bytes)
            && let Some(old_header) = decode_meta_header(&raw)
        {
            if old_header.expire_ms > 0 && old_header.expire_ms != expire_ms {
                self.ttl_manager.remove_known_to_batch(
                    &mut batch,
                    old_header.expire_ms,
                    self.db_index,
                    key,
                );
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

    pub(in crate::store::db) fn write_string(&self, key: &str, value: &[u8], expire_ms: u64) {
        let mut batch = WriteBatch::new();
        self.write_string_to_batch(&mut batch, key, value, expire_ms);
        self.write_batch_if_not_empty(&batch);
    }

    pub(in crate::store::db) fn write_plain_string(&self, key: &str, value: &[u8], expire_ms: u64) {
        let mut batch = WriteBatch::new();
        self.write_string_to_batch_with_old_raw(&mut batch, key, value, expire_ms, None);
        self.write_plain_string_batch_if_not_empty(&batch);
    }

    pub(in crate::store::db) fn write_string_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
    ) {
        self.write_string_to_batch_with_old_raw(batch, key, value, expire_ms, None);
    }

    pub(in crate::store::db) fn write_string_to_batch_with_old_raw(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let key_bytes = self.mk(key);
        let stored_raw = old_raw
            .is_none()
            .then(|| self.store.get_raw(&key_bytes))
            .flatten();
        if let Some(old_raw) = old_raw {
            self.prepare_string_overwrite_to_batch(batch, key, Some(old_raw));
        } else if let Some(stored_raw) = stored_raw.as_deref() {
            self.enqueue_fulltext_delete_for_string_overwrite(batch, key, stored_raw);
        }
        let effective_old_raw = old_raw.or(stored_raw.as_deref());
        self.write_string_value_to_batch(batch, key, value, expire_ms, effective_old_raw);
    }

    pub(in crate::store::db) fn write_string_to_batch_with_deferred_old_raw(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        if let Some(old_raw) = old_raw {
            self.enqueue_fulltext_delete_for_string_overwrite(batch, key, old_raw);
        }
        self.write_string_value_to_batch(batch, key, value, expire_ms, old_raw);
    }

    fn write_string_value_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let key_bytes = self.mk(key);
        if let Some(header) = old_raw.and_then(decode_meta_header)
            && header.expire_ms > 0
            && header.expire_ms != expire_ms
        {
            self.ttl_manager
                .remove_known_to_batch(batch, header.expire_ms, self.db_index, key);
        }
        (batch.put(&key_bytes, &encode_raw_string(value, expire_ms)))
            .expect("write batch append invariant violated");
        if expire_ms > 0 {
            self.ttl_manager
                .add_to_batch(batch, expire_ms, self.db_index, key);
        } else {
            self.ttl_manager.remove_to_batch(batch, self.db_index, key);
        }
    }

    pub(in crate::store::db) fn write_string_byte_key_to_batch_with_deferred_old_raw(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        if let Some(old_raw) = old_raw
            && let Ok(key) = std::str::from_utf8(key)
        {
            self.enqueue_fulltext_delete_for_string_overwrite(batch, key, old_raw);
        }
        self.write_string_byte_key_value_to_batch(batch, key, value, expire_ms, old_raw);
    }

    fn write_string_byte_key_value_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        value: &[u8],
        expire_ms: u64,
        old_raw: Option<&[u8]>,
    ) {
        let key_bytes = main_key_bytes(self.db_index, key);
        if let Some(header) = old_raw.and_then(decode_meta_header)
            && header.expire_ms > 0
            && header.expire_ms != expire_ms
            && let Ok(key) = std::str::from_utf8(key)
        {
            self.ttl_manager
                .remove_known_to_batch(batch, header.expire_ms, self.db_index, key);
        }
        (batch.put(&key_bytes, &encode_raw_string(value, expire_ms)))
            .expect("write batch append invariant violated");
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

    fn enqueue_fulltext_delete_for_string_overwrite(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        old_raw: &[u8],
    ) {
        let Some(header) = decode_meta_header(old_raw) else {
            return;
        };
        let result = match header.type_tag {
            TYPE_HASH => self.fulltext_enqueue_hash_delete_to_batch(batch, key),
            TYPE_JSON => self.fulltext_enqueue_json_delete_to_batch(batch, key),
            _ => Ok(()),
        };
        if let Err(err) = result {
            log::error!("failed to enqueue fulltext delete for overwritten {key}: {err}");
        }
    }

    pub(in crate::store::db) fn prepare_string_overwrite_to_batch(
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
        if header.type_tag == TYPE_STRING {
            return;
        }
        self.enqueue_fulltext_delete_for_string_overwrite(batch, key, raw);
    }

    pub(in crate::store::db) fn prepare_string_byte_key_overwrite_to_batch(
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
        if header.type_tag == TYPE_STRING {
            return;
        }
        if let Ok(key) = std::str::from_utf8(key) {
            self.enqueue_fulltext_delete_for_string_overwrite(batch, key, raw);
        }
    }

    pub(in crate::store::db) fn write_structure_to_batch(
        batch: &mut WriteBatch,
        db_index: u16,
        key: &str,
        value: &Structure,
        expire_ms: u64,
        version: u64,
    ) {
        match value {
            Structure::String(value) => {
                (batch.put(
                    &main_key(db_index, key),
                    &encode_raw_string(value.as_bytes(), expire_ms),
                ))
                .expect("write batch append invariant violated");
            }
            Structure::Hash(hash) => {
                let packed = hash
                    .iter()
                    .map(|(field, value)| (field.clone(), value.as_bytes().to_vec()))
                    .collect::<PackedHashFields>();
                if let Some(raw) = encode_packed_hash(expire_ms, &packed) {
                    (batch.put(&main_key(db_index, key), &raw))
                        .expect("write batch append invariant violated");
                    return;
                }
                (batch.put(
                    &main_key(db_index, key),
                    &encode_hash_meta(expire_ms, version),
                ))
                .expect("write batch append invariant violated");

                for (field, value) in hash {
                    (batch.put(
                        &hash_field_key(db_index, key, version, field),
                        value.as_bytes(),
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            Structure::SortedSet(set) => {
                if let Some(raw) = encode_packed_zset(expire_ms, set) {
                    (batch.put(&main_key(db_index, key), &raw))
                        .expect("write batch append invariant violated");
                    return;
                }
                (batch.put(
                    &main_key(db_index, key),
                    &encode_zset_meta(expire_ms, version),
                ))
                .expect("write batch append invariant violated");

                for (member, score) in set {
                    (batch.put(
                        &zset_member_key(db_index, key, version, member),
                        &score.to_be_bytes(),
                    ))
                    .expect("write batch append invariant violated");
                    (batch.put(
                        &zset_rank_key(db_index, key, version, *score, member),
                        INDEX_MARKER_VALUE,
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            Structure::Set(set) => {
                let packed = set.iter().cloned().collect::<PackedSetMembers>();
                if let Some(raw) = encode_packed_set(expire_ms, &packed) {
                    (batch.put(&main_key(db_index, key), &raw))
                        .expect("write batch append invariant violated");
                    return;
                }
                (batch.put(
                    &main_key(db_index, key),
                    &encode_set_meta(expire_ms, version, set.len()),
                ))
                .expect("write batch append invariant violated");

                for member in set {
                    (batch.put(
                        &set_member_key(db_index, key, version, member),
                        INDEX_MARKER_VALUE,
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            Structure::List(list) => {
                let packed = list
                    .iter()
                    .map(|value| value.as_bytes().to_vec())
                    .collect::<Vec<_>>();
                if let Some(raw) = encode_packed_list(expire_ms, &packed) {
                    (batch.put(&main_key(db_index, key), &raw))
                        .expect("write batch append invariant violated");
                    return;
                }
                (batch.put(
                    &main_key(db_index, key),
                    &encode_list_meta(expire_ms, version, 0, list.len() as i64),
                ))
                .expect("write batch append invariant violated");

                for (index, value) in list.iter().enumerate() {
                    (batch.put(
                        &list_item_key(db_index, key, version, index as i64),
                        value.as_bytes(),
                    ))
                    .expect("write batch append invariant violated");
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
                let meta = StreamMeta {
                    expire_ms,
                    version: 0,
                    last_id,
                    length: encoded_entries.len() as u64,
                    entries_added: encoded_entries.len() as u64,
                };
                let packed_entries = encoded_entries
                    .iter()
                    .map(|(id, fields)| (*id, encode_stream_entry(fields)))
                    .collect::<Vec<_>>();
                if let Some(raw) = encode_packed_stream(meta, &packed_entries) {
                    (batch.put(&main_key(db_index, key), &raw))
                        .expect("write batch append invariant violated");
                    return;
                }
                (batch.put(
                    &main_key(db_index, key),
                    &encode_stream_meta(StreamMeta {
                        expire_ms,
                        version,
                        last_id,
                        length: encoded_entries.len() as u64,
                        entries_added: encoded_entries.len() as u64,
                    }),
                ))
                .expect("write batch append invariant violated");
                for (id, fields) in encoded_entries {
                    (batch.put(
                        &stream_entry_key(db_index, key, version, id),
                        &encode_stream_entry(&fields),
                    ))
                    .expect("write batch append invariant violated");
                }
            }
            _ => {
                let encoded = encode_entry(value, expire_ms, version);
                (batch.put(&main_key(db_index, key), &encoded))
                    .expect("write batch append invariant violated");
            }
        }
    }
}
