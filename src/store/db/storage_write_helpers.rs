impl Db {
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

}
