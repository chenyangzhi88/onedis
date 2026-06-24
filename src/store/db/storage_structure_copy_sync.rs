impl Db {
    fn copy_prefixed_namespace_to_batch_sync(
        store: &KvStore,
        batch: &mut WriteBatch,
        source_prefix: Vec<u8>,
        target_prefix: Vec<u8>,
    ) {
        for (source_key_bytes, value) in store.scan_prefix_raw(&source_prefix) {
            if let Some(suffix) = source_key_bytes.strip_prefix(source_prefix.as_slice()) {
                let mut new_key = target_prefix.clone();
                new_key.extend_from_slice(suffix);
                batch.put(&new_key, &value);
            }
        }
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
            Self::copy_prefixed_namespace_to_batch_sync(
                store,
                batch,
                list_item_prefix(source_db_index, source_key, meta.version),
                list_item_prefix(target_db_index, target_key, target_version),
            );
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
                Self::copy_prefixed_namespace_to_batch_sync(store, batch, source_ns, target_ns);
            }
            return;
        }

        match header.type_tag {
            TYPE_HASH => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_hash_meta(header.expire_ms, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    hash_field_prefix(source_db_index, source_key, header.version),
                    hash_field_prefix(target_db_index, target_key, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    hash_field_expire_prefix(source_db_index, source_key, header.version),
                    hash_field_expire_prefix(target_db_index, target_key, target_version),
                );
            }
            TYPE_SORTED_SET => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_zset_meta(header.expire_ms, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    zset_member_prefix(source_db_index, source_key, header.version),
                    zset_member_prefix(target_db_index, target_key, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    zset_rank_prefix(source_db_index, source_key, header.version),
                    zset_rank_prefix(target_db_index, target_key, target_version),
                );
            }
            TYPE_SET => {
                let Some(meta) = decode_set_meta(raw) else {
                    return;
                };
                batch.put(
                    &main_key(target_db_index, target_key),
                    &encode_set_meta(meta.expire_ms, target_version, meta.len),
                );
                for (source_prefix, target_prefix) in [
                    (
                        set_member_prefix(source_db_index, source_key, meta.version),
                        set_member_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        set_slot_prefix(source_db_index, source_key, meta.version),
                        set_slot_prefix(target_db_index, target_key, target_version),
                    ),
                    (
                        set_member_slot_prefix(source_db_index, source_key, meta.version),
                        set_member_slot_prefix(target_db_index, target_key, target_version),
                    ),
                ] {
                    Self::copy_prefixed_namespace_to_batch_sync(
                        store,
                        batch,
                        source_prefix,
                        target_prefix,
                    );
                }
            }
            TYPE_JSON => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    json_node_prefix(source_db_index, source_key, header.version),
                    json_node_prefix(target_db_index, target_key, target_version),
                );
            }
            TYPE_LIST => {
                batch.put(
                    &main_key(target_db_index, target_key),
                    &re_encode_meta_with_version(raw, target_version),
                );
                Self::copy_prefixed_namespace_to_batch_sync(
                    store,
                    batch,
                    list_item_prefix(source_db_index, source_key, header.version),
                    list_item_prefix(target_db_index, target_key, target_version),
                );
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
                    Self::copy_prefixed_namespace_to_batch_sync(
                        store,
                        batch,
                        source_prefix,
                        target_prefix,
                    );
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
}
