use super::*;

impl Db {
    pub(in crate::store::db) async fn copy_prefixed_namespace_to_batch(
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

    pub(in crate::store::db) async fn copy_structure_between_dbs_to_batch_async(
        batch: &mut WriteBatch,
        context: StructureCopyContext<'_>,
    ) {
        let StructureCopyContext {
            source_store,
            target_store,
            source:
                DbKeyRef {
                    db_index: source_db_index,
                    key: source_key,
                },
            target:
                DbKeyRef {
                    db_index: target_db_index,
                    key: target_key,
                },
            raw,
            version_counter,
        } = context;
        let Some(header) = decode_meta_header(raw) else {
            return;
        };
        let target_version =
            Self::next_persisted_version_for_store_async(target_store, version_counter).await;

        if let Some(meta) = decode_list_meta(raw) {
            batch.put(
                &main_key(target_db_index, target_key),
                &encode_list_meta(meta.expire_ms, target_version, meta.head, meta.tail),
            );
            Self::copy_prefixed_namespace_to_batch(
                source_store,
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
                Self::copy_prefixed_namespace_to_batch(source_store, batch, source_ns, target_ns)
                    .await;
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
                    source_store,
                    batch,
                    hash_field_prefix(source_db_index, source_key, header.version),
                    hash_field_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    source_store,
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
                    source_store,
                    batch,
                    zset_member_prefix(source_db_index, source_key, header.version),
                    zset_member_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    source_store,
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
                    source_store,
                    batch,
                    set_member_prefix(source_db_index, source_key, meta.version),
                    set_member_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    source_store,
                    batch,
                    set_slot_prefix(source_db_index, source_key, meta.version),
                    set_slot_prefix(target_db_index, target_key, target_version),
                )
                .await;
                Self::copy_prefixed_namespace_to_batch(
                    source_store,
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
                    source_store,
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
                    source_store,
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
                        source_store,
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
}
