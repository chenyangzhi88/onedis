// ============================================================================
// Sub-key range helpers  (for DeleteRange)
// ============================================================================
//
// Sub-key layout (after the version migration):
//
//   [internal_prefix][namespace:3][key_bytes][0x00][version:8 BE][field/member/…]
//
// A DeleteRange from `prefix(version)` to `prefix(version+1)` covers exactly
// the sub-keys that belong to this (key, version) pair.

fn sub_key_range_start(_db_index: u16, ns: &[u8; 3], key: &str, version: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(crate::store::TABLE_LOCAL_INTERNAL_PREFIX.len() + 3 + key.len() + 1 + 8);
    buf.extend_from_slice(crate::store::TABLE_LOCAL_INTERNAL_PREFIX);
    buf.extend_from_slice(ns);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&version.to_be_bytes());
    buf
}

#[inline]
fn sub_key_range_end(db_index: u16, ns: &[u8; 3], key: &str, version: u64) -> Vec<u8> {
    sub_key_range_start(db_index, ns, key, version + 1)
}

/// Append `DeleteRange` ops to `batch` for every sub-key namespace that the
/// given type uses.
pub fn delete_sub_keys_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    type_tag: u8,
) {
    match type_tag {
        TYPE_HASH => {
            batch.delete_range(
                &sub_key_range_start(db_index, &HASH_FIELD_NS, key, version),
                &sub_key_range_end(db_index, &HASH_FIELD_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &HASH_FIELD_EXPIRE_NS, key, version),
                &sub_key_range_end(db_index, &HASH_FIELD_EXPIRE_NS, key, version),
            );
        }
        TYPE_SET => {
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_MEMBER_NS, key, version),
                &sub_key_range_end(db_index, &SET_MEMBER_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_SLOT_NS, key, version),
                &sub_key_range_end(db_index, &SET_SLOT_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &SET_MEMBER_SLOT_NS, key, version),
                &sub_key_range_end(db_index, &SET_MEMBER_SLOT_NS, key, version),
            );
        }
        TYPE_SORTED_SET => {
            // member index
            batch.delete_range(
                &sub_key_range_start(db_index, &ZSET_MEMBER_NS, key, version),
                &sub_key_range_end(db_index, &ZSET_MEMBER_NS, key, version),
            );
            // rank index
            batch.delete_range(
                &sub_key_range_start(db_index, &ZSET_RANK_NS, key, version),
                &sub_key_range_end(db_index, &ZSET_RANK_NS, key, version),
            );
        }
        TYPE_LIST => {
            batch.delete_range(
                &sub_key_range_start(db_index, &LIST_ITEM_NS, key, version),
                &sub_key_range_end(db_index, &LIST_ITEM_NS, key, version),
            );
        }
        TYPE_STREAM => {
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_ENTRY_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_ENTRY_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_GROUP_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_GROUP_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_PEL_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_PEL_NS, key, version),
            );
            batch.delete_range(
                &sub_key_range_start(db_index, &STREAM_CONSUMER_NS, key, version),
                &sub_key_range_end(db_index, &STREAM_CONSUMER_NS, key, version),
            );
        }
        TYPE_JSON => {
            batch.delete(&sub_key_range_start(db_index, &JSON_NODE_NS, key, version));
            batch.delete_range(
                &sub_key_range_start(db_index, &JSON_NODE_NS, key, version),
                &sub_key_range_end(db_index, &JSON_NODE_NS, key, version),
            );
        }
        TYPE_VECTOR => {
            for ns in [
                &VECTOR_META_NS,
                &VECTOR_DOC_NS,
                &VECTOR_TAG_NS,
                &VECTOR_NUMERIC_NS,
                &VECTOR_SEGMENT_NS,
                &VECTOR_GRAPH_NS,
            ] {
                batch.delete_range(
                    &sub_key_range_start(db_index, ns, key, version),
                    &sub_key_range_end(db_index, ns, key, version),
                );
            }
        }
        // String — no sub-keys
        _ => {}
    }
}
