// ============================================================================
// Sub-key range helpers  (for DeleteRange)
// ============================================================================
//
// Sub-key layout (after the version migration):
//
//   [internal_prefix][namespace:3][encoded owner][0x00][version:8 BE][field/member/…]
//
// NUL-containing owners use the shared marker + length encoding from the DB
// key codec. The exclusive upper bound is derived from the complete prefix, so
// the range covers exactly this (key, version) pair.

fn sub_key_range_start(
    key_encoding: TtlKeyEncoding,
    db_index: u16,
    ns: &[u8; 3],
    key: &str,
    version: u64,
) -> Vec<u8> {
    key_encoding.sub_key_range_start(db_index, ns, key, version)
}

#[inline]
fn sub_key_range_end(
    key_encoding: TtlKeyEncoding,
    db_index: u16,
    ns: &[u8; 3],
    key: &str,
    version: u64,
) -> Vec<u8> {
    key_encoding.sub_key_range_end(db_index, ns, key, version)
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
    delete_sub_keys_to_batch_with_encoding(
        batch,
        TtlKeyEncoding::current(),
        db_index,
        key,
        version,
        type_tag,
    );
}

fn delete_sub_keys_to_batch_with_encoding(
    batch: &mut WriteBatch,
    key_encoding: TtlKeyEncoding,
    db_index: u16,
    key: &str,
    version: u64,
    type_tag: u8,
) {
    match type_tag {
        TYPE_HASH => {
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &HASH_FIELD_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &HASH_FIELD_NS, key, version),
            )).expect("write batch append invariant violated");
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &HASH_FIELD_EXPIRE_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &HASH_FIELD_EXPIRE_NS, key, version),
            )).expect("write batch append invariant violated");
        }
        TYPE_SET => {
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &SET_MEMBER_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &SET_MEMBER_NS, key, version),
            )).expect("write batch append invariant violated");
        }
        TYPE_SORTED_SET => {
            // member index
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &ZSET_MEMBER_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &ZSET_MEMBER_NS, key, version),
            )).expect("write batch append invariant violated");
            // rank index
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &ZSET_RANK_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &ZSET_RANK_NS, key, version),
            )).expect("write batch append invariant violated");
        }
        TYPE_LIST => {
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &LIST_ITEM_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &LIST_ITEM_NS, key, version),
            )).expect("write batch append invariant violated");
        }
        TYPE_STREAM => {
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &STREAM_ENTRY_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &STREAM_ENTRY_NS, key, version),
            )).expect("write batch append invariant violated");
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &STREAM_GROUP_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &STREAM_GROUP_NS, key, version),
            )).expect("write batch append invariant violated");
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &STREAM_PEL_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &STREAM_PEL_NS, key, version),
            )).expect("write batch append invariant violated");
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &STREAM_CONSUMER_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &STREAM_CONSUMER_NS, key, version),
            )).expect("write batch append invariant violated");
        }
        TYPE_JSON => {
            (batch.delete(&sub_key_range_start(
                key_encoding,
                db_index,
                &JSON_NODE_NS,
                key,
                version,
            ))).expect("write batch append invariant violated");
            (batch.delete_range(
                &sub_key_range_start(key_encoding, db_index, &JSON_NODE_NS, key, version),
                &sub_key_range_end(key_encoding, db_index, &JSON_NODE_NS, key, version),
            )).expect("write batch append invariant violated");
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
                (batch.delete_range(
                    &sub_key_range_start(key_encoding, db_index, ns, key, version),
                    &sub_key_range_end(key_encoding, db_index, ns, key, version),
                )).expect("write batch append invariant violated");
            }
        }
        // String — no sub-keys
        _ => {}
    }
}
