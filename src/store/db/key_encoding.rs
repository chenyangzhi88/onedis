const HASH_FIELD_NAMESPACE: [u8; 3] = [0xFF, b'h', 0x00];
const HASH_FIELD_EXPIRE_NAMESPACE: [u8; 3] = [0xFF, b'H', 0x00];
const LIST_ITEM_NAMESPACE: [u8; 3] = [0xFF, b'l', 0x00];
const SET_MEMBER_NAMESPACE: [u8; 3] = [0xFF, b's', 0x00];
const SET_SLOT_NAMESPACE: [u8; 3] = [0xFF, b'S', 0x00];
const SET_MEMBER_SLOT_NAMESPACE: [u8; 3] = [0xFF, b't', 0x00];
const ZSET_MEMBER_NAMESPACE: [u8; 3] = [0xFF, b'z', 0x00];
const ZSET_RANK_NAMESPACE: [u8; 3] = [0xFF, b'Z', 0x00];
const STREAM_ENTRY_NAMESPACE: [u8; 3] = [0xFF, b'x', 0x00];
const STREAM_GROUP_NAMESPACE: [u8; 3] = [0xFF, b'g', 0x00];
const STREAM_PEL_NAMESPACE: [u8; 3] = [0xFF, b'p', 0x00];
const STREAM_CONSUMER_NAMESPACE: [u8; 3] = [0xFF, b'c', 0x00];
const JSON_NODE_NAMESPACE: [u8; 3] = [0xFF, b'j', 0x00];
const FULLTEXT_META_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x00];
const FULLTEXT_FILE_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x01];
const FULLTEXT_OUTBOX_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x02];
const VECTOR_META_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x00];
const VECTOR_DOC_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x01];
const VECTOR_TAG_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x02];
const VECTOR_NUMERIC_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x03];
const VECTOR_SEGMENT_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x04];
const VECTOR_GRAPH_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x05];
const INDEX_MARKER_VALUE: &[u8] = b"\x01";
const LIST_META_MAGIC: [u8; 4] = *b"ULST";
const STREAM_META_MAGIC: [u8; 4] = *b"USTR";
const JSON_INDEXED_MARKER: &str = "__onedis_json_indexed_v1__";
const WRONG_TYPE_ERROR: &str = "ERR Operation against a key holding the wrong kind of value";
const SET_WRITE_LOCK_SHARDS: usize = 256;

fn trace_lrange_sample() -> Option<u64> {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    static COUNT: AtomicU64 = AtomicU64::new(0);
    if !*ENABLED.get_or_init(|| std::env::var_os("ONEDIS_LRANGE_TRACE").is_some()) {
        return None;
    }
    let count = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    (count <= 20 || count.is_multiple_of(1000)).then_some(count)
}

/// 数据库索引前缀（2 字节大端序），用于在共享 KvStore 中隔离不同逻辑数据库的数据。
fn db_prefix(db_index: u16) -> [u8; 2] {
    db_index.to_be_bytes()
}

fn db_prefix_exclusive_upper_bound(db_index: u16) -> Option<Vec<u8>> {
    let mut upper = db_prefix(db_index).to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] += 1;
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}

fn prefix_exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] += 1;
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}

/// 生成带数据库前缀的主键（用于 string/hash-meta/set-meta/list-meta/zset-meta 等直接键）。
fn main_key(db_index: u16, key: &str) -> Vec<u8> {
    let pfx = db_prefix(db_index);
    let mut k = Vec::with_capacity(2 + key.len());
    k.extend_from_slice(&pfx);
    k.extend_from_slice(key.as_bytes());
    k
}

fn main_key_bytes(db_index: u16, key: &[u8]) -> Vec<u8> {
    let pfx = db_prefix(db_index);
    let mut k = Vec::with_capacity(2 + key.len());
    k.extend_from_slice(&pfx);
    k.extend_from_slice(key);
    k
}

fn sub_key_range_start_bytes(db_index: u16, ns: &[u8; 3], key: &[u8], version: u64) -> Vec<u8> {
    let pfx = db_prefix(db_index);
    let mut buf = Vec::with_capacity(2 + 3 + key.len() + 1 + 8);
    buf.extend_from_slice(&pfx);
    buf.extend_from_slice(ns);
    buf.extend_from_slice(key);
    buf.push(0x00);
    buf.extend_from_slice(&version.to_be_bytes());
    buf
}

#[inline]
fn sub_key_range_end_bytes(db_index: u16, ns: &[u8; 3], key: &[u8], version: u64) -> Vec<u8> {
    sub_key_range_start_bytes(db_index, ns, key, version + 1)
}

fn delete_sub_keys_to_batch_bytes(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &[u8],
    version: u64,
    type_tag: u8,
) {
    match type_tag {
        TYPE_HASH => {
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &HASH_FIELD_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &HASH_FIELD_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &HASH_FIELD_EXPIRE_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &HASH_FIELD_EXPIRE_NAMESPACE, key, version),
            );
        }
        TYPE_SET => {
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &SET_MEMBER_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &SET_MEMBER_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &SET_SLOT_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &SET_SLOT_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &SET_MEMBER_SLOT_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &SET_MEMBER_SLOT_NAMESPACE, key, version),
            );
        }
        TYPE_SORTED_SET => {
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &ZSET_MEMBER_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &ZSET_MEMBER_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &ZSET_RANK_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &ZSET_RANK_NAMESPACE, key, version),
            );
        }
        TYPE_LIST => {
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &LIST_ITEM_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &LIST_ITEM_NAMESPACE, key, version),
            );
        }
        TYPE_STREAM => {
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &STREAM_ENTRY_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &STREAM_ENTRY_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &STREAM_GROUP_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &STREAM_GROUP_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &STREAM_PEL_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &STREAM_PEL_NAMESPACE, key, version),
            );
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &STREAM_CONSUMER_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &STREAM_CONSUMER_NAMESPACE, key, version),
            );
        }
        TYPE_JSON => {
            batch.delete(&sub_key_range_start_bytes(
                db_index,
                &JSON_NODE_NAMESPACE,
                key,
                version,
            ));
            batch.delete_range(
                &sub_key_range_start_bytes(db_index, &JSON_NODE_NAMESPACE, key, version),
                &sub_key_range_end_bytes(db_index, &JSON_NODE_NAMESPACE, key, version),
            );
        }
        TYPE_VECTOR => {
            for ns in [
                &VECTOR_META_NAMESPACE,
                &VECTOR_DOC_NAMESPACE,
                &VECTOR_TAG_NAMESPACE,
                &VECTOR_NUMERIC_NAMESPACE,
                &VECTOR_SEGMENT_NAMESPACE,
                &VECTOR_GRAPH_NAMESPACE,
            ] {
                batch.delete_range(
                    &sub_key_range_start_bytes(db_index, ns, key, version),
                    &sub_key_range_end_bytes(db_index, ns, key, version),
                );
            }
        }
        _ => {}
    }
}

fn decode_db_prefix(key: &[u8]) -> Option<u16> {
    let prefix = key.get(..2)?;
    Some(u16::from_be_bytes(prefix.try_into().ok()?))
}

fn is_known_subkey_namespace(rest: &[u8]) -> bool {
    rest.starts_with(&HASH_FIELD_NAMESPACE)
        || rest.starts_with(&HASH_FIELD_EXPIRE_NAMESPACE)
        || rest.starts_with(&LIST_ITEM_NAMESPACE)
        || rest.starts_with(&SET_MEMBER_NAMESPACE)
        || rest.starts_with(&SET_SLOT_NAMESPACE)
        || rest.starts_with(&SET_MEMBER_SLOT_NAMESPACE)
        || rest.starts_with(&ZSET_MEMBER_NAMESPACE)
        || rest.starts_with(&ZSET_RANK_NAMESPACE)
        || rest.starts_with(&STREAM_ENTRY_NAMESPACE)
        || rest.starts_with(&STREAM_GROUP_NAMESPACE)
        || rest.starts_with(&STREAM_PEL_NAMESPACE)
        || rest.starts_with(&STREAM_CONSUMER_NAMESPACE)
        || rest.starts_with(&JSON_NODE_NAMESPACE)
        || rest.starts_with(&VECTOR_META_NAMESPACE)
        || rest.starts_with(&VECTOR_DOC_NAMESPACE)
        || rest.starts_with(&VECTOR_TAG_NAMESPACE)
        || rest.starts_with(&VECTOR_NUMERIC_NAMESPACE)
        || rest.starts_with(&VECTOR_SEGMENT_NAMESPACE)
        || rest.starts_with(&VECTOR_GRAPH_NAMESPACE)
}

fn logical_main_key_from_raw_key(key: &[u8]) -> Option<Vec<u8>> {
    if key.len() <= 2 {
        return None;
    }
    let rest = &key[2..];
    if is_known_subkey_namespace(rest) {
        return None;
    }
    Some(key.to_vec())
}

fn collect_logical_mutations(batch: &WriteBatch) -> (Vec<Vec<u8>>, Vec<u16>) {
    let mut keys = Vec::new();
    let mut dbs = Vec::new();
    for (write_type, key, _) in batch.iter() {
        match write_type {
            common::types::write_batch::WriteType::Put
            | common::types::write_batch::WriteType::PutBlobMedium
            | common::types::write_batch::WriteType::PutBlobExternal
            | common::types::write_batch::WriteType::Delete
            | common::types::write_batch::WriteType::Merge => {
                if let Some(key) = logical_main_key_from_raw_key(key) {
                    keys.push(key);
                }
            }
            common::types::write_batch::WriteType::RangeDelete => {
                if key.len() == 2
                    && let Some(db_index) = decode_db_prefix(key)
                {
                    dbs.push(db_index);
                }
            }
        }
    }
    (keys, dbs)
}

// ============================================================================
// 数据结构定义（保持不变，用于序列化兼容）
// ============================================================================


