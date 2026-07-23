use super::*;

pub(in crate::store::db) const HASH_FIELD_NAMESPACE: [u8; 3] = [0xFF, b'h', 0x00];
pub(in crate::store::db) const HASH_FIELD_EXPIRE_NAMESPACE: [u8; 3] = [0xFF, b'H', 0x00];
pub(in crate::store::db) const LIST_ITEM_NAMESPACE: [u8; 3] = [0xFF, b'l', 0x00];
pub(in crate::store::db) const SET_MEMBER_NAMESPACE: [u8; 3] = [0xFF, b's', 0x00];
pub(in crate::store::db) const SET_SLOT_NAMESPACE: [u8; 3] = [0xFF, b'S', 0x00];
pub(in crate::store::db) const SET_MEMBER_SLOT_NAMESPACE: [u8; 3] = [0xFF, b't', 0x00];
pub(in crate::store::db) const ZSET_MEMBER_NAMESPACE: [u8; 3] = [0xFF, b'z', 0x00];
pub(in crate::store::db) const ZSET_RANK_NAMESPACE: [u8; 3] = [0xFF, b'Z', 0x00];
pub(in crate::store::db) const STREAM_ENTRY_NAMESPACE: [u8; 3] = [0xFF, b'x', 0x00];
pub(in crate::store::db) const STREAM_GROUP_NAMESPACE: [u8; 3] = [0xFF, b'g', 0x00];
pub(in crate::store::db) const STREAM_PEL_NAMESPACE: [u8; 3] = [0xFF, b'p', 0x00];
pub(in crate::store::db) const STREAM_CONSUMER_NAMESPACE: [u8; 3] = [0xFF, b'c', 0x00];
pub(in crate::store::db) const JSON_NODE_NAMESPACE: [u8; 3] = [0xFF, b'j', 0x00];
pub(in crate::store::db) const FULLTEXT_META_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x00];
pub(in crate::store::db) const FULLTEXT_FILE_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x01];
pub(in crate::store::db) const FULLTEXT_OUTBOX_NAMESPACE: [u8; 3] = [0xFF, b'f', 0x02];
pub(in crate::store::db) const VECTOR_META_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x00];
pub(in crate::store::db) const VECTOR_DOC_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x01];
pub(in crate::store::db) const VECTOR_TAG_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x02];
pub(in crate::store::db) const VECTOR_NUMERIC_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x03];
pub(in crate::store::db) const VECTOR_SEGMENT_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x04];
pub(in crate::store::db) const VECTOR_GRAPH_NAMESPACE: [u8; 3] = [0xFF, b'v', 0x05];
pub(in crate::store::db) const VERSION_OWNER_NAMESPACE: [u8; 3] = [0xFF, b'o', 0x00];
pub(in crate::store::db) const INDEX_MARKER_VALUE: &[u8] = b"\x01";
pub(in crate::store::db) const LIST_META_MAGIC: [u8; 4] = *b"ULST";
pub(in crate::store::db) const STREAM_META_MAGIC: [u8; 4] = *b"USTR";
pub(in crate::store::db) const JSON_INDEXED_MARKER: &str = "__onedis_json_indexed_v1__";
pub(in crate::store::db) const WRONG_TYPE_ERROR: &str =
    "ERR Operation against a key holding the wrong kind of value";
pub(in crate::store::db) const SET_WRITE_LOCK_SHARDS: usize = 256;

pub(in crate::store::db) fn trace_lrange_sample() -> Option<u64> {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    static COUNT: AtomicU64 = AtomicU64::new(0);
    if !*ENABLED.get_or_init(|| std::env::var_os("ONEDIS_LRANGE_TRACE").is_some()) {
        return None;
    }
    let count = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    (count <= 20 || count.is_multiple_of(1000)).then_some(count)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::store::db) enum KeyEncodingLayout {
    /// Legacy on-disk format used before kv-engine tables carried DB isolation.
    DbPrefixedV1,
    /// Current on-disk format for new onedis tables. DB isolation lives in the
    /// kv-engine table, so keys no longer carry the logical DB prefix.
    TableLocalV2,
}

pub(in crate::store::db) const KEY_ENCODING_LAYOUT_META_KEY: &[u8] = b"\x80m\x00layout";
pub(in crate::store::db) const DB_PREFIXED_V1_LAYOUT_VALUE: &[u8] = b"db-prefixed-v1";
pub(in crate::store::db) const TABLE_LOCAL_V2_LAYOUT_VALUE: &[u8] = b"table-local-v2";

impl KeyEncodingLayout {
    pub(in crate::store::db) const CURRENT: Self = Self::TableLocalV2;

    pub(in crate::store::db) fn db_prefix(self, db_index: u16) -> Vec<u8> {
        match self {
            Self::DbPrefixedV1 => db_index.to_be_bytes().to_vec(),
            Self::TableLocalV2 => Vec::new(),
        }
    }

    pub(in crate::store::db) fn internal_prefix(self, db_index: u16) -> Vec<u8> {
        match self {
            Self::DbPrefixedV1 => self.db_prefix(db_index),
            Self::TableLocalV2 => crate::store::TABLE_LOCAL_INTERNAL_PREFIX.to_vec(),
        }
    }

    pub(in crate::store::db) fn db_prefix_exclusive_upper_bound(
        self,
        db_index: u16,
    ) -> Option<Vec<u8>> {
        match self {
            Self::DbPrefixedV1 => prefix_exclusive_upper_bound(&self.db_prefix(db_index)),
            Self::TableLocalV2 => None,
        }
    }

    pub(in crate::store::db) fn main_key(self, db_index: u16, key: &str) -> Vec<u8> {
        self.main_key_bytes(db_index, key.as_bytes())
    }

    pub(in crate::store::db) fn main_key_bytes(self, db_index: u16, key: &[u8]) -> Vec<u8> {
        match self {
            Self::DbPrefixedV1 => {
                let pfx = self.db_prefix(db_index);
                let mut k = Vec::with_capacity(pfx.len() + key.len());
                k.extend_from_slice(&pfx);
                k.extend_from_slice(key);
                k
            }
            Self::TableLocalV2 => key.to_vec(),
        }
    }

    pub(in crate::store::db) fn sub_key_range_start_bytes(
        self,
        db_index: u16,
        ns: &[u8; 3],
        key: &[u8],
        version: u64,
    ) -> Vec<u8> {
        match self {
            Self::DbPrefixedV1 => {
                let pfx = self.internal_prefix(db_index);
                let mut buf = Vec::with_capacity(pfx.len() + 3 + key.len() + 1 + 8);
                buf.extend_from_slice(&pfx);
                buf.extend_from_slice(ns);
                buf.extend_from_slice(key);
                buf.push(0x00);
                buf.extend_from_slice(&version.to_be_bytes());
                buf
            }
            Self::TableLocalV2 => {
                let pfx = self.internal_prefix(db_index);
                let mut buf = Vec::with_capacity(pfx.len() + 3 + key.len() + 1 + 8);
                buf.extend_from_slice(&pfx);
                buf.extend_from_slice(ns);
                buf.extend_from_slice(key);
                buf.push(0x00);
                buf.extend_from_slice(&version.to_be_bytes());
                buf
            }
        }
    }

    pub(in crate::store::db) fn sub_key_range_end_bytes(
        self,
        db_index: u16,
        ns: &[u8; 3],
        key: &[u8],
        version: u64,
    ) -> Vec<u8> {
        self.sub_key_range_start_bytes(db_index, ns, key, version + 1)
    }

    pub(in crate::store::db) fn is_db_range_delete_start(self, db_index: u16, key: &[u8]) -> bool {
        match self {
            Self::DbPrefixedV1 => key == self.db_prefix(db_index),
            Self::TableLocalV2 => key.is_empty(),
        }
    }

    pub(in crate::store::db) fn logical_main_key_from_raw_key(
        self,
        db_index: u16,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        match self {
            Self::DbPrefixedV1 => {
                let prefix = self.internal_prefix(db_index);
                if key.len() <= prefix.len() || !key.starts_with(prefix.as_slice()) {
                    return None;
                }
                let rest = &key[prefix.len()..];
                if is_known_subkey_namespace(rest) {
                    return None;
                }
                Some(key.to_vec())
            }
            Self::TableLocalV2 => {
                let prefix = self.internal_prefix(db_index);
                if key == KEY_ENCODING_LAYOUT_META_KEY
                    || key
                        .strip_prefix(prefix.as_slice())
                        .is_some_and(is_known_subkey_namespace)
                {
                    return None;
                }
                Some(key.to_vec())
            }
        }
    }

    pub(in crate::store::db) fn encode(self) -> &'static [u8] {
        match self {
            Self::DbPrefixedV1 => DB_PREFIXED_V1_LAYOUT_VALUE,
            Self::TableLocalV2 => TABLE_LOCAL_V2_LAYOUT_VALUE,
        }
    }

    pub(in crate::store::db) fn decode(raw: &[u8]) -> Option<Self> {
        match raw {
            DB_PREFIXED_V1_LAYOUT_VALUE => Some(Self::DbPrefixedV1),
            TABLE_LOCAL_V2_LAYOUT_VALUE => Some(Self::TableLocalV2),
            _ => None,
        }
    }

    pub(in crate::store::db) fn open_or_initialize_for_table(store: &KvStore) -> Self {
        if let Some(raw) = store.get_raw(KEY_ENCODING_LAYOUT_META_KEY) {
            return Self::decode(&raw).unwrap_or_else(|| {
                panic!(
                    "unsupported onedis key encoding layout metadata: {:?}",
                    String::from_utf8_lossy(&raw)
                )
            });
        }
        if !store.scan_range_raw_limited(&[], None, 1).is_empty() {
            panic!(
                "onedis table contains data without key encoding metadata; remove old data before starting with TableLocalV2"
            );
        }
        store.put_raw(KEY_ENCODING_LAYOUT_META_KEY, Self::TableLocalV2.encode());
        Self::TableLocalV2
    }
}

/// 数据库索引前缀（2 字节大端序），用于兼容当前 DbPrefixedV1 磁盘格式。
pub(in crate::store::db) fn db_prefix(db_index: u16) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.db_prefix(db_index)
}

pub(in crate::store::db) fn internal_prefix(db_index: u16) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.internal_prefix(db_index)
}

pub(in crate::store::db) fn db_prefix_exclusive_upper_bound(db_index: u16) -> Option<Vec<u8>> {
    KeyEncodingLayout::CURRENT.db_prefix_exclusive_upper_bound(db_index)
}

pub(in crate::store::db) fn prefix_exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
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

/// 生成 DbPrefixedV1 主键（用于 string/hash-meta/set-meta/list-meta/zset-meta 等直接键）。
pub(in crate::store::db) fn main_key(db_index: u16, key: &str) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.main_key(db_index, key)
}

pub(in crate::store::db) fn main_key_bytes(db_index: u16, key: &[u8]) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.main_key_bytes(db_index, key)
}

pub(in crate::store::db) fn sub_key_range_start_bytes(
    db_index: u16,
    ns: &[u8; 3],
    key: &[u8],
    version: u64,
) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.sub_key_range_start_bytes(db_index, ns, key, version)
}

#[inline]
pub(in crate::store::db) fn sub_key_range_end_bytes(
    db_index: u16,
    ns: &[u8; 3],
    key: &[u8],
    version: u64,
) -> Vec<u8> {
    KeyEncodingLayout::CURRENT.sub_key_range_end_bytes(db_index, ns, key, version)
}

pub(in crate::store::db) fn sub_key_namespaces_for_type(
    type_tag: u8,
) -> &'static [&'static [u8; 3]] {
    match type_tag {
        TYPE_HASH => &[&HASH_FIELD_NAMESPACE, &HASH_FIELD_EXPIRE_NAMESPACE],
        TYPE_SET => &[
            &SET_MEMBER_NAMESPACE,
            &SET_SLOT_NAMESPACE,
            &SET_MEMBER_SLOT_NAMESPACE,
        ],
        TYPE_SORTED_SET => &[&ZSET_MEMBER_NAMESPACE, &ZSET_RANK_NAMESPACE],
        TYPE_LIST => &[&LIST_ITEM_NAMESPACE],
        TYPE_STREAM => &[
            &STREAM_ENTRY_NAMESPACE,
            &STREAM_GROUP_NAMESPACE,
            &STREAM_PEL_NAMESPACE,
            &STREAM_CONSUMER_NAMESPACE,
        ],
        TYPE_JSON => &[&JSON_NODE_NAMESPACE],
        TYPE_VECTOR => &[
            &VECTOR_META_NAMESPACE,
            &VECTOR_DOC_NAMESPACE,
            &VECTOR_TAG_NAMESPACE,
            &VECTOR_NUMERIC_NAMESPACE,
            &VECTOR_SEGMENT_NAMESPACE,
            &VECTOR_GRAPH_NAMESPACE,
        ],
        _ => &[],
    }
}

pub(in crate::store::db) fn delete_sub_keys_by_scan_to_batch_bytes(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &[u8],
    version: u64,
    type_tag: u8,
) {
    for ns in sub_key_namespaces_for_type(type_tag) {
        let start = sub_key_range_start_bytes(db_index, ns, key, version);
        let end = sub_key_range_end_bytes(db_index, ns, key, version);
        for (sub_key, _) in store.scan_range_raw_limited(&start, Some(end), usize::MAX) {
            batch.delete(&sub_key);
        }
    }
}

pub(in crate::store::db) fn delete_sub_keys_by_scan_to_batch(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    type_tag: u8,
) {
    delete_sub_keys_by_scan_to_batch_bytes(
        store,
        batch,
        db_index,
        key.as_bytes(),
        version,
        type_tag,
    );
}

pub(in crate::store::db) fn delete_sub_keys_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    type_tag: u8,
) {
    delete_sub_keys_to_batch_bytes(batch, db_index, key.as_bytes(), version, type_tag);
}

pub(in crate::store::db) fn delete_sub_keys_to_batch_bytes(
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

#[cfg(test)]
pub(in crate::store::db) fn decode_db_prefix(key: &[u8]) -> Option<u16> {
    let prefix = key.get(..2)?;
    Some(u16::from_be_bytes(prefix.try_into().ok()?))
}

pub(in crate::store::db) fn is_known_subkey_namespace(rest: &[u8]) -> bool {
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
        || rest.starts_with(&FULLTEXT_META_NAMESPACE)
        || rest.starts_with(&FULLTEXT_FILE_NAMESPACE)
        || rest.starts_with(&FULLTEXT_OUTBOX_NAMESPACE)
        || rest.starts_with(&VECTOR_META_NAMESPACE)
        || rest.starts_with(&VECTOR_DOC_NAMESPACE)
        || rest.starts_with(&VECTOR_TAG_NAMESPACE)
        || rest.starts_with(&VECTOR_NUMERIC_NAMESPACE)
        || rest.starts_with(&VECTOR_SEGMENT_NAMESPACE)
        || rest.starts_with(&VECTOR_GRAPH_NAMESPACE)
        || rest.starts_with(&VERSION_OWNER_NAMESPACE)
}

pub(in crate::store::db) fn logical_main_key_from_raw_key(
    layout: KeyEncodingLayout,
    db_index: u16,
    key: &[u8],
) -> Option<Vec<u8>> {
    layout.logical_main_key_from_raw_key(db_index, key)
}

pub(in crate::store::db) fn collect_logical_mutations(
    layout: KeyEncodingLayout,
    db_index: u16,
    batch: &WriteBatch,
) -> (Vec<Vec<u8>>, Vec<u16>) {
    let mut keys = Vec::new();
    let mut dbs = Vec::new();
    for (write_type, key, _) in batch.iter() {
        match write_type {
            common::types::write_batch::WriteType::Put
            | common::types::write_batch::WriteType::PutBlobMedium
            | common::types::write_batch::WriteType::PutBlobExternal
            | common::types::write_batch::WriteType::Delete
            | common::types::write_batch::WriteType::Merge => {
                if let Some(key) = logical_main_key_from_raw_key(layout, db_index, key) {
                    keys.push(key);
                }
            }
            common::types::write_batch::WriteType::RangeDelete => {
                if layout.is_db_range_delete_start(db_index, key) {
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
