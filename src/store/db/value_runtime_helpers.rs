use super::*;

pub(in crate::store::db) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(in crate::store::db) fn random_u64() -> u64 {
    static RANDOM_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut x = nanos ^ RANDOM_COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

pub(in crate::store::db) fn set_write_lock_shard(db_index: u16, key: &str) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in db_index.to_be_bytes().into_iter().chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as usize & (SET_WRITE_LOCK_SHARDS - 1)
}

pub(in crate::store::db) fn normalize_byte_index(len: usize, index: i64) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    (normalized >= 0).then_some(normalized as usize)
}

pub(in crate::store::db) fn byte_range_slice(
    bytes: &[u8],
    start: Option<i64>,
    end: Option<i64>,
) -> &[u8] {
    if bytes.is_empty() {
        return bytes;
    }
    let start = normalize_byte_index(bytes.len(), start.unwrap_or(0)).unwrap_or(0);
    let end = end
        .and_then(|idx| normalize_byte_index(bytes.len(), idx))
        .unwrap_or(bytes.len() - 1);
    if start > end || start >= bytes.len() {
        return &bytes[0..0];
    }
    &bytes[start..=end.min(bytes.len() - 1)]
}
