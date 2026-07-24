// ============================================================================
// Helpers
// ============================================================================

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn main_key(_db_index: u16, key: &str) -> Vec<u8> {
    key.as_bytes().to_vec()
}

fn json_node_prefix(_db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(
        crate::store::TABLE_LOCAL_INTERNAL_PREFIX.len() + JSON_NODE_NS.len() + key.len() + 1 + 8,
    );
    prefix.extend_from_slice(crate::store::TABLE_LOCAL_INTERNAL_PREFIX);
    prefix.extend_from_slice(&JSON_NODE_NS);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub fn reserve_version_high_water_to_batch(batch: &mut WriteBatch, high_water: u64) {
    batch.put(VERSION_COUNTER_KEY, &high_water.to_be_bytes());
}

fn parse_version_mark_key(key: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(VERSION_MARK_PREFIX)?;
    if suffix.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(suffix.try_into().ok()?))
}

fn ttl_db_prefix(db_index: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(TTL_INDEX_PREFIX.len() + 2);
    key.extend_from_slice(TTL_INDEX_PREFIX);
    key.extend_from_slice(&db_index.to_be_bytes());
    key
}

#[cfg(test)]
fn ttl_db_expire_upper_bound(db_index: u16, now_ms: u64) -> Vec<u8> {
    let mut key = ttl_db_prefix(db_index);
    key.extend_from_slice(&now_ms.saturating_add(1).to_be_bytes());
    key
}

fn ttl_index_key(expire_ms: u64, db_index: u16, user_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TTL_INDEX_PREFIX.len() + 2 + 8 + user_key.len());
    key.extend_from_slice(TTL_INDEX_PREFIX);
    key.extend_from_slice(&db_index.to_be_bytes());
    key.extend_from_slice(&expire_ms.to_be_bytes());
    key.extend_from_slice(user_key.as_bytes());
    key
}

fn parse_ttl_index_key(key: &[u8]) -> Option<(u64, u16, String)> {
    let suffix = key.strip_prefix(TTL_INDEX_PREFIX)?;
    if suffix.len() < 10 {
        return None;
    }
    let db_index = u16::from_be_bytes(suffix[0..2].try_into().ok()?);
    let expire_ms = u64::from_be_bytes(suffix[2..10].try_into().ok()?);
    let user_key = String::from_utf8(suffix[10..].to_vec()).ok()?;
    Some((expire_ms, db_index, user_key))
}
