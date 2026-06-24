fn list_item_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + LIST_ITEM_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&LIST_ITEM_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn list_item_key(db_index: u16, key: &str, version: u64, index: i64) -> Vec<u8> {
    let mut composite_key = list_item_prefix(db_index, key, version);
    composite_key.extend_from_slice(&index.to_be_bytes());
    composite_key
}

fn set_member_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + SET_MEMBER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&SET_MEMBER_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn set_member_key(db_index: u16, key: &str, version: u64, member: &str) -> Vec<u8> {
    let mut composite_key = set_member_prefix(db_index, key, version);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}

fn set_member_key_bytes(db_index: u16, key: &str, version: u64, member: &[u8]) -> Vec<u8> {
    let mut composite_key = set_member_prefix(db_index, key, version);
    composite_key.extend_from_slice(member);
    composite_key
}

fn set_slot_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + SET_SLOT_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&SET_SLOT_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn set_slot_key(db_index: u16, key: &str, version: u64, slot: u64) -> Vec<u8> {
    let mut composite_key = set_slot_prefix(db_index, key, version);
    composite_key.extend_from_slice(&slot.to_be_bytes());
    composite_key
}

fn set_member_slot_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + SET_MEMBER_SLOT_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&SET_MEMBER_SLOT_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn set_member_slot_key(db_index: u16, key: &str, version: u64, member: &[u8]) -> Vec<u8> {
    let mut composite_key = set_member_slot_prefix(db_index, key, version);
    composite_key.extend_from_slice(member);
    composite_key
}

fn zset_member_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + ZSET_MEMBER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&ZSET_MEMBER_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn zset_member_key(db_index: u16, key: &str, version: u64, member: &str) -> Vec<u8> {
    let mut composite_key = zset_member_prefix(db_index, key, version);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}

fn zset_rank_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + ZSET_RANK_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&ZSET_RANK_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn encode_sorted_f64(score: f64) -> [u8; 8] {
    let bits = score.to_bits();
    let encoded = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    encoded.to_be_bytes()
}

fn decode_sorted_f64(bytes: [u8; 8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes);
    let bits = if encoded >> 63 == 1 {
        encoded ^ (1 << 63)
    } else {
        !encoded
    };
    f64::from_bits(bits)
}

fn decode_zset_score(raw: &[u8]) -> Option<f64> {
    let bytes: [u8; 8] = raw.try_into().ok()?;
    Some(f64::from_be_bytes(bytes))
}

fn zset_rank_key(db_index: u16, key: &str, version: u64, score: f64, member: &str) -> Vec<u8> {
    let mut composite_key = zset_rank_prefix(db_index, key, version);
    composite_key.extend_from_slice(&encode_sorted_f64(score));
    composite_key.push(0x00);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}
