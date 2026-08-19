use super::*;

pub(in crate::store::db) fn list_item_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + LIST_ITEM_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&LIST_ITEM_NAMESPACE);
    append_versioned_sub_key_owner(&mut prefix, key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn list_item_key(
    db_index: u16,
    key: &str,
    version: u64,
    index: i64,
) -> Vec<u8> {
    let mut composite_key = list_item_prefix(db_index, key, version);
    composite_key.extend_from_slice(&index.to_be_bytes());
    composite_key
}

pub(in crate::store::db) fn set_member_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + SET_MEMBER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&SET_MEMBER_NAMESPACE);
    append_versioned_sub_key_owner(&mut prefix, key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn set_member_key(
    db_index: u16,
    key: &str,
    version: u64,
    member: &str,
) -> Vec<u8> {
    let mut composite_key = set_member_prefix(db_index, key, version);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}

pub(in crate::store::db) fn zset_member_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + ZSET_MEMBER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&ZSET_MEMBER_NAMESPACE);
    append_versioned_sub_key_owner(&mut prefix, key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn zset_member_key(
    db_index: u16,
    key: &str,
    version: u64,
    member: &str,
) -> Vec<u8> {
    let mut composite_key = zset_member_prefix(db_index, key, version);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}

pub(in crate::store::db) fn zset_rank_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + ZSET_RANK_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&ZSET_RANK_NAMESPACE);
    append_versioned_sub_key_owner(&mut prefix, key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn encode_sorted_f64(score: f64) -> [u8; 8] {
    let bits = score.to_bits();
    let encoded = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    encoded.to_be_bytes()
}

pub(in crate::store::db) fn decode_sorted_f64(bytes: [u8; 8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes);
    let bits = if encoded >> 63 == 1 {
        encoded ^ (1 << 63)
    } else {
        !encoded
    };
    f64::from_bits(bits)
}

pub(in crate::store::db) fn decode_zset_score(raw: &[u8]) -> Option<f64> {
    let bytes: [u8; 8] = raw.try_into().ok()?;
    Some(f64::from_be_bytes(bytes))
}

pub(in crate::store::db) fn zset_rank_key(
    db_index: u16,
    key: &str,
    version: u64,
    score: f64,
    member: &str,
) -> Vec<u8> {
    let mut composite_key = zset_rank_prefix(db_index, key, version);
    composite_key.extend_from_slice(&encode_sorted_f64(score));
    composite_key.push(0x00);
    composite_key.extend_from_slice(member.as_bytes());
    composite_key
}

pub(in crate::store::db) fn zset_score_scan_bounds(
    db_index: u16,
    key: &str,
    version: u64,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    if min.is_nan()
        || max.is_nan()
        || min > max
        || (min == max && (!min_inclusive || !max_inclusive))
    {
        return None;
    }
    let min_for_encoding = if min == 0.0 && min_inclusive {
        -0.0
    } else if min == 0.0 {
        0.0
    } else {
        min
    };
    let mut min_prefix = zset_rank_prefix(db_index, key, version);
    min_prefix.extend_from_slice(&encode_sorted_f64(min_for_encoding));
    let lower = if min_inclusive {
        min_prefix
    } else {
        prefix_exclusive_upper_bound(&min_prefix)?
    };

    let max_for_encoding = if max == 0.0 && !max_inclusive {
        -0.0
    } else if max == 0.0 {
        0.0
    } else {
        max
    };
    let mut max_prefix = zset_rank_prefix(db_index, key, version);
    max_prefix.extend_from_slice(&encode_sorted_f64(max_for_encoding));
    let upper = if max_inclusive {
        prefix_exclusive_upper_bound(&max_prefix)
    } else {
        Some(max_prefix)
    };
    Some((lower, upper))
}
