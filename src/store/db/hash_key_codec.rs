use super::*;

pub(in crate::store::db) fn hash_field_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + HASH_FIELD_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&HASH_FIELD_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn hash_field_key(
    db_index: u16,
    key: &str,
    version: u64,
    field: &str,
) -> Vec<u8> {
    let mut composite_key = hash_field_prefix(db_index, key, version);
    composite_key.extend_from_slice(field.as_bytes());
    composite_key
}

pub(in crate::store::db) fn hash_field_expire_prefix(
    db_index: u16,
    key: &str,
    version: u64,
) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + HASH_FIELD_EXPIRE_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&HASH_FIELD_EXPIRE_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn hash_field_expire_key(
    db_index: u16,
    key: &str,
    version: u64,
    field: &str,
) -> Vec<u8> {
    let mut composite_key = hash_field_expire_prefix(db_index, key, version);
    composite_key.extend_from_slice(field.as_bytes());
    composite_key
}
