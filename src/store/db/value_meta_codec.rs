use super::*;

pub(in crate::store::db) fn encode_hash_meta(expire_ms: u64, version: u64) -> Vec<u8> {
    encode_hash_meta_with_field_ttl_flag(expire_ms, version, false)
}

pub(in crate::store::db) fn encode_hash_meta_with_field_ttl_flag(
    expire_ms: u64,
    version: u64,
    may_have_field_ttl: bool,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HASH_META_COMPACT_LEN);
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.push(TYPE_HASH);
    buf.push(if may_have_field_ttl {
        HASH_META_FLAG_MAY_HAVE_FIELD_TTL
    } else {
        0
    });
    buf
}

pub(in crate::store::db) fn encode_set_meta(expire_ms: u64, version: u64, len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(25);
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.push(TYPE_SET);
    buf.extend_from_slice(&(len as u64).to_be_bytes());
    buf
}

pub(in crate::store::db) fn encode_list_meta(
    expire_ms: u64,
    version: u64,
    head: i64,
    tail: i64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(36);
    buf.extend_from_slice(&LIST_META_MAGIC);
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&head.to_be_bytes());
    buf.extend_from_slice(&tail.to_be_bytes());
    buf
}

pub(in crate::store::db) fn encode_stream_meta(meta: StreamMeta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(52);
    buf.extend_from_slice(&STREAM_META_MAGIC);
    buf.extend_from_slice(&meta.expire_ms.to_be_bytes());
    buf.extend_from_slice(&meta.version.to_be_bytes());
    buf.extend_from_slice(&meta.last_id.ms.to_be_bytes());
    buf.extend_from_slice(&meta.last_id.seq.to_be_bytes());
    buf.extend_from_slice(&meta.length.to_be_bytes());
    buf.extend_from_slice(&meta.entries_added.to_be_bytes());
    buf
}

pub(in crate::store::db) fn encode_zset_meta(expire_ms: u64, version: u64) -> Vec<u8> {
    encode_entry(&Structure::SortedSet(BTreeMap::new()), expire_ms, version)
}

pub(in crate::store::db) fn decode_list_meta(raw: &[u8]) -> Option<ListMeta> {
    if raw.len() != 36 || raw[..4] != LIST_META_MAGIC {
        return None;
    }
    Some(ListMeta {
        expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
        version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
        head: i64::from_be_bytes(raw[20..28].try_into().ok()?),
        tail: i64::from_be_bytes(raw[28..36].try_into().ok()?),
    })
}

pub(in crate::store::db) fn decode_stream_meta(raw: &[u8]) -> Option<StreamMeta> {
    if raw.len() != 52 || raw[..4] != STREAM_META_MAGIC {
        return None;
    }
    Some(StreamMeta {
        expire_ms: u64::from_be_bytes(raw[4..12].try_into().ok()?),
        version: u64::from_be_bytes(raw[12..20].try_into().ok()?),
        last_id: StreamId {
            ms: u64::from_be_bytes(raw[20..28].try_into().ok()?),
            seq: u64::from_be_bytes(raw[28..36].try_into().ok()?),
        },
        length: u64::from_be_bytes(raw[36..44].try_into().ok()?),
        entries_added: u64::from_be_bytes(raw[44..52].try_into().ok()?),
    })
}

pub(in crate::store::db) fn decode_set_meta(raw: &[u8]) -> Option<SetMeta> {
    let header = decode_meta_header(raw)?;
    if header.type_tag != TYPE_SET || raw.len() < 25 {
        return None;
    }
    Some(SetMeta {
        expire_ms: header.expire_ms,
        version: header.version,
        len: u64::from_be_bytes(raw[17..25].try_into().ok()?) as usize,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct HashMeta {
    pub(in crate::store::db) expire_ms: u64,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) may_have_field_ttl: bool,
}

pub(in crate::store::db) const HASH_META_COMPACT_LEN: usize = 18;
pub(in crate::store::db) const HASH_META_FLAG_MAY_HAVE_FIELD_TTL: u8 = 0x01;

pub(in crate::store::db) fn decode_hash_meta(raw: &[u8]) -> Option<HashMeta> {
    let header = decode_meta_header(raw)?;
    if header.type_tag != TYPE_HASH {
        return None;
    }
    Some(HashMeta {
        expire_ms: header.expire_ms,
        version: header.version,
        may_have_field_ttl: if raw.len() == HASH_META_COMPACT_LEN {
            raw[17] & HASH_META_FLAG_MAY_HAVE_FIELD_TTL != 0
        } else {
            true
        },
    })
}

pub(in crate::store::db) fn decode_hash_meta_checked(raw: &[u8]) -> Result<HashMeta, Error> {
    let Some(header) = decode_meta_header(raw) else {
        return Err(Error::msg("Failed to decode hash metadata"));
    };
    if header.type_tag != TYPE_HASH {
        return Err(Error::msg(WRONG_TYPE_ERROR));
    }
    decode_hash_meta(raw).ok_or_else(|| Error::msg("Failed to decode hash metadata"))
}

pub(in crate::store::db) fn re_encode_meta_with_version(raw: &[u8], new_version: u64) -> Vec<u8> {
    let mut new_raw = raw.to_vec();
    if new_raw.len() >= 16 {
        new_raw[8..16].copy_from_slice(&new_version.to_be_bytes());
    }
    new_raw
}
