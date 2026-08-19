use super::*;

/// Packed records stop retrying after a short contention window. A hot key is cheaper and more
/// scalable in the split layout than repeatedly rewriting the whole inline value.
pub(in crate::store::db) const SMALL_INLINE_CAS_ATTEMPTS: usize = 3;

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

pub(in crate::store::db) const SMALL_SET_MAX_MEMBERS: usize = 128;
pub(in crate::store::db) const SMALL_SET_MAX_ENCODED_BYTES: usize = 8 * 1024;
const PACKED_SET_MAGIC: [u8; 4] = [0xf1, b'S', b'P', 1];
const PACKED_SET_HEADER_LEN: usize = 17 + PACKED_SET_MAGIC.len() + 2;

pub(in crate::store::db) type PackedSetMembers = BTreeSet<String>;

pub(in crate::store::db) fn is_packed_set_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_SET_HEADER_LEN
        && raw.get(16) == Some(&TYPE_SET)
        && raw[17..17 + PACKED_SET_MAGIC.len()] == PACKED_SET_MAGIC
}

pub(in crate::store::db) fn decode_packed_set(raw: &[u8]) -> Option<PackedSetMembers> {
    if !is_packed_set_raw(raw) {
        return None;
    }
    let mut offset = 17 + PACKED_SET_MAGIC.len();
    let count = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    if count > SMALL_SET_MAX_MEMBERS {
        return None;
    }
    let mut members = BTreeSet::new();
    for _ in 0..count {
        let member_len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
        offset += 4;
        let member_end = offset.checked_add(member_len)?;
        let member = std::str::from_utf8(raw.get(offset..member_end)?)
            .ok()?
            .to_owned();
        offset = member_end;
        if !members.insert(member) {
            return None;
        }
    }
    (offset == raw.len()).then_some(members)
}

pub(in crate::store::db) fn encode_packed_set(
    expire_ms: u64,
    members: &PackedSetMembers,
) -> Option<Vec<u8>> {
    if members.len() > SMALL_SET_MAX_MEMBERS {
        return None;
    }
    let encoded_len = members
        .iter()
        .try_fold(PACKED_SET_HEADER_LEN, |len, member| {
            u32::try_from(member.len()).ok()?;
            len.checked_add(4 + member.len())
        })?;
    if encoded_len > SMALL_SET_MAX_ENCODED_BYTES {
        return None;
    }
    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_SET);
    raw.extend_from_slice(&PACKED_SET_MAGIC);
    raw.extend_from_slice(&(members.len() as u16).to_be_bytes());
    for member in members {
        raw.extend_from_slice(&(member.len() as u32).to_be_bytes());
        raw.extend_from_slice(member.as_bytes());
    }
    Some(raw)
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

pub(in crate::store::db) const SMALL_LIST_MAX_ITEMS: usize = 64;
pub(in crate::store::db) const SMALL_LIST_MAX_ENCODED_BYTES: usize = 8 * 1024;
const PACKED_LIST_MAGIC: [u8; 4] = [0xf1, b'L', b'P', 1];
const PACKED_LIST_HEADER_LEN: usize = 17 + PACKED_LIST_MAGIC.len() + 2;

pub(in crate::store::db) type PackedListItems = Vec<Vec<u8>>;

pub(in crate::store::db) fn is_packed_list_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_LIST_HEADER_LEN
        && raw.get(16) == Some(&TYPE_LIST)
        && raw[17..17 + PACKED_LIST_MAGIC.len()] == PACKED_LIST_MAGIC
}

pub(in crate::store::db) fn decode_packed_list(raw: &[u8]) -> Option<PackedListItems> {
    if !is_packed_list_raw(raw) {
        return None;
    }
    let mut offset = 17 + PACKED_LIST_MAGIC.len();
    let count = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    if count > SMALL_LIST_MAX_ITEMS {
        return None;
    }
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
        offset += 4;
        let end = offset.checked_add(len)?;
        items.push(raw.get(offset..end)?.to_vec());
        offset = end;
    }
    (offset == raw.len()).then_some(items)
}

pub(in crate::store::db) fn encode_packed_list(
    expire_ms: u64,
    items: &[Vec<u8>],
) -> Option<Vec<u8>> {
    if items.len() > SMALL_LIST_MAX_ITEMS {
        return None;
    }
    let encoded_len = items.iter().try_fold(PACKED_LIST_HEADER_LEN, |len, item| {
        u32::try_from(item.len()).ok()?;
        len.checked_add(4 + item.len())
    })?;
    if encoded_len > SMALL_LIST_MAX_ENCODED_BYTES {
        return None;
    }
    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_LIST);
    raw.extend_from_slice(&PACKED_LIST_MAGIC);
    raw.extend_from_slice(&(items.len() as u16).to_be_bytes());
    for item in items {
        raw.extend_from_slice(&(item.len() as u32).to_be_bytes());
        raw.extend_from_slice(item);
    }
    Some(raw)
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

pub(in crate::store::db) const SMALL_STREAM_MAX_ENTRIES: usize = 32;
pub(in crate::store::db) const SMALL_STREAM_MAX_ENCODED_BYTES: usize = 8 * 1024;
const PACKED_STREAM_MAGIC: [u8; 4] = [0xf1, b'X', b'P', 1];
const PACKED_STREAM_HEADER_LEN: usize = 17 + PACKED_STREAM_MAGIC.len() + 32 + 2;

pub(in crate::store::db) type PackedStreamEntries = Vec<(StreamId, Vec<u8>)>;

pub(in crate::store::db) fn is_packed_stream_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_STREAM_HEADER_LEN
        && raw.get(16) == Some(&TYPE_STREAM)
        && raw[17..17 + PACKED_STREAM_MAGIC.len()] == PACKED_STREAM_MAGIC
}

pub(in crate::store::db) fn decode_packed_stream(
    raw: &[u8],
) -> Option<(StreamMeta, PackedStreamEntries)> {
    if !is_packed_stream_raw(raw) {
        return None;
    }
    let mut offset = 17 + PACKED_STREAM_MAGIC.len();
    let last_id = StreamId {
        ms: u64::from_be_bytes(raw.get(offset..offset + 8)?.try_into().ok()?),
        seq: u64::from_be_bytes(raw.get(offset + 8..offset + 16)?.try_into().ok()?),
    };
    offset += 16;
    let length = u64::from_be_bytes(raw.get(offset..offset + 8)?.try_into().ok()?);
    offset += 8;
    let entries_added = u64::from_be_bytes(raw.get(offset..offset + 8)?.try_into().ok()?);
    offset += 8;
    let count = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    if count > SMALL_STREAM_MAX_ENTRIES || length != count as u64 || entries_added < length {
        return None;
    }

    let mut entries = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let id = StreamId {
            ms: u64::from_be_bytes(raw.get(offset..offset + 8)?.try_into().ok()?),
            seq: u64::from_be_bytes(raw.get(offset + 8..offset + 16)?.try_into().ok()?),
        };
        offset += 16;
        if previous.is_some_and(|previous| id <= previous) || id > last_id {
            return None;
        }
        previous = Some(id);
        let value_len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
        offset += 4;
        let value_end = offset.checked_add(value_len)?;
        let value = raw.get(offset..value_end)?.to_vec();
        decode_stream_entry(&value)?;
        offset = value_end;
        entries.push((id, value));
    }
    if offset != raw.len() {
        return None;
    }
    Some((
        StreamMeta {
            expire_ms: u64::from_be_bytes(raw[0..8].try_into().ok()?),
            version: 0,
            last_id,
            length,
            entries_added,
        },
        entries,
    ))
}

pub(in crate::store::db) fn encode_packed_stream(
    meta: StreamMeta,
    entries: &[(StreamId, Vec<u8>)],
) -> Option<Vec<u8>> {
    if entries.len() > SMALL_STREAM_MAX_ENTRIES
        || meta.length != entries.len() as u64
        || meta.entries_added < meta.length
    {
        return None;
    }
    let mut previous = None;
    let encoded_len =
        entries
            .iter()
            .try_fold(PACKED_STREAM_HEADER_LEN, |encoded_len, (id, value)| {
                if previous.is_some_and(|previous| *id <= previous)
                    || *id > meta.last_id
                    || decode_stream_entry(value).is_none()
                {
                    return None;
                }
                previous = Some(*id);
                u32::try_from(value.len()).ok()?;
                encoded_len.checked_add(16 + 4 + value.len())
            })?;
    if encoded_len > SMALL_STREAM_MAX_ENCODED_BYTES {
        return None;
    }

    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&meta.expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_STREAM);
    raw.extend_from_slice(&PACKED_STREAM_MAGIC);
    raw.extend_from_slice(&meta.last_id.ms.to_be_bytes());
    raw.extend_from_slice(&meta.last_id.seq.to_be_bytes());
    raw.extend_from_slice(&meta.length.to_be_bytes());
    raw.extend_from_slice(&meta.entries_added.to_be_bytes());
    raw.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    for (id, value) in entries {
        raw.extend_from_slice(&id.ms.to_be_bytes());
        raw.extend_from_slice(&id.seq.to_be_bytes());
        raw.extend_from_slice(&(value.len() as u32).to_be_bytes());
        raw.extend_from_slice(value);
    }
    Some(raw)
}

pub(in crate::store::db) fn encode_zset_meta(expire_ms: u64, version: u64) -> Vec<u8> {
    encode_entry(&Structure::SortedSet(BTreeMap::new()), expire_ms, version)
}

pub(in crate::store::db) const SMALL_ZSET_MAX_MEMBERS: usize = 64;
pub(in crate::store::db) const SMALL_ZSET_MAX_ENCODED_BYTES: usize = 8 * 1024;
const PACKED_ZSET_MAGIC: [u8; 4] = [0xf1, b'Z', b'P', 1];
const PACKED_ZSET_HEADER_LEN: usize = 17 + PACKED_ZSET_MAGIC.len() + 2;

pub(in crate::store::db) type PackedZsetEntries = BTreeMap<String, f64>;

pub(in crate::store::db) fn is_packed_zset_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_ZSET_HEADER_LEN
        && raw.get(16) == Some(&TYPE_SORTED_SET)
        && raw[17..17 + PACKED_ZSET_MAGIC.len()] == PACKED_ZSET_MAGIC
}

pub(in crate::store::db) fn decode_packed_zset(raw: &[u8]) -> Option<PackedZsetEntries> {
    if !is_packed_zset_raw(raw) {
        return None;
    }
    let mut offset = 17 + PACKED_ZSET_MAGIC.len();
    let count = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    if count > SMALL_ZSET_MAX_MEMBERS {
        return None;
    }
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        let member_len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
        offset += 4;
        let score = f64::from_be_bytes(raw.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        if score.is_nan() {
            return None;
        }
        let member_end = offset.checked_add(member_len)?;
        let member = std::str::from_utf8(raw.get(offset..member_end)?)
            .ok()?
            .to_owned();
        offset = member_end;
        if entries.insert(member, score).is_some() {
            return None;
        }
    }
    (offset == raw.len()).then_some(entries)
}

pub(in crate::store::db) fn encode_packed_zset(
    expire_ms: u64,
    entries: &PackedZsetEntries,
) -> Option<Vec<u8>> {
    if entries.len() > SMALL_ZSET_MAX_MEMBERS || entries.values().any(|score| score.is_nan()) {
        return None;
    }
    let encoded_len = entries
        .keys()
        .try_fold(PACKED_ZSET_HEADER_LEN, |len, member| {
            u32::try_from(member.len()).ok()?;
            len.checked_add(4 + 8 + member.len())
        })?;
    if encoded_len > SMALL_ZSET_MAX_ENCODED_BYTES {
        return None;
    }
    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_SORTED_SET);
    raw.extend_from_slice(&PACKED_ZSET_MAGIC);
    raw.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    for (member, score) in entries {
        raw.extend_from_slice(&(member.len() as u32).to_be_bytes());
        raw.extend_from_slice(&score.to_be_bytes());
        raw.extend_from_slice(member.as_bytes());
    }
    Some(raw)
}

pub(in crate::store::db) fn decode_list_meta(raw: &[u8]) -> Option<ListMeta> {
    if let Some(items) = decode_packed_list(raw) {
        return Some(ListMeta {
            expire_ms: u64::from_be_bytes(raw[0..8].try_into().ok()?),
            version: 0,
            head: 0,
            tail: items.len() as i64,
        });
    }
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
    if let Some((meta, _)) = decode_packed_stream(raw) {
        return Some(meta);
    }
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
    if header.type_tag != TYPE_SET {
        return None;
    }
    if let Some(members) = decode_packed_set(raw) {
        return Some(SetMeta {
            expire_ms: header.expire_ms,
            version: 0,
            len: members.len(),
            packed: true,
        });
    }
    if raw.len() != 25 {
        return None;
    }
    Some(SetMeta {
        expire_ms: header.expire_ms,
        version: header.version,
        len: u64::from_be_bytes(raw[17..25].try_into().ok()?) as usize,
        packed: false,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct HashMeta {
    pub(in crate::store::db) expire_ms: u64,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) may_have_field_ttl: bool,
    pub(in crate::store::db) packed: bool,
}

pub(in crate::store::db) const HASH_META_COMPACT_LEN: usize = 18;
pub(in crate::store::db) const HASH_META_FLAG_MAY_HAVE_FIELD_TTL: u8 = 0x01;
pub(in crate::store::db) const HASH_META_FLAG_PACKED: u8 = 0x02;
pub(in crate::store::db) const SMALL_HASH_MAX_FIELDS: usize = 128;
pub(in crate::store::db) const SMALL_HASH_MAX_ENCODED_BYTES: usize = 8 * 1024;
const PACKED_HASH_MAGIC: [u8; 4] = [0xf1, b'H', b'P', 1];
const PACKED_HASH_HEADER_LEN: usize = HASH_META_COMPACT_LEN + PACKED_HASH_MAGIC.len() + 2;

pub(in crate::store::db) type PackedHashFields = BTreeMap<String, Vec<u8>>;

pub(in crate::store::db) fn is_packed_hash_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_HASH_HEADER_LEN
        && raw.get(16) == Some(&TYPE_HASH)
        && raw[17] & HASH_META_FLAG_PACKED != 0
        && raw[HASH_META_COMPACT_LEN..HASH_META_COMPACT_LEN + PACKED_HASH_MAGIC.len()]
            == PACKED_HASH_MAGIC
}

pub(in crate::store::db) fn decode_packed_hash(raw: &[u8]) -> Option<PackedHashFields> {
    if !is_packed_hash_raw(raw) {
        return None;
    }
    let mut offset = HASH_META_COMPACT_LEN + PACKED_HASH_MAGIC.len();
    let count = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    if count > SMALL_HASH_MAX_FIELDS {
        return None;
    }
    let mut fields = BTreeMap::new();
    for _ in 0..count {
        let field_len = u16::from_be_bytes(raw.get(offset..offset + 2)?.try_into().ok()?) as usize;
        offset += 2;
        let value_len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
        offset += 4;
        let field_end = offset.checked_add(field_len)?;
        let field = std::str::from_utf8(raw.get(offset..field_end)?)
            .ok()?
            .to_string();
        offset = field_end;
        let value_end = offset.checked_add(value_len)?;
        let value = raw.get(offset..value_end)?.to_vec();
        offset = value_end;
        if fields.insert(field, value).is_some() {
            return None;
        }
    }
    (offset == raw.len()).then_some(fields)
}

pub(in crate::store::db) fn encode_packed_hash(
    expire_ms: u64,
    fields: &PackedHashFields,
) -> Option<Vec<u8>> {
    let encoded_len = packed_hash_encoded_len(fields)?;
    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_HASH);
    raw.push(HASH_META_FLAG_PACKED);
    raw.extend_from_slice(&PACKED_HASH_MAGIC);
    raw.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (field, value) in fields {
        raw.extend_from_slice(&(field.len() as u16).to_be_bytes());
        raw.extend_from_slice(&(value.len() as u32).to_be_bytes());
        raw.extend_from_slice(field.as_bytes());
        raw.extend_from_slice(value);
    }
    Some(raw)
}

pub(in crate::store::db) fn hash_uses_packed_layout(fields: &PackedHashFields) -> bool {
    packed_hash_encoded_len(fields).is_some()
}

fn packed_hash_encoded_len(fields: &PackedHashFields) -> Option<usize> {
    if fields.len() > SMALL_HASH_MAX_FIELDS {
        return None;
    }
    let encoded_len = fields
        .iter()
        .try_fold(PACKED_HASH_HEADER_LEN, |len, (field, value)| {
            u16::try_from(field.len()).ok()?;
            u32::try_from(value.len()).ok()?;
            len.checked_add(2 + 4 + field.len() + value.len())
        })?;
    if encoded_len > SMALL_HASH_MAX_ENCODED_BYTES {
        return None;
    }
    Some(encoded_len)
}

pub(in crate::store::db) fn decode_hash_meta(raw: &[u8]) -> Option<HashMeta> {
    let header = decode_meta_header(raw)?;
    if header.type_tag != TYPE_HASH {
        return None;
    }
    let packed = is_packed_hash_raw(raw);
    Some(HashMeta {
        expire_ms: header.expire_ms,
        version: header.version,
        may_have_field_ttl: if packed {
            false
        } else if raw.len() == HASH_META_COMPACT_LEN {
            raw[17] & HASH_META_FLAG_MAY_HAVE_FIELD_TTL != 0
        } else {
            true
        },
        packed,
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
