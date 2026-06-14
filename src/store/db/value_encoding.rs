fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn random_u64() -> u64 {
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

fn set_write_lock_shard(db_index: u16, key: &str) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in db_index.to_be_bytes().into_iter().chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as usize & (SET_WRITE_LOCK_SHARDS - 1)
}

fn normalize_byte_index(len: usize, index: i64) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    (normalized >= 0).then_some(normalized as usize)
}

fn byte_range_slice(bytes: &[u8], start: Option<i64>, end: Option<i64>) -> &[u8] {
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

fn structure_type_tag(s: &Structure) -> u8 {
    match s {
        Structure::String(_) => TYPE_STRING,
        Structure::Hash(_) => TYPE_HASH,
        Structure::SortedSet(_) => TYPE_SORTED_SET,
        Structure::VectorCollection(_) => TYPE_VECTOR,
        Structure::Set(_) => TYPE_SET,
        Structure::List(_) => TYPE_LIST,
        Structure::Stream(_) => TYPE_STREAM,
        Structure::Json(_) => TYPE_JSON,
    }
}

fn encode_entry(structure: &Structure, expire_ms: u64, version: u64) -> Vec<u8> {
    if let Structure::String(value) = structure {
        return encode_raw_string(value.as_bytes(), expire_ms);
    }
    let type_tag = structure_type_tag(structure);
    let config = bincode::config::standard();
    let data = bincode::encode_to_vec(structure, config).unwrap();
    let mut buf = Vec::with_capacity(17 + data.len());
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.push(type_tag);
    buf.extend_from_slice(&data);
    buf
}

fn encode_raw_string(value: &[u8], expire_ms: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17 + value.len());
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&0u64.to_be_bytes());
    buf.push(TYPE_STRING);
    buf.extend_from_slice(value);
    buf
}

fn decode_entry(raw: &[u8]) -> Option<(u64, u64, Structure)> {
    if decode_list_meta(raw).is_some() {
        return None;
    }
    if decode_stream_meta(raw).is_some() {
        return None;
    }
    if raw.len() < 17 {
        return None;
    }
    let expire_ms = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let version = u64::from_be_bytes(raw[8..16].try_into().ok()?);
    if raw[16] == TYPE_STRING {
        let value = String::from_utf8(raw[17..].to_vec()).ok()?;
        return Some((expire_ms, version, Structure::String(value)));
    }
    let config = bincode::config::standard();
    let (structure, _) = bincode::decode_from_slice::<Structure, _>(&raw[17..], config).ok()?;
    Some((expire_ms, version, structure))
}

fn decode_string_bytes(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() >= 17 && raw[16] == TYPE_STRING {
        Some(raw[17..].to_vec())
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathToken {
    Field(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> Result<Vec<JsonPathToken>, Error> {
    if path == "$" || path == "." {
        return Ok(Vec::new());
    }

    let bytes = path.as_bytes();
    let mut idx = if bytes.first() == Some(&b'$') { 1 } else { 0 };
    let mut tokens = Vec::new();

    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(Error::msg("ERR invalid JSON path"));
                }
                tokens.push(JsonPathToken::Field(path[start..idx].to_string()));
            }
            b'[' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                    idx += 1;
                }
                if start == idx || idx >= bytes.len() || bytes[idx] != b']' {
                    return Err(Error::msg("ERR invalid JSON path"));
                }
                let index = path[start..idx]
                    .parse::<usize>()
                    .map_err(|_| Error::msg("ERR invalid JSON path"))?;
                idx += 1;
                tokens.push(JsonPathToken::Index(index));
            }
            _ => return Err(Error::msg("ERR invalid JSON path")),
        }
    }

    Ok(tokens)
}

fn json_get_path<'a>(value: &'a JsonValue, tokens: &[JsonPathToken]) -> Option<&'a JsonValue> {
    let mut current = value;
    for token in tokens {
        current = match token {
            JsonPathToken::Field(field) => current.as_object()?.get(field)?,
            JsonPathToken::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

fn json_get_parent_mut<'a>(
    value: &'a mut JsonValue,
    tokens: &'a [JsonPathToken],
) -> Option<(&'a mut JsonValue, &'a JsonPathToken)> {
    let (last, parent_tokens) = tokens.split_last()?;
    let mut current = value;
    for token in parent_tokens {
        current = match token {
            JsonPathToken::Field(field) => current.as_object_mut()?.get_mut(field)?,
            JsonPathToken::Index(index) => current.as_array_mut()?.get_mut(*index)?,
        };
    }
    Some((current, last))
}

fn json_set_path(
    value: &mut JsonValue,
    tokens: &[JsonPathToken],
    new_value: JsonValue,
) -> Option<bool> {
    if tokens.is_empty() {
        *value = new_value;
        return Some(true);
    }

    let (parent, last) = json_get_parent_mut(value, tokens)?;
    match last {
        JsonPathToken::Field(field) => {
            let object = parent.as_object_mut()?;
            let existed = object.contains_key(field);
            object.insert(field.clone(), new_value);
            Some(existed)
        }
        JsonPathToken::Index(index) => {
            let array = parent.as_array_mut()?;
            let target = array.get_mut(*index)?;
            *target = new_value;
            Some(true)
        }
    }
}

fn json_del_path(value: &mut JsonValue, tokens: &[JsonPathToken]) -> bool {
    if tokens.is_empty() {
        return true;
    }

    let Some((parent, last)) = json_get_parent_mut(value, tokens) else {
        return false;
    };
    match last {
        JsonPathToken::Field(field) => parent
            .as_object_mut()
            .and_then(|object| object.remove(field))
            .is_some(),
        JsonPathToken::Index(index) => {
            let Some(array) = parent.as_array_mut() else {
                return false;
            };
            if *index >= array.len() {
                return false;
            }
            array.remove(*index);
            true
        }
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(number) if number.is_i64() || number.is_u64() => "integer",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn json_node_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + JSON_NODE_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&JSON_NODE_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn encode_json_path(tokens: &[JsonPathToken]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for token in tokens {
        match token {
            JsonPathToken::Field(field) => {
                let bytes = field.as_bytes();
                encoded.push(b'f');
                encoded.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                encoded.extend_from_slice(bytes);
            }
            JsonPathToken::Index(index) => {
                encoded.push(b'i');
                encoded.extend_from_slice(&(*index as u64).to_be_bytes());
            }
        }
    }
    encoded
}

fn json_node_key(db_index: u16, key: &str, version: u64, tokens: &[JsonPathToken]) -> Vec<u8> {
    let mut composite_key = json_node_prefix(db_index, key, version);
    composite_key.extend_from_slice(&encode_json_path(tokens));
    composite_key
}

fn encode_json_node(node: &JsonNode) -> Vec<u8> {
    bincode::encode_to_vec(node, bincode::config::standard()).unwrap()
}

fn decode_json_node(raw: &[u8]) -> Option<JsonNode> {
    bincode::decode_from_slice::<JsonNode, _>(raw, bincode::config::standard())
        .ok()
        .map(|(node, _)| node)
}

fn json_node_from_value(value: &JsonValue) -> Result<JsonNode, Error> {
    match value {
        JsonValue::Object(object) => Ok(JsonNode::Object(object.keys().cloned().collect())),
        JsonValue::Array(array) => Ok(JsonNode::Array(array.len())),
        _ => serde_json::to_string(value)
            .map(JsonNode::Scalar)
            .map_err(|_| Error::msg("ERR failed to encode JSON value")),
    }
}

fn json_scalar_to_value(raw: &str) -> Result<JsonValue, Error> {
    let value: JsonValue =
        serde_json::from_str(raw).map_err(|_| Error::msg("Type parsing error"))?;
    if value.is_object() || value.is_array() {
        return Err(Error::msg("Type parsing error"));
    }
    Ok(value)
}

fn write_json_subtree_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &mut Vec<JsonPathToken>,
    value: &JsonValue,
) -> Result<(), Error> {
    let node_key = json_node_key(db_index, key, version, tokens);
    batch.put(&node_key, &encode_json_node(&json_node_from_value(value)?));

    match value {
        JsonValue::Object(object) => {
            for (field, child) in object {
                tokens.push(JsonPathToken::Field(field.clone()));
                write_json_subtree_to_batch(batch, db_index, key, version, tokens, child)?;
                tokens.pop();
            }
        }
        JsonValue::Array(array) => {
            for (index, child) in array.iter().enumerate() {
                tokens.push(JsonPathToken::Index(index));
                write_json_subtree_to_batch(batch, db_index, key, version, tokens, child)?;
                tokens.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

fn delete_json_subtree_to_batch(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &[JsonPathToken],
) {
    let start = json_node_key(db_index, key, version, tokens);
    batch.delete(&start);
    let prefix = if tokens.is_empty() {
        json_node_prefix(db_index, key, version)
    } else {
        start
    };
    for (node_key, _) in store.scan_prefix_raw(&prefix) {
        batch.delete(&node_key);
    }
}

fn delete_json_nodes_to_batch(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
) {
    for (node_key, _) in store.scan_prefix_raw(&json_node_prefix(db_index, key, version)) {
        batch.delete(&node_key);
    }
}

pub fn decode_string_bytes_slice(raw: &[u8]) -> Option<&[u8]> {
    if raw.len() >= 17 && raw[16] == TYPE_STRING {
        Some(&raw[17..])
    } else {
        None
    }
}

/// 只解码 expire_ms，不解码 Structure（用于快速检查过期）
fn decode_expire_ms(raw: &[u8]) -> u64 {
    if let Some(meta) = decode_list_meta(raw) {
        return meta.expire_ms;
    }
    if let Some(meta) = decode_stream_meta(raw) {
        return meta.expire_ms;
    }
    if raw.len() < 8 {
        return 0;
    }
    u64::from_be_bytes(raw[0..8].try_into().unwrap())
}

fn encode_hash_meta(expire_ms: u64, version: u64) -> Vec<u8> {
    encode_hash_meta_with_field_ttl_flag(expire_ms, version, false)
}

fn encode_hash_meta_with_field_ttl_flag(
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

fn encode_set_meta(expire_ms: u64, version: u64, len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(25);
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.push(TYPE_SET);
    buf.extend_from_slice(&(len as u64).to_be_bytes());
    buf
}

fn encode_list_meta(expire_ms: u64, version: u64, head: i64, tail: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(36);
    buf.extend_from_slice(&LIST_META_MAGIC);
    buf.extend_from_slice(&expire_ms.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&head.to_be_bytes());
    buf.extend_from_slice(&tail.to_be_bytes());
    buf
}

fn encode_stream_meta(meta: StreamMeta) -> Vec<u8> {
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

fn encode_zset_meta(expire_ms: u64, version: u64) -> Vec<u8> {
    encode_entry(&Structure::SortedSet(BTreeMap::new()), expire_ms, version)
}

fn decode_list_meta(raw: &[u8]) -> Option<ListMeta> {
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

fn decode_stream_meta(raw: &[u8]) -> Option<StreamMeta> {
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

fn decode_set_meta(raw: &[u8]) -> Option<SetMeta> {
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
struct HashMeta {
    expire_ms: u64,
    version: u64,
    may_have_field_ttl: bool,
}

const HASH_META_COMPACT_LEN: usize = 18;
const HASH_META_FLAG_MAY_HAVE_FIELD_TTL: u8 = 0x01;

fn decode_hash_meta(raw: &[u8]) -> Option<HashMeta> {
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

fn decode_hash_meta_checked(raw: &[u8]) -> Result<HashMeta, Error> {
    let Some(header) = decode_meta_header(raw) else {
        return Err(Error::msg("Failed to decode hash metadata"));
    };
    if header.type_tag != TYPE_HASH {
        return Err(Error::msg(WRONG_TYPE_ERROR));
    }
    decode_hash_meta(raw).ok_or_else(|| Error::msg("Failed to decode hash metadata"))
}

fn re_encode_meta_with_version(raw: &[u8], new_version: u64) -> Vec<u8> {
    let mut new_raw = raw.to_vec();
    if new_raw.len() >= 16 {
        new_raw[8..16].copy_from_slice(&new_version.to_be_bytes());
    }
    new_raw
}

fn hash_field_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + HASH_FIELD_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&HASH_FIELD_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn hash_field_key(db_index: u16, key: &str, version: u64, field: &str) -> Vec<u8> {
    let mut composite_key = hash_field_prefix(db_index, key, version);
    composite_key.extend_from_slice(field.as_bytes());
    composite_key
}

fn hash_field_expire_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + HASH_FIELD_EXPIRE_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&HASH_FIELD_EXPIRE_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn hash_field_expire_key(db_index: u16, key: &str, version: u64, field: &str) -> Vec<u8> {
    let mut composite_key = hash_field_expire_prefix(db_index, key, version);
    composite_key.extend_from_slice(field.as_bytes());
    composite_key
}

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

fn stream_entry_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_ENTRY_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&STREAM_ENTRY_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_group_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_GROUP_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&STREAM_GROUP_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_pel_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_PEL_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&STREAM_PEL_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_consumer_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_CONSUMER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&db_prefix(db_index));
    prefix.extend_from_slice(&STREAM_CONSUMER_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_group_key(db_index: u16, key: &str, version: u64, group: &str) -> Vec<u8> {
    let mut composite_key = stream_group_prefix(db_index, key, version);
    composite_key.extend_from_slice(group.as_bytes());
    composite_key
}

fn stream_pel_group_prefix(db_index: u16, key: &str, version: u64, group: &str) -> Vec<u8> {
    let mut prefix = stream_pel_prefix(db_index, key, version);
    prefix.extend_from_slice(&(group.len() as u32).to_be_bytes());
    prefix.extend_from_slice(group.as_bytes());
    prefix
}

fn stream_consumer_group_prefix(db_index: u16, key: &str, version: u64, group: &str) -> Vec<u8> {
    let mut prefix = stream_consumer_prefix(db_index, key, version);
    prefix.extend_from_slice(&(group.len() as u32).to_be_bytes());
    prefix.extend_from_slice(group.as_bytes());
    prefix
}

fn stream_pel_key(db_index: u16, key: &str, version: u64, group: &str, id: StreamId) -> Vec<u8> {
    let mut composite_key = stream_pel_group_prefix(db_index, key, version, group);
    composite_key.extend_from_slice(&id.ms.to_be_bytes());
    composite_key.extend_from_slice(&id.seq.to_be_bytes());
    composite_key
}

fn stream_consumer_key(
    db_index: u16,
    key: &str,
    version: u64,
    group: &str,
    consumer: &str,
) -> Vec<u8> {
    let mut composite_key = stream_consumer_group_prefix(db_index, key, version, group);
    composite_key.extend_from_slice(consumer.as_bytes());
    composite_key
}

fn decode_stream_pel_id(prefix: &[u8], key: &[u8]) -> Option<StreamId> {
    let suffix = key.strip_prefix(prefix)?;
    if suffix.len() != 16 {
        return None;
    }
    Some(StreamId {
        ms: u64::from_be_bytes(suffix[0..8].try_into().ok()?),
        seq: u64::from_be_bytes(suffix[8..16].try_into().ok()?),
    })
}

fn stream_entry_key(db_index: u16, key: &str, version: u64, id: StreamId) -> Vec<u8> {
    let mut composite_key = stream_entry_prefix(db_index, key, version);
    composite_key.extend_from_slice(&id.ms.to_be_bytes());
    composite_key.extend_from_slice(&id.seq.to_be_bytes());
    composite_key
}

fn encode_stream_group_state(state: &StreamGroupState) -> Vec<u8> {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&state.last_delivered_id.ms.to_be_bytes());
    buf.extend_from_slice(&state.last_delivered_id.seq.to_be_bytes());
    buf.extend_from_slice(&state.entries_read.to_be_bytes());
    buf
}

fn decode_stream_group_state(raw: &[u8]) -> Option<StreamGroupState> {
    if raw.len() != 24 {
        return None;
    }
    Some(StreamGroupState {
        last_delivered_id: StreamId {
            ms: u64::from_be_bytes(raw[0..8].try_into().ok()?),
            seq: u64::from_be_bytes(raw[8..16].try_into().ok()?),
        },
        entries_read: u64::from_be_bytes(raw[16..24].try_into().ok()?),
    })
}

fn encode_stream_pel_state(state: &StreamPelState) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + state.consumer.len() + 4);
    buf.extend_from_slice(&state.last_delivery_ms.to_be_bytes());
    buf.extend_from_slice(&state.deliveries.to_be_bytes());
    buf.extend_from_slice(&(state.consumer.len() as u32).to_be_bytes());
    buf.extend_from_slice(state.consumer.as_bytes());
    buf
}

fn decode_stream_pel_state(raw: &[u8]) -> Option<StreamPelState> {
    if raw.len() < 20 {
        return None;
    }
    let last_delivery_ms = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let deliveries = u64::from_be_bytes(raw[8..16].try_into().ok()?);
    let len = u32::from_be_bytes(raw[16..20].try_into().ok()?) as usize;
    let consumer = String::from_utf8(raw.get(20..20 + len)?.to_vec()).ok()?;
    Some(StreamPelState {
        consumer,
        last_delivery_ms,
        deliveries,
    })
}

fn encode_stream_consumer_state(state: &StreamConsumerState) -> Vec<u8> {
    state.last_seen_ms.to_be_bytes().to_vec()
}

fn decode_stream_consumer_state(raw: &[u8]) -> Option<StreamConsumerState> {
    if raw.len() != 8 {
        return None;
    }
    Some(StreamConsumerState {
        last_seen_ms: u64::from_be_bytes(raw.try_into().ok()?),
    })
}

fn decode_stream_entry_id(prefix: &[u8], entry_key: &[u8]) -> Option<StreamId> {
    let suffix = entry_key.strip_prefix(prefix)?;
    if suffix.len() != 16 {
        return None;
    }
    Some(StreamId {
        ms: u64::from_be_bytes(suffix[0..8].try_into().ok()?),
        seq: u64::from_be_bytes(suffix[8..16].try_into().ok()?),
    })
}

fn encode_stream_entry(fields: &[(String, String)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for (field, value) in fields {
        buf.extend_from_slice(&(field.len() as u32).to_be_bytes());
        buf.extend_from_slice(field.as_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    buf
}

fn decode_stream_entry(raw: &[u8]) -> Option<Vec<(String, String)>> {
    let mut offset = 0usize;
    let count = read_u32(raw, &mut offset)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let field = read_string(raw, &mut offset)?;
        let value = read_string(raw, &mut offset)?;
        fields.push((field, value));
    }
    (offset == raw.len()).then_some(fields)
}

fn read_u32(raw: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = raw.get(*offset..end)?;
    *offset = end;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn decode_u64_be(raw: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(raw.get(..8)?.try_into().ok()?))
}

fn read_string(raw: &[u8], offset: &mut usize) -> Option<String> {
    let len = read_u32(raw, offset)? as usize;
    let end = offset.checked_add(len)?;
    let bytes = raw.get(*offset..end)?;
    *offset = end;
    String::from_utf8(bytes.to_vec()).ok()
}

fn parse_stream_id(text: &str) -> Option<StreamId> {
    let (ms, seq) = text.split_once('-')?;
    Some(StreamId {
        ms: ms.parse().ok()?,
        seq: seq.parse().ok()?,
    })
}
