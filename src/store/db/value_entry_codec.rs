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
