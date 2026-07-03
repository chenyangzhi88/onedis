fn stream_entry_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_ENTRY_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&STREAM_ENTRY_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_group_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_GROUP_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&STREAM_GROUP_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_pel_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_PEL_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&STREAM_PEL_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

fn stream_consumer_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + STREAM_CONSUMER_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
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
