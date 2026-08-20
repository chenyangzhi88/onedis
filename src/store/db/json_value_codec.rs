use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::store::db) enum JsonPathToken {
    Field(String),
    Index(usize),
}

const MAX_JSON_PATH_BYTES: usize = 4 * 1024;
const MAX_JSON_PATH_DEPTH: usize = 128;
pub(in crate::store::db) const SMALL_JSON_MAX_ENCODED_BYTES: usize = 16 * 1024;
pub(in crate::store::db) const SMALL_JSON_MAX_NODES: usize = 256;
const PACKED_JSON_MAGIC: [u8; 4] = [0xf1, b'J', b'P', 1];
const PACKED_JSON_HEADER_LEN: usize = 17 + PACKED_JSON_MAGIC.len() + 4;

pub(in crate::store::db) fn is_packed_json_raw(raw: &[u8]) -> bool {
    raw.len() >= PACKED_JSON_HEADER_LEN
        && raw.get(16) == Some(&TYPE_JSON)
        && raw[17..17 + PACKED_JSON_MAGIC.len()] == PACKED_JSON_MAGIC
}

pub(in crate::store::db) fn decode_packed_json(raw: &[u8]) -> Option<JsonValue> {
    if !is_packed_json_raw(raw) {
        return None;
    }
    let offset = 17 + PACKED_JSON_MAGIC.len();
    let len = u32::from_be_bytes(raw.get(offset..offset + 4)?.try_into().ok()?) as usize;
    let payload = raw.get(offset + 4..offset + 4 + len)?;
    if offset + 4 + len != raw.len() {
        return None;
    }
    serde_json::from_slice(payload).ok()
}

pub(in crate::store::db) fn encode_packed_json(
    expire_ms: u64,
    value: &JsonValue,
) -> Option<Vec<u8>> {
    if json_node_count(value) > SMALL_JSON_MAX_NODES {
        return None;
    }
    let payload = serde_json::to_vec(value).ok()?;
    let encoded_len = PACKED_JSON_HEADER_LEN.checked_add(payload.len())?;
    if encoded_len > SMALL_JSON_MAX_ENCODED_BYTES {
        return None;
    }
    let mut raw = Vec::with_capacity(encoded_len);
    raw.extend_from_slice(&expire_ms.to_be_bytes());
    raw.extend_from_slice(&0u64.to_be_bytes());
    raw.push(TYPE_JSON);
    raw.extend_from_slice(&PACKED_JSON_MAGIC);
    raw.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    raw.extend_from_slice(&payload);
    Some(raw)
}

pub(in crate::store::db) fn json_node_count(value: &JsonValue) -> usize {
    let mut count = 0usize;
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        count = count.saturating_add(1);
        match value {
            JsonValue::Array(values) => pending.extend(values),
            JsonValue::Object(values) => pending.extend(values.values()),
            _ => {}
        }
    }
    count
}

pub(in crate::store::db) fn json_value_at_path<'a>(
    root: &'a JsonValue,
    tokens: &[JsonPathToken],
) -> Option<&'a JsonValue> {
    let mut current = root;
    for token in tokens {
        current = match (token, current) {
            (JsonPathToken::Field(field), JsonValue::Object(object)) => object.get(field)?,
            (JsonPathToken::Index(index), JsonValue::Array(array)) => array.get(*index)?,
            _ => return None,
        };
    }
    Some(current)
}

pub(in crate::store::db) fn json_value_at_path_mut<'a>(
    root: &'a mut JsonValue,
    tokens: &[JsonPathToken],
) -> Option<&'a mut JsonValue> {
    let mut current = root;
    for token in tokens {
        current = match (token, current) {
            (JsonPathToken::Field(field), JsonValue::Object(object)) => object.get_mut(field)?,
            (JsonPathToken::Index(index), JsonValue::Array(array)) => array.get_mut(*index)?,
            _ => return None,
        };
    }
    Some(current)
}

pub(in crate::store::db) fn remove_json_value_at_path(
    root: &mut JsonValue,
    tokens: &[JsonPathToken],
) -> bool {
    let Some((last, parents)) = tokens.split_last() else {
        return false;
    };
    let Some(parent) = json_value_at_path_mut(root, parents) else {
        return false;
    };
    match (last, parent) {
        (JsonPathToken::Field(field), JsonValue::Object(object)) => object.remove(field).is_some(),
        (JsonPathToken::Index(index), JsonValue::Array(array)) if *index < array.len() => {
            array.remove(*index);
            true
        }
        _ => false,
    }
}

pub(in crate::store::db) fn parse_json_path(path: &str) -> Result<Vec<JsonPathToken>, Error> {
    if path.is_empty() || path.len() > MAX_JSON_PATH_BYTES {
        return Err(Error::msg("ERR invalid JSON path"));
    }
    if path == "$" || path == "." {
        return Ok(Vec::new());
    }

    let bytes = path.as_bytes();
    let mut idx = match bytes.first() {
        Some(b'$') => 1,
        Some(b'.') if bytes.get(1) == Some(&b'[') => 1,
        Some(b'.') => 0,
        _ => return Err(Error::msg("ERR invalid JSON path")),
    };
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
        if tokens.len() > MAX_JSON_PATH_DEPTH {
            return Err(Error::msg("ERR invalid JSON path"));
        }
    }

    Ok(tokens)
}

pub(in crate::store::db) fn json_type_name(value: &JsonValue) -> &'static str {
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

pub(in crate::store::db) fn validate_json_value_limits(
    value: &JsonValue,
    encoded_bytes: usize,
) -> Result<(), Error> {
    let limits = crate::resource_limits::resource_limits()?;
    if encoded_bytes > limits.json_document_bytes {
        return Err(Error::msg(
            "ERR JSON document exceeds configured byte limit",
        ));
    }
    let mut nodes = 0usize;
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > limits.json_nodes {
            return Err(Error::msg(
                "ERR JSON document exceeds configured node limit",
            ));
        }
        match value {
            JsonValue::Array(values) => pending.extend(values),
            JsonValue::Object(values) => pending.extend(values.values()),
            _ => {}
        }
    }
    Ok(())
}

pub(in crate::store::db) fn json_node_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + JSON_NODE_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&JSON_NODE_NAMESPACE);
    append_versioned_sub_key_owner(&mut prefix, key.as_bytes());
    prefix.push(0x00);
    prefix.extend_from_slice(&version.to_be_bytes());
    prefix
}

pub(in crate::store::db) fn encode_json_path(tokens: &[JsonPathToken]) -> Vec<u8> {
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

fn decode_json_path(mut encoded: &[u8]) -> Option<Vec<JsonPathToken>> {
    let mut tokens = Vec::new();
    while !encoded.is_empty() {
        match *encoded.first()? {
            b'f' => {
                let len = usize::try_from(u32::from_be_bytes(encoded.get(1..5)?.try_into().ok()?))
                    .ok()?;
                let field = std::str::from_utf8(encoded.get(5..5 + len)?)
                    .ok()?
                    .to_string();
                tokens.push(JsonPathToken::Field(field));
                encoded = encoded.get(5 + len..)?;
            }
            b'i' => {
                let id = u64::from_be_bytes(encoded.get(1..9)?.try_into().ok()?);
                tokens.push(JsonPathToken::Index(usize::try_from(id).ok()?));
                encoded = encoded.get(9..)?;
            }
            _ => return None,
        }
    }
    Some(tokens)
}

pub(in crate::store::db) fn json_node_key(
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &[JsonPathToken],
) -> Vec<u8> {
    let mut composite_key = json_node_prefix(db_index, key, version);
    composite_key.extend_from_slice(&encode_json_path(tokens));
    composite_key
}

pub(in crate::store::db) fn encode_json_node(node: &JsonNode) -> Vec<u8> {
    bincode::encode_to_vec(node, bincode::config::standard()).unwrap()
}

pub(in crate::store::db) fn decode_json_node(raw: &[u8]) -> Option<JsonNode> {
    bincode::decode_from_slice::<JsonNode, _>(raw, bincode::config::standard())
        .ok()
        .map(|(node, _)| node)
}

pub(in crate::store::db) fn json_node_from_value(value: &JsonValue) -> Result<JsonNode, Error> {
    match value {
        JsonValue::Object(_) => Ok(JsonNode::Object(0)),
        JsonValue::Array(array) => Ok(JsonNode::Array(
            (0..array.len()).map(|index| index as u64).collect(),
        )),
        _ => serde_json::to_string(value)
            .map(JsonNode::Scalar)
            .map_err(|_| Error::msg("ERR failed to encode JSON value")),
    }
}

pub(in crate::store::db) fn json_scalar_to_value(raw: &str) -> Result<JsonValue, Error> {
    let value: JsonValue =
        serde_json::from_str(raw).map_err(|_| Error::msg("Type parsing error"))?;
    if value.is_object() || value.is_array() {
        return Err(Error::msg("Type parsing error"));
    }
    Ok(value)
}

pub(in crate::store::db) fn write_json_subtree_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &mut Vec<JsonPathToken>,
    value: &JsonValue,
) -> Result<(), Error> {
    let node_key = json_node_key(db_index, key, version, tokens);
    batch
        .put(&node_key, &encode_json_node(&json_node_from_value(value)?))
        .map_err(|error| Error::msg(error.to_string()))?;

    match value {
        JsonValue::Object(object) => {
            for (field, child) in object {
                tokens.push(JsonPathToken::Field(field.clone()));
                write_json_subtree_to_batch(batch, db_index, key, version, tokens, child)?;
                tokens.pop();
            }
        }
        JsonValue::Array(array) => {
            for (element_id, child) in array.iter().enumerate() {
                tokens.push(JsonPathToken::Index(element_id));
                write_json_subtree_to_batch(batch, db_index, key, version, tokens, child)?;
                tokens.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

pub(in crate::store::db) async fn observe_and_delete_json_subtree_to_batch_async(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &[JsonPathToken],
) -> Result<Option<Vec<CompareCondition>>, Error> {
    let start = json_node_key(db_index, key, version, tokens);
    let prefix = if tokens.is_empty() {
        json_node_prefix(db_index, key, version)
    } else {
        start
    };
    let entries = store.scan_prefix_raw_async(&prefix).await?;
    if entries.is_empty() {
        return Ok(None);
    }
    let keys = entries
        .iter()
        .map(|(node_key, _)| node_key.clone())
        .collect::<Vec<_>>();
    let observed = store.multi_get_raw_observed_async(&keys).await?;
    let mut conditions = Vec::with_capacity(entries.len());
    for ((node_key, scanned_value), observation) in entries.into_iter().zip(observed) {
        if observation.value().map(|value| value.as_ref()) != Some(scanned_value.as_slice()) {
            return Ok(None);
        }
        batch
            .delete(&node_key)
            .map_err(|error| Error::msg(error.to_string()))?;
        if node_key != prefix {
            conditions.push(CompareCondition::from_observed(&observation));
        }
    }
    Ok(Some(conditions))
}

pub(in crate::store::db) struct JsonNodeSnapshot {
    nodes: HashMap<Vec<u8>, JsonNode>,
    object_fields: HashMap<Vec<u8>, Vec<String>>,
}

impl JsonNodeSnapshot {
    pub(in crate::store::db) fn from_entries(
        root_prefix: &[u8],
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Self, Error> {
        let mut nodes = HashMap::with_capacity(entries.len());
        let mut object_fields = HashMap::<Vec<u8>, Vec<String>>::new();
        for (raw_key, raw_value) in entries {
            let encoded_path = raw_key
                .strip_prefix(root_prefix)
                .ok_or_else(|| Error::msg("Type parsing error"))?
                .to_vec();
            let node =
                decode_json_node(&raw_value).ok_or_else(|| Error::msg("Type parsing error"))?;
            let tokens =
                decode_json_path(&encoded_path).ok_or_else(|| Error::msg("Type parsing error"))?;
            if let Some((JsonPathToken::Field(field), parent_tokens)) = tokens.split_last() {
                object_fields
                    .entry(encode_json_path(parent_tokens))
                    .or_default()
                    .push(field.clone());
            }
            nodes.insert(encoded_path, node);
        }
        for fields in object_fields.values_mut() {
            fields.sort_unstable();
            fields.dedup();
        }
        Ok(Self {
            nodes,
            object_fields,
        })
    }

    pub(in crate::store::db) fn value_at(
        &self,
        tokens: &mut Vec<JsonPathToken>,
    ) -> Result<Option<JsonValue>, Error> {
        let encoded_path = encode_json_path(tokens);
        let Some(node) = self.nodes.get(&encoded_path) else {
            return Ok(None);
        };
        match node {
            JsonNode::Scalar(raw) => json_scalar_to_value(raw).map(Some),
            JsonNode::Object(_) => {
                let mut object = serde_json::Map::new();
                for field in self.object_fields.get(&encoded_path).into_iter().flatten() {
                    tokens.push(JsonPathToken::Field(field.clone()));
                    let child = self.value_at(tokens)?;
                    tokens.pop();
                    let Some(child) = child else {
                        return Err(Error::msg("Type parsing error"));
                    };
                    object.insert(field.clone(), child);
                }
                Ok(Some(JsonValue::Object(object)))
            }
            JsonNode::Array(element_ids) => {
                let mut array = Vec::with_capacity(element_ids.len());
                for element_id in element_ids {
                    let physical_id = usize::try_from(*element_id)
                        .map_err(|_| Error::msg("Type parsing error"))?;
                    tokens.push(JsonPathToken::Index(physical_id));
                    let child = self.value_at(tokens)?;
                    tokens.pop();
                    let Some(child) = child else {
                        return Err(Error::msg("Type parsing error"));
                    };
                    array.push(child);
                }
                Ok(Some(JsonValue::Array(array)))
            }
        }
    }
}

pub(in crate::store::db) fn delete_json_subtree_to_batch(
    store: &KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    key: &str,
    version: u64,
    tokens: &[JsonPathToken],
) -> Result<(), Error> {
    let start = json_node_key(db_index, key, version, tokens);
    batch
        .delete(&start)
        .map_err(|error| Error::msg(error.to_string()))?;
    let prefix = if tokens.is_empty() {
        json_node_prefix(db_index, key, version)
    } else {
        start
    };
    for (node_key, _) in store.scan_prefix_raw(&prefix)? {
        batch
            .delete(&node_key)
            .map_err(|error| Error::msg(error.to_string()))?;
    }
    Ok(())
}
