use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::store::db) enum JsonPathToken {
    Field(String),
    Index(usize),
}

pub(in crate::store::db) fn parse_json_path(path: &str) -> Result<Vec<JsonPathToken>, Error> {
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

pub(in crate::store::db) fn json_get_path<'a>(
    value: &'a JsonValue,
    tokens: &[JsonPathToken],
) -> Option<&'a JsonValue> {
    let mut current = value;
    for token in tokens {
        current = match token {
            JsonPathToken::Field(field) => current.as_object()?.get(field)?,
            JsonPathToken::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

pub(in crate::store::db) fn json_get_parent_mut<'a>(
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

pub(in crate::store::db) fn json_set_path(
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

pub(in crate::store::db) fn json_del_path(value: &mut JsonValue, tokens: &[JsonPathToken]) -> bool {
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

pub(in crate::store::db) fn json_node_prefix(db_index: u16, key: &str, version: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + JSON_NODE_NAMESPACE.len() + key.len() + 1 + 8);
    prefix.extend_from_slice(&internal_prefix(db_index));
    prefix.extend_from_slice(&JSON_NODE_NAMESPACE);
    prefix.extend_from_slice(key.as_bytes());
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
        JsonValue::Object(object) => Ok(JsonNode::Object(object.keys().cloned().collect())),
        JsonValue::Array(array) => Ok(JsonNode::Array(array.len())),
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

pub(in crate::store::db) fn delete_json_subtree_to_batch(
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

pub(in crate::store::db) fn delete_json_nodes_to_batch(
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
