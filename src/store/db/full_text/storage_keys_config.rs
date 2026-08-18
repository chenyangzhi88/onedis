use super::*;
const FULLTEXT_MUTATION_BATCH_MAGIC: &[u8; 8] = b"\0ODFTB01";

pub(super) fn encode_record<T: Encode>(value: &T) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|_| Error::msg("ERR failed to encode fulltext record"))
}

pub(super) fn encode_fulltext_mutation_batch(
    incarnation: u64,
    kind: FullTextMutationKind,
    keys: &[&str],
) -> Result<Vec<u8>, Error> {
    let record = FullTextMutationBatchRecord {
        incarnation,
        kind,
        keys: keys.iter().map(|key| (*key).to_string()).collect(),
    };
    let encoded = encode_record(&record)?;
    let mut raw = Vec::with_capacity(FULLTEXT_MUTATION_BATCH_MAGIC.len() + encoded.len());
    raw.extend_from_slice(FULLTEXT_MUTATION_BATCH_MAGIC);
    raw.extend_from_slice(&encoded);
    Ok(raw)
}

pub(super) fn decode_fulltext_mutation_records(
    raw: &[u8],
) -> Result<Vec<FullTextMutationRecord>, Error> {
    let Some(encoded) = raw.strip_prefix(FULLTEXT_MUTATION_BATCH_MAGIC) else {
        return decode_record(raw).map(|record| vec![record]);
    };
    let batch = decode_record::<FullTextMutationBatchRecord>(encoded)?;
    if batch.keys.is_empty() {
        return Err(Error::msg("ERR empty fulltext mutation batch"));
    }
    Ok(batch
        .keys
        .into_iter()
        .map(|key| FullTextMutationRecord {
            incarnation: batch.incarnation,
            kind: batch.kind,
            key,
        })
        .collect())
}

pub(super) fn decode_record<T: Decode<()>>(raw: &[u8]) -> Result<T, Error> {
    bincode::decode_from_slice::<T, _>(raw, bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(|error| Error::msg(format!("ERR failed to decode fulltext record: {error}")))
}

pub(super) fn decode_fulltext_meta(raw: &[u8]) -> Result<FullTextIndexMeta, Error> {
    decode_record::<FullTextIndexMeta>(raw).or_else(|current_error| {
        decode_record::<LegacyFullTextIndexMetaV1>(raw)
            .map(FullTextIndexMeta::from)
            .map_err(|_| current_error)
    })
}

pub(super) fn decode_fulltext_meta_for_index(
    index: &str,
    raw: &[u8],
) -> Result<FullTextIndexMeta, Error> {
    let mut meta = decode_fulltext_meta(raw)?;
    if meta.active_storage.is_empty() {
        meta.active_storage = index.to_string();
    }
    Ok(meta)
}

pub(super) fn fulltext_meta_prefix(db_index: u16) -> Vec<u8> {
    let mut key = internal_prefix(db_index);
    key.extend_from_slice(&FULLTEXT_META_NAMESPACE);
    key
}

pub(super) fn fulltext_alias_prefix(db_index: u16) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0alias\0");
    key
}

pub(super) fn fulltext_alias_key(db_index: u16, alias: &str) -> Vec<u8> {
    let mut key = fulltext_alias_prefix(db_index);
    key.extend_from_slice(alias.as_bytes());
    key
}

pub(super) fn fulltext_alias_from_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_alias_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

pub(super) fn fulltext_config_key(db_index: u16, name: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0config\0");
    key.extend_from_slice(name.as_bytes());
    key
}

pub(super) fn fulltext_repair_marker_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0repair\0");
    key.extend_from_slice(index.as_bytes());
    key
}

pub(super) fn fulltext_dict_root_prefix(db_index: u16) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0dict\0");
    key
}

pub(super) fn fulltext_dict_prefix(db_index: u16, dict: &str) -> Vec<u8> {
    let mut key = fulltext_dict_root_prefix(db_index);
    key.extend_from_slice(dict.as_bytes());
    key.push(0x00);
    key
}

pub(super) fn fulltext_dict_term_key(db_index: u16, dict: &str, term: &str) -> Vec<u8> {
    let mut key = fulltext_dict_prefix(db_index, dict);
    key.extend_from_slice(term.as_bytes());
    key
}

pub(super) fn fulltext_dict_term_from_key(db_index: u16, dict: &str, key: &[u8]) -> Option<String> {
    let prefix = fulltext_dict_prefix(db_index, dict);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

#[cfg(test)]
pub(super) fn fulltext_any_dict_term_from_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_dict_root_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let split = rest.iter().position(|byte| *byte == 0x00)?;
    String::from_utf8(rest[split + 1..].to_vec()).ok()
}

pub(super) fn fulltext_suggest_prefix(db_index: u16, key_name: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0sug\0");
    key.extend_from_slice(key_name.as_bytes());
    key.push(0x00);
    key
}

pub(super) fn fulltext_suggest_key(db_index: u16, key_name: &str, string: &str) -> Vec<u8> {
    let mut key = fulltext_suggest_prefix(db_index, key_name);
    key.extend_from_slice(string.as_bytes());
    key
}

pub(super) fn fulltext_suggest_string_from_key(
    db_index: u16,
    key_name: &str,
    key: &[u8],
) -> Option<String> {
    let prefix = fulltext_suggest_prefix(db_index, key_name);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

pub(super) fn fulltext_syn_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0syn\0");
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

pub(super) fn fulltext_syn_key(db_index: u16, index: &str, group: &str) -> Vec<u8> {
    let mut key = fulltext_syn_prefix(db_index, index);
    key.extend_from_slice(group.as_bytes());
    key
}

pub(super) fn fulltext_syn_group_from_key(
    db_index: u16,
    index: &str,
    key: &[u8],
) -> Option<String> {
    let prefix = fulltext_syn_prefix(db_index, index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

pub(super) fn fulltext_file_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = internal_prefix(db_index);
    key.extend_from_slice(&FULLTEXT_FILE_NAMESPACE);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

pub(super) fn fulltext_generation_storage_name(index: &str, generation: u64) -> String {
    format!("__onedis_fulltext_generation__:{generation}:{index}")
}

pub(super) fn fulltext_meta_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key.extend_from_slice(&0u64.to_be_bytes());
    key.extend_from_slice(b"meta");
    key
}

pub(super) fn fulltext_index_from_meta_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_meta_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let split = rest.iter().position(|byte| *byte == 0x00)?;
    if split == 0 {
        return None;
    }
    let suffix = &rest[split + 1..];
    if suffix.len() != 12 || suffix[..8] != 0u64.to_be_bytes() || suffix[8..] != *b"meta" {
        return None;
    }
    String::from_utf8(rest[..split].to_vec()).ok()
}

pub(super) fn fulltext_temporary_activity_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key.extend_from_slice(&0u64.to_be_bytes());
    key.extend_from_slice(b"temp");
    key
}

pub(super) fn fulltext_outbox_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = internal_prefix(db_index);
    key.extend_from_slice(&FULLTEXT_OUTBOX_NAMESPACE);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

pub(super) fn fulltext_outbox_key(db_index: u16, index: &str, seq: u64) -> Vec<u8> {
    let mut key = fulltext_outbox_prefix(db_index, index);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

pub(super) fn fulltext_outbox_latest_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0outbox_latest\0");
    key.extend_from_slice(index.as_bytes());
    key
}

pub(super) fn fulltext_index_from_outbox_latest_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_meta_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let index = rest.strip_prefix(b"\0outbox_latest\0")?;
    (!index.is_empty())
        .then(|| String::from_utf8(index.to_vec()).ok())
        .flatten()
}

pub(super) fn fulltext_outbox_seq_from_key(db_index: u16, index: &str, key: &[u8]) -> Option<u64> {
    let prefix = fulltext_outbox_prefix(db_index, index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

pub(super) fn fulltext_index_and_seq_from_outbox_key(
    db_index: u16,
    key: &[u8],
) -> Option<(String, u64)> {
    let mut prefix = internal_prefix(db_index);
    prefix.extend_from_slice(&FULLTEXT_OUTBOX_NAMESPACE);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let split = rest.iter().position(|byte| *byte == 0x00)?;
    if split == 0 || rest.len() != split.saturating_add(1 + std::mem::size_of::<u64>()) {
        return None;
    }
    let index = String::from_utf8(rest[..split].to_vec()).ok()?;
    let seq = u64::from_be_bytes(rest[split + 1..].try_into().ok()?);
    Some((index, seq))
}

pub(super) fn current_fulltext_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn delete_prefix_to_batch(
    batch: &mut WriteBatch,
    store: &crate::store::kv_store::KvStore,
    prefix: &[u8],
) -> Result<(), Error> {
    if let Some(end) = prefix_exclusive_upper_bound(prefix) {
        batch
            .delete_range(prefix, &end)
            .map_err(|error| Error::msg(error.to_string()))?;
    } else {
        for (key, _) in store.scan_prefix_raw(prefix) {
            batch
                .delete(&key)
                .map_err(|error| Error::msg(error.to_string()))?;
        }
    }
    Ok(())
}

pub(super) fn fulltext_supported_config_names() -> Vec<&'static str> {
    let defaults = fulltext_default_config();
    defaults.keys().copied().collect()
}

pub(super) fn fulltext_default_config_value(name: &str) -> Option<&'static str> {
    fulltext_default_config().get(name).copied().or_else(|| {
        fulltext_default_config()
            .get(&name.to_ascii_uppercase().as_str())
            .copied()
    })
}

pub(super) fn fulltext_default_config() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("CLUSTER_ALIAS_PROPAGATION", "local"),
        ("CLUSTER_CONFIG_PROPAGATION", "local"),
        ("CLUSTER_ENABLED", "false"),
        ("CLUSTER_ROUTING", "local"),
        ("CLUSTER_SHARD_ID", "0"),
        ("CLUSTER_SHARDS", "1"),
        ("CLUSTER_VECTOR_MERGE", "local"),
        ("CHECKPOINT_INTERVAL_MS", "1000"),
        ("CONSISTENCY", "CONSISTENT"),
        ("DEFAULT_DIALECT", "2"),
        ("FRISOINI", ""),
        ("MAXAGGREGATERESULTS", "10000"),
        ("MAXEXPANSIONS", "200"),
        ("MAXPREFIXEXPANSIONS", "200"),
        ("MAXSEARCHRESULTS", "10000"),
        ("MEMORY_BUDGET_AGGREGATE_CURSOR_BYTES", "16777216"),
        ("MEMORY_BUDGET_READER_BYTES", "67108864"),
        ("MEMORY_BUDGET_SORT_BYTES", "16777216"),
        ("MEMORY_BUDGET_VECTOR_HEAP_BYTES", "16777216"),
        ("MEMORY_BUDGET_WRITER_BYTES", "50000000"),
        ("DIRECTORY_CACHE_BYTES", "67108864"),
        ("MERGE_DELETE_RATIO", "0.25"),
        ("MERGE_MAX_DOCS", "10000000"),
        ("MERGE_MIN_LAYER_DOCS", "10000"),
        ("MERGE_MIN_SEGMENTS", "4"),
        ("MINPREFIX", "2"),
        ("NOGC", "false"),
        ("ON_TIMEOUT", "RETURN"),
        ("OUTBOX_COMPACT_THRESHOLD", "1024"),
        ("REFRESH_INTERVAL_MS", "100"),
        ("REFRESH_MAX_BYTES", "4194304"),
        ("REFRESH_MAX_DOCS", "8192"),
        ("REFRESH_TIMEOUT_MS", "500"),
        ("REPAIR_THROTTLE_MS", "1000"),
        ("TIMEOUT", "500"),
    ])
}

pub(super) fn validate_fulltext_config_name(name: &str) -> Result<(), Error> {
    if fulltext_default_config().contains_key(name) {
        Ok(())
    } else {
        Err(Error::msg("ERR unsupported fulltext config option"))
    }
}

pub(super) fn validate_fulltext_config_value(name: &str, value: &str) -> Result<(), Error> {
    validate_fulltext_config_name(name)?;
    match name {
        "DEFAULT_DIALECT" => {
            let dialect = value
                .parse::<u8>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if (1..=4).contains(&dialect) {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "MINPREFIX" => {
            let min_prefix = value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if min_prefix > 0 {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "REFRESH_MAX_DOCS" | "REFRESH_MAX_BYTES" => {
            value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            Ok(())
        }
        "MEMORY_BUDGET_WRITER_BYTES" => {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if parsed < 15_000_000 {
                Err(Error::msg("ERR fulltext writer memory budget is too small"))
            } else {
                Ok(())
            }
        }
        "DIRECTORY_CACHE_BYTES"
        | "MEMORY_BUDGET_READER_BYTES"
        | "MEMORY_BUDGET_SORT_BYTES"
        | "MEMORY_BUDGET_AGGREGATE_CURSOR_BYTES"
        | "MEMORY_BUDGET_VECTOR_HEAP_BYTES" => {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if parsed == 0 {
                Err(Error::msg("ERR invalid fulltext config value"))
            } else {
                Ok(())
            }
        }
        "MAXSEARCHRESULTS"
        | "MAXAGGREGATERESULTS"
        | "MAXEXPANSIONS"
        | "MAXPREFIXEXPANSIONS"
        | "CLUSTER_SHARD_ID"
        | "CHECKPOINT_INTERVAL_MS"
        | "REFRESH_INTERVAL_MS"
        | "REFRESH_TIMEOUT_MS"
        | "OUTBOX_COMPACT_THRESHOLD"
        | "REPAIR_THROTTLE_MS"
        | "TIMEOUT" => {
            value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            Ok(())
        }
        "MERGE_MAX_DOCS" | "MERGE_MIN_LAYER_DOCS" | "MERGE_MIN_SEGMENTS" => {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if parsed > 0 {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "MERGE_DELETE_RATIO" => {
            let parsed = value
                .parse::<f32>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if parsed.is_finite() && parsed > 0.0 && parsed <= 1.0 {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "CLUSTER_SHARDS" => {
            let shards = value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if shards > 0 {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "NOGC" => {
            let normalized = value.to_ascii_lowercase();
            if matches!(
                normalized.as_str(),
                "true" | "false" | "1" | "0" | "yes" | "no"
            ) {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "CLUSTER_ENABLED" => {
            let normalized = value.to_ascii_lowercase();
            if matches!(
                normalized.as_str(),
                "true" | "false" | "1" | "0" | "yes" | "no"
            ) {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "FRISOINI" => Ok(()),
        "ON_TIMEOUT" => {
            let normalized = value.to_ascii_uppercase();
            if normalized == "RETURN" || normalized == "FAIL" {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext config value"))
            }
        }
        "CLUSTER_ROUTING"
        | "CLUSTER_ALIAS_PROPAGATION"
        | "CLUSTER_CONFIG_PROPAGATION"
        | "CLUSTER_VECTOR_MERGE" => {
            let normalized = value.to_ascii_lowercase();
            if normalized == "local" {
                Ok(())
            } else {
                Err(Error::msg("ERR unsupported fulltext cluster mode"))
            }
        }
        "CONSISTENCY" => {
            let normalized = value.to_ascii_uppercase();
            if normalized == "CONSISTENT" || normalized == "EVENTUAL" {
                Ok(())
            } else {
                Err(Error::msg("ERR invalid fulltext consistency mode"))
            }
        }
        _ => Err(Error::msg("ERR unsupported fulltext config option")),
    }
}
