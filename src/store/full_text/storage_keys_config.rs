fn encode_record<T: Encode>(value: &T) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|_| Error::msg("ERR failed to encode fulltext record"))
}

fn decode_record<T: Decode<()>>(raw: &[u8]) -> Result<T, Error> {
    bincode::decode_from_slice::<T, _>(raw, bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(|_| Error::msg("ERR failed to decode fulltext record"))
}

fn decode_fulltext_meta(raw: &[u8]) -> Result<FullTextIndexMeta, Error> {
    decode_record::<FullTextIndexMeta>(raw)
        .or_else(|_| {
            decode_record::<LegacyPhase2FullTextIndexMeta>(raw).map(FullTextIndexMeta::from)
        })
        .or_else(|_| decode_record::<LegacyFullTextIndexMeta>(raw).map(FullTextIndexMeta::from))
}

fn fulltext_meta_prefix(db_index: u16) -> Vec<u8> {
    let mut key = db_prefix(db_index).to_vec();
    key.extend_from_slice(&FULLTEXT_META_NAMESPACE);
    key
}

fn fulltext_alias_prefix(db_index: u16) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0alias\0");
    key
}

fn fulltext_alias_key(db_index: u16, alias: &str) -> Vec<u8> {
    let mut key = fulltext_alias_prefix(db_index);
    key.extend_from_slice(alias.as_bytes());
    key
}

fn fulltext_alias_from_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_alias_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

fn fulltext_config_key(db_index: u16, name: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0config\0");
    key.extend_from_slice(name.as_bytes());
    key
}

fn fulltext_repair_marker_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0repair\0");
    key.extend_from_slice(index.as_bytes());
    key
}

fn fulltext_dict_root_prefix(db_index: u16) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0dict\0");
    key
}

fn fulltext_dict_prefix(db_index: u16, dict: &str) -> Vec<u8> {
    let mut key = fulltext_dict_root_prefix(db_index);
    key.extend_from_slice(dict.as_bytes());
    key.push(0x00);
    key
}

fn fulltext_dict_term_key(db_index: u16, dict: &str, term: &str) -> Vec<u8> {
    let mut key = fulltext_dict_prefix(db_index, dict);
    key.extend_from_slice(term.as_bytes());
    key
}

fn fulltext_dict_term_from_key(db_index: u16, dict: &str, key: &[u8]) -> Option<String> {
    let prefix = fulltext_dict_prefix(db_index, dict);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

fn fulltext_any_dict_term_from_key(db_index: u16, key: &[u8]) -> Option<String> {
    let prefix = fulltext_dict_root_prefix(db_index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let split = rest.iter().position(|byte| *byte == 0x00)?;
    String::from_utf8(rest[split + 1..].to_vec()).ok()
}

fn fulltext_suggest_prefix(db_index: u16, key_name: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0sug\0");
    key.extend_from_slice(key_name.as_bytes());
    key.push(0x00);
    key
}

fn fulltext_suggest_key(db_index: u16, key_name: &str, string: &str) -> Vec<u8> {
    let mut key = fulltext_suggest_prefix(db_index, key_name);
    key.extend_from_slice(string.as_bytes());
    key
}

fn fulltext_suggest_string_from_key(db_index: u16, key_name: &str, key: &[u8]) -> Option<String> {
    let prefix = fulltext_suggest_prefix(db_index, key_name);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

fn fulltext_syn_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(b"\0syn\0");
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

fn fulltext_syn_key(db_index: u16, index: &str, group: &str) -> Vec<u8> {
    let mut key = fulltext_syn_prefix(db_index, index);
    key.extend_from_slice(group.as_bytes());
    key
}

fn fulltext_syn_group_from_key(db_index: u16, index: &str, key: &[u8]) -> Option<String> {
    let prefix = fulltext_syn_prefix(db_index, index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    String::from_utf8(rest.to_vec()).ok()
}

fn fulltext_file_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = db_prefix(db_index).to_vec();
    key.extend_from_slice(&FULLTEXT_FILE_NAMESPACE);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

fn fulltext_meta_key(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = fulltext_meta_prefix(db_index);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key.extend_from_slice(&0u64.to_be_bytes());
    key.extend_from_slice(b"meta");
    key
}

fn fulltext_index_from_meta_key(db_index: u16, key: &[u8]) -> Option<String> {
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

fn fulltext_outbox_prefix(db_index: u16, index: &str) -> Vec<u8> {
    let mut key = db_prefix(db_index).to_vec();
    key.extend_from_slice(&FULLTEXT_OUTBOX_NAMESPACE);
    key.extend_from_slice(index.as_bytes());
    key.push(0x00);
    key
}

fn fulltext_outbox_key(db_index: u16, index: &str, seq: u64) -> Vec<u8> {
    let mut key = fulltext_outbox_prefix(db_index, index);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn fulltext_outbox_seq_from_key(db_index: u16, index: &str, key: &[u8]) -> Option<u64> {
    let prefix = fulltext_outbox_prefix(db_index, index);
    let rest = key.strip_prefix(prefix.as_slice())?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

fn new_fulltext_sequence() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let millis = current_fulltext_millis();
    (millis << 16) | (COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xFFFF)
}

fn current_fulltext_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn delete_prefix_to_batch(
    batch: &mut WriteBatch,
    store: &crate::store::kv_store::KvStore,
    prefix: &[u8],
) {
    if let Some(end) = prefix_exclusive_upper_bound(prefix) {
        batch.delete_range(prefix, &end);
    } else {
        for (key, _) in store.scan_prefix_raw(prefix) {
            batch.delete(&key);
        }
    }
}

fn fulltext_supported_config_names() -> Vec<&'static str> {
    let defaults = fulltext_default_config();
    defaults.keys().copied().collect()
}

fn fulltext_default_config_value(name: &str) -> Option<&'static str> {
    fulltext_default_config().get(name).copied().or_else(|| {
        fulltext_default_config()
            .get(&name.to_ascii_uppercase().as_str())
            .copied()
    })
}

fn fulltext_default_config() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("CLUSTER_ALIAS_PROPAGATION", "local"),
        ("CLUSTER_CONFIG_PROPAGATION", "local"),
        ("CLUSTER_ENABLED", "false"),
        ("CLUSTER_ROUTING", "local"),
        ("CLUSTER_SHARD_ID", "0"),
        ("CLUSTER_SHARDS", "1"),
        ("CLUSTER_VECTOR_MERGE", "local"),
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
        ("MINPREFIX", "2"),
        ("NOGC", "false"),
        ("ON_TIMEOUT", "RETURN"),
        ("OUTBOX_COMPACT_THRESHOLD", "1024"),
        ("REFRESH_INTERVAL_MS", "100"),
        ("REFRESH_MAX_BYTES", "4194304"),
        ("REFRESH_MAX_DOCS", "1024"),
        ("REFRESH_TIMEOUT_MS", "500"),
        ("REPAIR_THROTTLE_MS", "1000"),
        ("TIMEOUT", "500"),
    ])
}

fn validate_fulltext_config_name(name: &str) -> Result<(), Error> {
    if fulltext_default_config().contains_key(name) {
        Ok(())
    } else {
        Err(Error::msg("ERR unsupported fulltext config option"))
    }
}

fn validate_fulltext_config_value(name: &str, value: &str) -> Result<(), Error> {
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
        "MAXSEARCHRESULTS"
        | "MAXAGGREGATERESULTS"
        | "MAXEXPANSIONS"
        | "MAXPREFIXEXPANSIONS"
        | "CLUSTER_SHARD_ID"
        | "REFRESH_MAX_DOCS"
        | "REFRESH_MAX_BYTES"
        | "REFRESH_INTERVAL_MS"
        | "REFRESH_TIMEOUT_MS"
        | "OUTBOX_COMPACT_THRESHOLD"
        | "REPAIR_THROTTLE_MS"
        | "MEMORY_BUDGET_READER_BYTES"
        | "MEMORY_BUDGET_WRITER_BYTES"
        | "MEMORY_BUDGET_SORT_BYTES"
        | "MEMORY_BUDGET_AGGREGATE_CURSOR_BYTES"
        | "MEMORY_BUDGET_VECTOR_HEAP_BYTES"
        | "TIMEOUT" => {
            value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            Ok(())
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
        _ => Err(Error::msg("ERR unsupported fulltext config option")),
    }
}

