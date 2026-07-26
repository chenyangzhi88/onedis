fn parse_distance(distance: &str) -> Result<VectorDistance, Error> {
    match distance.to_ascii_uppercase().as_str() {
        "COSINE" => Ok(VectorDistance::Cosine),
        "L2" => Ok(VectorDistance::L2),
        "IP" => Ok(VectorDistance::Ip),
        _ => Err(Error::msg("ERR unsupported vector distance")),
    }
}

fn distance_name(distance: VectorDistance) -> &'static str {
    match distance {
        VectorDistance::Cosine => "COSINE",
        VectorDistance::L2 => "L2",
        VectorDistance::Ip => "IP",
    }
}

fn vector_segment_max_docs() -> u64 {
    std::env::var("ONEDIS_VECTOR_SEGMENT_MAX_DOCS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0 && *value <= MAX_VECTOR_INITIAL_CAP as u64)
        .unwrap_or(DEFAULT_VECTOR_SEGMENT_MAX_DOCS)
}

fn normalize_hnsw_m(value: Option<usize>) -> Result<usize, Error> {
    let m = value.unwrap_or(DEFAULT_HNSW_M as usize);
    if m == 0 || m > 256 {
        return Err(Error::msg("ERR invalid vector HNSW M"));
    }
    Ok(m)
}

fn validate_vector_meta_config(meta: &VectorIndexMeta) -> Result<(), Error> {
    if meta.dim == 0 || meta.dim as usize > MAX_VECTOR_DIMENSIONS {
        return Err(Error::msg("ERR invalid persisted vector dimension"));
    }
    if meta.m == 0 || meta.m > 256 {
        return Err(Error::msg("ERR invalid persisted vector HNSW M"));
    }
    if meta.ef_construction < meta.m || meta.ef_construction as usize > MAX_VECTOR_HNSW_EF {
        return Err(Error::msg(
            "ERR invalid persisted vector EF_CONSTRUCTION",
        ));
    }
    if meta.ef_runtime == 0 || meta.ef_runtime as usize > MAX_VECTOR_HNSW_EF {
        return Err(Error::msg("ERR invalid persisted vector EF_RUNTIME"));
    }
    if meta.initial_cap == 0 || meta.initial_cap > MAX_VECTOR_INITIAL_CAP as u64 {
        return Err(Error::msg("ERR invalid persisted vector INITIAL_CAP"));
    }
    if meta.segment_max_docs == 0 || meta.segment_max_docs > MAX_VECTOR_INITIAL_CAP as u64 {
        return Err(Error::msg("ERR invalid persisted vector segment size"));
    }
    validate_schema(&meta.schema)
}

fn validate_schema(schema: &[VectorFieldSchema]) -> Result<(), Error> {
    let mut seen = std::collections::HashSet::new();
    for field in schema {
        if field.name.is_empty() || !seen.insert(field.name.clone()) {
            return Err(Error::msg("ERR invalid vector schema"));
        }
    }
    Ok(())
}

fn validate_vector(vector: &[f32], dim: usize) -> Result<(), Error> {
    if vector.len() != dim {
        return Err(Error::msg("ERR vector dimension mismatch"));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(Error::msg("ERR vector contains NaN or Inf"));
    }
    Ok(())
}

fn validate_vector_for_distance(vector: &[f32], distance: VectorDistance) -> Result<(), Error> {
    let norm_squared = vector
        .iter()
        .map(|value| {
            let value = f64::from(*value);
            value * value
        })
        .sum::<f64>();
    match distance {
        VectorDistance::Cosine if norm_squared == 0.0 => {
            return Err(Error::msg("ERR zero norm vector for cosine distance"));
        }
        VectorDistance::L2 if norm_squared > f64::from(f32::MAX) / 4.0 => {
            return Err(Error::msg("ERR vector magnitude is too large for L2 distance"));
        }
        VectorDistance::Ip if norm_squared > f64::from(f32::MAX) => {
            return Err(Error::msg("ERR vector magnitude is too large for IP distance"));
        }
        _ => {}
    }
    Ok(())
}

fn vector_search_memory_budget_bytes() -> usize {
    std::env::var("ONEDIS_VECTOR_SEARCH_MEMORY_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VECTOR_SEARCH_MEMORY_BUDGET_BYTES)
}

fn vector_exact_scan_limit() -> usize {
    std::env::var("ONEDIS_VECTOR_EXACT_SCAN_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VECTOR_EXACT_SCAN_LIMIT)
}

fn parse_attrs(attrs_json: &str) -> Result<JsonValue, Error> {
    let value: JsonValue =
        serde_json::from_str(attrs_json).map_err(|_| Error::msg("ERR invalid vector attrs"))?;
    if !value.is_object() {
        return Err(Error::msg("ERR vector attrs must be a JSON object"));
    }
    Ok(value)
}

fn validate_attrs_against_schema(
    schema: &[VectorFieldSchema],
    attrs: &JsonValue,
) -> Result<(), Error> {
    for field in schema {
        let Some(value) = attrs.get(&field.name) else {
            continue;
        };
        match field.kind {
            VectorFieldKind::Tag => {
                if !value.is_string() && !value.is_array() {
                    return Err(Error::msg("ERR vector tag field must be string or array"));
                }
            }
            VectorFieldKind::Numeric => {
                if value.as_f64().is_none_or(|number| !number.is_finite()) {
                    return Err(Error::msg("ERR vector numeric field must be finite number"));
                }
            }
            VectorFieldKind::Text => {}
        }
    }
    Ok(())
}
