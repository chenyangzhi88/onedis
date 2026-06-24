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
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VECTOR_SEGMENT_MAX_DOCS)
}

fn normalize_hnsw_m(value: Option<usize>) -> Result<usize, Error> {
    let m = value.unwrap_or(DEFAULT_HNSW_M as usize);
    if m == 0 || m > 256 {
        return Err(Error::msg("ERR invalid vector HNSW M"));
    }
    Ok(m)
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
    if distance == VectorDistance::Cosine
        && vector.iter().map(|value| value * value).sum::<f32>() == 0.0
    {
        return Err(Error::msg("ERR zero norm vector for cosine distance"));
    }
    Ok(())
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
