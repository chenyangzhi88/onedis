fn encode_record<T: Encode>(value: &T) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|_| Error::msg("ERR failed to encode vector record"))
}

fn decode_record<T: Decode<()>>(raw: &[u8]) -> Result<T, Error> {
    bincode::decode_from_slice::<T, _>(raw, bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(|_| Error::msg("ERR failed to decode vector record"))
}

fn put_vector_marker_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    expire_ms: u64,
    version: u64,
    dim: u32,
) {
    let marker = Structure::VectorCollection(Vector {
        dimension: dim as usize,
        vectors: Default::default(),
        norms: Default::default(),
    });
    batch.put(
        &main_key(db_index, index),
        &encode_entry(&marker, expire_ms, version),
    );
}

fn persist_vector_segment_snapshot(
    store: &crate::store::kv_store::KvStore,
    db_index: u16,
    index: &str,
    version: u64,
    segment: &VectorSegmentMeta,
    snapshot_raw: &[u8],
) -> Result<(), Error> {
    store.blob_put_raw(&segment.graph_key, snapshot_raw);
    let meta_key = vector_meta_key(db_index, index, version);
    let Some(meta_raw) = store.get_raw(&meta_key) else {
        return Err(Error::msg("ERR vector index metadata missing"));
    };
    let mut meta = decode_record::<VectorIndexMeta>(&meta_raw)?;
    meta.next_segment_id = meta
        .next_segment_id
        .max(segment.segment_id.saturating_add(1));
    meta.snapshot_doc_version = meta.snapshot_doc_version.max(segment.max_doc_version);
    let mut batch = WriteBatch::new();
    batch.put(
        &vector_segment_key(db_index, index, version, segment.segment_id),
        &encode_record(segment)?,
    );
    batch.put(&meta_key, &encode_record(&meta)?);
    store.write_batch(&batch);
    Ok(())
}

fn delete_vector_namespace_to_batch(
    store: &crate::store::kv_store::KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
) {
    for namespace in [
        VECTOR_META_NAMESPACE,
        VECTOR_DOC_NAMESPACE,
        VECTOR_TAG_NAMESPACE,
        VECTOR_NUMERIC_NAMESPACE,
        VECTOR_SEGMENT_NAMESPACE,
        VECTOR_GRAPH_NAMESPACE,
    ] {
        let prefix = vector_prefix(db_index, &namespace, index, version);
        for (key, _) in store.scan_prefix_raw(&prefix) {
            batch.delete(&key);
        }
    }
}

fn delete_vector_segments_to_batch(
    store: &crate::store::kv_store::KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
) {
    for namespace in [VECTOR_SEGMENT_NAMESPACE, VECTOR_GRAPH_NAMESPACE] {
        let prefix = vector_prefix(db_index, &namespace, index, version);
        for (key, _) in store.scan_prefix_raw(&prefix) {
            batch.delete(&key);
        }
    }
}

fn vector_prefix(db_index: u16, ns: &[u8; 3], index: &str, version: u64) -> Vec<u8> {
    sub_key_range_start_bytes(db_index, ns, index.as_bytes(), version)
}

fn vector_meta_key(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_META_NAMESPACE, index, version);
    key.extend_from_slice(b"meta");
    key
}

fn vector_doc_prefix(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    vector_prefix(db_index, &VECTOR_DOC_NAMESPACE, index, version)
}

fn vector_doc_key(db_index: u16, index: &str, version: u64, id: &str) -> Vec<u8> {
    let mut key = vector_doc_prefix(db_index, index, version);
    key.extend_from_slice(id.as_bytes());
    key
}

fn vector_segment_prefix(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    vector_prefix(db_index, &VECTOR_SEGMENT_NAMESPACE, index, version)
}

fn vector_segment_key(db_index: u16, index: &str, version: u64, segment_id: u64) -> Vec<u8> {
    let mut key = vector_segment_prefix(db_index, index, version);
    key.extend_from_slice(&segment_id.to_be_bytes());
    key
}

fn vector_graph_key(db_index: u16, index: &str, version: u64, segment_id: u64) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_GRAPH_NAMESPACE, index, version);
    key.extend_from_slice(&segment_id.to_be_bytes());
    key
}

fn vector_tag_key(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: &str,
    doc_id: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_TAG_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    append_len_prefixed(&mut key, value.as_bytes());
    key.extend_from_slice(doc_id.as_bytes());
    key
}

fn vector_tag_prefix(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_TAG_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    append_len_prefixed(&mut key, value.as_bytes());
    key
}

fn vector_numeric_key(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: f64,
    doc_id: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_NUMERIC_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    key.extend_from_slice(&sortable_f64(value).to_be_bytes());
    key.extend_from_slice(doc_id.as_bytes());
    key
}

fn vector_numeric_field_prefix(db_index: u16, index: &str, version: u64, field: &str) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_NUMERIC_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    key
}

fn append_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn sortable_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn unsortable_f64(value: u64) -> f64 {
    let bits = if value & (1 << 63) != 0 {
        value ^ (1 << 63)
    } else {
        !value
    };
    f64::from_bits(bits)
}

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

fn indexed_filter_field<'a>(
    meta: &'a VectorIndexMeta,
    predicate: &FilterPredicate,
) -> Option<&'a str> {
    let (field_name, expected_kind) = match predicate {
        FilterPredicate::TagEq(field, _) | FilterPredicate::TagIn(field, _) => {
            (field.as_str(), VectorFieldKind::Tag)
        }
        FilterPredicate::NumericCmp(field, _, _) => (field.as_str(), VectorFieldKind::Numeric),
    };
    meta.schema
        .iter()
        .find(|field| field.indexed && field.name == field_name && field.kind == expected_kind)
        .map(|field| field.name.as_str())
}

fn put_attr_index_entries_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
    schema: &[VectorFieldSchema],
    doc_id: &str,
    doc_version: u64,
    attrs: &JsonValue,
) -> Result<(), Error> {
    for field in schema.iter().filter(|field| field.indexed) {
        let Some(value) = attrs.get(&field.name) else {
            continue;
        };
        match field.kind {
            VectorFieldKind::Tag => {
                for tag in tag_values(value)? {
                    batch.put(
                        &vector_tag_key(db_index, index, version, &field.name, &tag, doc_id),
                        &doc_version.to_be_bytes(),
                    );
                }
            }
            VectorFieldKind::Numeric => {
                if let Some(number) = value.as_f64() {
                    batch.put(
                        &vector_numeric_key(db_index, index, version, &field.name, number, doc_id),
                        &doc_version.to_be_bytes(),
                    );
                }
            }
            VectorFieldKind::Text => {}
        }
    }
    Ok(())
}

fn delete_attr_index_entries_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
    schema: &[VectorFieldSchema],
    doc_id: &str,
    doc_version: u64,
    attrs: &JsonValue,
) {
    for field in schema.iter().filter(|field| field.indexed) {
        let Some(value) = attrs.get(&field.name) else {
            continue;
        };
        match field.kind {
            VectorFieldKind::Tag => {
                if let Ok(tags) = tag_values(value) {
                    for tag in tags {
                        batch.delete(&vector_tag_key(
                            db_index,
                            index,
                            version,
                            &field.name,
                            &tag,
                            doc_id,
                        ));
                    }
                }
            }
            VectorFieldKind::Numeric => {
                if let Some(number) = value.as_f64() {
                    batch.delete(&vector_numeric_key(
                        db_index,
                        index,
                        version,
                        &field.name,
                        number,
                        doc_id,
                    ));
                }
            }
            VectorFieldKind::Text => {}
        }
    }
    let _ = doc_version;
}

fn tag_values(value: &JsonValue) -> Result<Vec<String>, Error> {
    if let Some(text) = value.as_str() {
        return Ok(vec![text.to_string()]);
    }
    if let Some(values) = value.as_array() {
        let mut tags = Vec::with_capacity(values.len());
        for value in values {
            let Some(text) = value.as_str() else {
                return Err(Error::msg("ERR vector tag array must contain strings"));
            };
            tags.push(text.to_string());
        }
        return Ok(tags);
    }
    Err(Error::msg("ERR vector tag field must be string or array"))
}

fn parse_filter(filter: &str) -> Result<Vec<FilterPredicate>, Error> {
    let mut predicates = Vec::new();
    let normalized = filter.replace("&&", " AND ");
    for part in normalized.split(" AND ") {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let before_len = predicates.len();
        if let Some((field, values)) = parse_in_predicate(part)? {
            predicates.push(FilterPredicate::TagIn(field, values));
            continue;
        }
        if let Some((field, value)) = split_binary(part, "==") {
            predicates.push(FilterPredicate::TagEq(
                normalize_filter_field(field),
                trim_filter_string(value.trim()),
            ));
            continue;
        }
        for (op_text, op) in [
            (">=", NumericOp::Ge),
            ("<=", NumericOp::Le),
            (">", NumericOp::Gt),
            ("<", NumericOp::Lt),
        ] {
            if let Some((field, value)) = split_binary(part, op_text) {
                let value = value
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| Error::msg("ERR invalid vector numeric filter"))?;
                if !value.is_finite() {
                    return Err(Error::msg("ERR invalid vector numeric filter"));
                }
                predicates.push(FilterPredicate::NumericCmp(
                    normalize_filter_field(field),
                    op,
                    value,
                ));
                break;
            }
        }
        if predicates.len() == before_len {
            return Err(Error::msg("ERR unsupported vector filter"));
        }
    }
    Ok(predicates)
}

fn split_binary<'a>(input: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    input.split_once(op)
}

fn parse_in_predicate(part: &str) -> Result<Option<(String, Vec<String>)>, Error> {
    let Some((field, values)) = part.split_once(" IN ") else {
        return Ok(None);
    };
    let values = values.trim();
    if !values.starts_with('(') || !values.ends_with(')') {
        return Err(Error::msg("ERR invalid vector IN filter"));
    }
    let values = values[1..values.len() - 1]
        .split(',')
        .map(|value| trim_filter_string(value.trim()))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(Error::msg("ERR invalid vector IN filter"));
    }
    Ok(Some((normalize_filter_field(field), values)))
}

fn normalize_filter_field(field: &str) -> String {
    field.trim().trim_start_matches('.').to_string()
}

fn trim_filter_string(value: &str) -> String {
    value.trim_matches('"').trim_matches('\'').to_string()
}

fn matches_filters(attrs: &JsonValue, predicates: &[FilterPredicate]) -> bool {
    predicates.iter().all(|predicate| match predicate {
        FilterPredicate::TagEq(field, expected) => attr_tag_matches(attrs.get(field), expected),
        FilterPredicate::TagIn(field, expected) => expected
            .iter()
            .any(|expected| attr_tag_matches(attrs.get(field), expected)),
        FilterPredicate::NumericCmp(field, op, expected) => attrs
            .get(field)
            .and_then(JsonValue::as_f64)
            .is_some_and(|actual| match op {
                NumericOp::Gt => actual > *expected,
                NumericOp::Ge => actual >= *expected,
                NumericOp::Lt => actual < *expected,
                NumericOp::Le => actual <= *expected,
            }),
    })
}

fn attr_tag_matches(value: Option<&JsonValue>, expected: &str) -> bool {
    let Some(value) = value else {
        return false;
    };
    if let Some(text) = value.as_str() {
        return text == expected;
    }
    value
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}

fn collect_return_attrs(attrs: &JsonValue, fields: &[String]) -> Vec<(String, String)> {
    fields
        .iter()
        .filter_map(|field| {
            attrs
                .get(field)
                .map(|value| (field.clone(), json_attr_to_string(value)))
        })
        .collect()
}

fn json_attr_to_string(value: &JsonValue) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn doc_to_search_result(
    raw: Vec<u8>,
    meta: &VectorIndexMeta,
    query: &[f32],
    return_attrs: &[String],
    filters: &[FilterPredicate],
    expected_doc_version: Option<u64>,
) -> Result<Option<VectorSearchResult>, Error> {
    let doc = decode_record::<VectorDocRecord>(&raw)?;
    if expected_doc_version.is_some_and(|version| version != doc.doc_version) {
        return Ok(None);
    }
    if doc.deleted {
        return Ok(None);
    }
    let attrs = parse_attrs(&doc.attrs_json)?;
    if !matches_filters(&attrs, filters) {
        return Ok(None);
    }
    let score = distance_score(meta.distance, query, &doc.vector)?;
    let attrs = collect_return_attrs(&attrs, return_attrs);
    Ok(Some(VectorSearchResult {
        id: doc.id,
        score,
        attrs,
    }))
}

fn distance_score(distance: VectorDistance, lhs: &[f32], rhs: &[f32]) -> Result<f32, Error> {
    match distance {
        VectorDistance::L2 => Ok(lhs
            .iter()
            .zip(rhs)
            .map(|(a, b)| {
                let delta = a - b;
                delta * delta
            })
            .sum()),
        VectorDistance::Ip => Ok(-lhs.iter().zip(rhs).map(|(a, b)| a * b).sum::<f32>()),
        VectorDistance::Cosine => {
            let dot = lhs.iter().zip(rhs).map(|(a, b)| a * b).sum::<f32>();
            let lhs_norm = lhs.iter().map(|value| value * value).sum::<f32>().sqrt();
            let rhs_norm = rhs.iter().map(|value| value * value).sum::<f32>().sqrt();
            if lhs_norm == 0.0 || rhs_norm == 0.0 {
                return Err(Error::msg("ERR zero norm vector for cosine distance"));
            }
            Ok(1.0 - dot / (lhs_norm * rhs_norm))
        }
    }
}

fn sort_and_limit_results(results: &mut Vec<VectorSearchResult>, k: usize) {
    results.sort_by(|left, right| {
        left.score
            .partial_cmp(&right.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
    results.truncate(k);
}

fn window_results(
    results: Vec<VectorSearchResult>,
    options: &VectorSearchOptions,
) -> Vec<VectorSearchResult> {
    let offset = options.offset.min(results.len());
    let count = options.limit.unwrap_or(options.k);
    results.into_iter().skip(offset).take(count).collect()
}

fn reduce_vector_candidates(
    mut candidates: Vec<VectorCandidate>,
    limit: usize,
) -> Result<Vec<VectorCandidate>, Error> {
    candidates.sort_by(|left, right| {
        left.distance
            .partial_cmp(&right.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left.doc_version.cmp(&right.doc_version))
    });
    let mut seen = HashSet::new();
    let mut reduced = Vec::with_capacity(limit.min(candidates.len()));
    for candidate in candidates {
        let key = VectorCandidateKey {
            id: candidate.id.clone(),
            doc_version: candidate.doc_version,
        };
        if !seen.insert(key) {
            continue;
        }
        reduced.push(candidate);
        if reduced.len() >= limit {
            break;
        }
    }
    Ok(reduced)
}

