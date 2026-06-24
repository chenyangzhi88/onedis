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
