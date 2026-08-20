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

struct VectorResultContext<'a> {
    meta: &'a VectorIndexMeta,
    query: &'a [f32],
    query_norm_squared: f64,
    return_attrs: &'a [String],
    return_attrs_json: bool,
    filters: &'a [FilterPredicate],
}

fn runtime_doc_to_search_result(
    id: &str,
    doc: &VectorDocRecord,
    candidate_score: Option<f32>,
    context: &VectorResultContext<'_>,
) -> Result<Option<VectorSearchResult>, Error> {
    let attrs = if context.filters.is_empty() && context.return_attrs.is_empty() {
        None
    } else {
        let attrs = parse_attrs(&doc.attrs_json)?;
        if !matches_filters(&attrs, context.filters) {
            return Ok(None);
        }
        Some(attrs)
    };
    let attrs_json = context.return_attrs_json.then(|| doc.attrs_json.clone());
    Ok(Some(VectorSearchResult {
        id: id.to_string(),
        score: candidate_score
            .map(Ok)
            .unwrap_or_else(|| {
                distance_score_prepared(
                    context.meta.distance,
                    context.query,
                    context.query_norm_squared,
                    &doc.vector,
                )
            })?,
        attrs: attrs
            .as_ref()
            .map(|attrs| collect_return_attrs(attrs, context.return_attrs))
            .unwrap_or_default(),
        attrs_json,
    }))
}

fn distance_score(distance: VectorDistance, lhs: &[f32], rhs: &[f32]) -> Result<f32, Error> {
    distance_score_prepared(distance, lhs, vector_norm_squared(lhs), rhs)
}

fn distance_score_prepared(
    distance: VectorDistance,
    lhs: &[f32],
    lhs_norm_squared: f64,
    rhs: &[f32],
) -> Result<f32, Error> {
    let score = match distance {
        VectorDistance::L2 => lhs
            .iter()
            .zip(rhs)
            .map(|(a, b)| {
                let delta = f64::from(*a) - f64::from(*b);
                delta * delta
            })
            .sum::<f64>(),
        VectorDistance::Ip => -lhs
            .iter()
            .zip(rhs)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>(),
        VectorDistance::Cosine => {
            let dot = lhs
                .iter()
                .zip(rhs)
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum::<f64>();
            let lhs_norm = lhs_norm_squared.sqrt();
            let rhs_norm = rhs
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                .sqrt();
            if lhs_norm == 0.0 || rhs_norm == 0.0 {
                return Err(Error::msg("ERR zero norm vector for cosine distance"));
            }
            1.0 - (dot / (lhs_norm * rhs_norm)).clamp(-1.0, 1.0)
        }
    };
    if !score.is_finite() || score < -f64::from(f32::MAX) || score > f64::from(f32::MAX) {
        return Err(Error::msg("ERR vector distance overflow"));
    }
    Ok(score as f32)
}

fn sort_and_limit_results(results: &mut Vec<VectorSearchResult>, k: usize) {
    results.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.id.cmp(&right.id))
    });
    results.truncate(k);
}

struct RankedVectorSearchResult(VectorSearchResult);

impl PartialEq for RankedVectorSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.0.score.to_bits() == other.0.score.to_bits() && self.0.id == other.0.id
    }
}

impl Eq for RankedVectorSearchResult {}

impl PartialOrd for RankedVectorSearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedVectorSearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .score
            .total_cmp(&other.0.score)
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

struct TopKVectorResults {
    heap: BinaryHeap<RankedVectorSearchResult>,
    k: usize,
    memory_budget: usize,
    memory_used: usize,
}

impl TopKVectorResults {
    fn new(k: usize, memory_budget: usize) -> Result<Self, Error> {
        if k.saturating_mul(std::mem::size_of::<VectorSearchResult>() + 32) > memory_budget {
            return Err(Error::msg("ERR vector search memory budget exceeded"));
        }
        Ok(Self {
            heap: BinaryHeap::with_capacity(k.min(4096)),
            k,
            memory_budget,
            memory_used: 0,
        })
    }

    fn push(&mut self, result: VectorSearchResult) -> Result<(), Error> {
        if self.k == 0 {
            return Ok(());
        }
        let ranked = RankedVectorSearchResult(result);
        if self.heap.len() >= self.k && self.heap.peek().is_some_and(|worst| ranked >= *worst) {
            return Ok(());
        }
        let result_bytes = estimated_vector_result_bytes(&ranked.0);
        if self.heap.len() >= self.k
            && let Some(removed) = self.heap.pop()
        {
            self.memory_used = self
                .memory_used
                .saturating_sub(estimated_vector_result_bytes(&removed.0));
        }
        if self.memory_used.saturating_add(result_bytes) > self.memory_budget {
            return Err(Error::msg("ERR vector search memory budget exceeded"));
        }
        self.memory_used = self.memory_used.saturating_add(result_bytes);
        self.heap.push(ranked);
        Ok(())
    }

    fn into_sorted(self) -> Vec<VectorSearchResult> {
        let mut results = self
            .heap
            .into_iter()
            .map(|ranked| ranked.0)
            .collect::<Vec<_>>();
        sort_and_limit_results(&mut results, self.k);
        results
    }
}

fn estimated_vector_result_bytes(result: &VectorSearchResult) -> usize {
    result
        .id
        .len()
        .saturating_add(result.attrs.iter().fold(0usize, |size, (field, value)| {
            size.saturating_add(field.len())
                .saturating_add(value.len())
                .saturating_add(2 * std::mem::size_of::<String>())
        }))
        .saturating_add(result.attrs_json.as_ref().map_or(0, String::len))
        .saturating_add(std::mem::size_of::<VectorSearchResult>() + 32)
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
    candidates: Vec<VectorCandidate>,
    limit: usize,
) -> Result<Vec<VectorCandidate>, Error> {
    let candidates_before_dedup = candidates.len();
    let mut latest_by_id = HashMap::<String, (u64, f32)>::new();
    for candidate in candidates {
        match latest_by_id.entry(candidate.id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                if candidate.doc_version > current.0
                    || (candidate.doc_version == current.0 && candidate.distance < current.1)
                {
                    *current = (candidate.doc_version, candidate.distance);
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((candidate.doc_version, candidate.distance));
            }
        }
    }
    let mut candidates = latest_by_id
        .into_iter()
        .map(|(id, (doc_version, distance))| VectorCandidate {
            id,
            doc_version,
            distance,
            source_position: None,
        })
        .collect::<Vec<_>>();
    global_metrics().record_vector_candidate_dedup(candidates_before_dedup, candidates.len());
    let compare = |left: &VectorCandidate, right: &VectorCandidate| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.id.cmp(&right.id))
    };
    if candidates.len() > limit {
        candidates.select_nth_unstable_by(limit, compare);
        candidates.truncate(limit);
    }
    candidates.sort_by(compare);
    Ok(candidates)
}
