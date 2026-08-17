use super::*;

struct RankedFullTextVectorResult(VectorSearchResult);

impl PartialEq for RankedFullTextVectorResult {
    fn eq(&self, other: &Self) -> bool {
        self.0.score.to_bits() == other.0.score.to_bits() && self.0.id == other.0.id
    }
}

impl Eq for RankedFullTextVectorResult {}

impl PartialOrd for RankedFullTextVectorResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedFullTextVectorResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .score
            .total_cmp(&other.0.score)
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

impl Db {
    pub(super) fn fulltext_vector_hits(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        runtime: &Arc<RwLock<FullTextRuntime>>,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        limits: FullTextSearchLimits,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let plan = fulltext_vector_plan(ast)?;
        if matches!(plan.kind, FullTextVectorPlanKind::Knn { k } if k > limits.result_cap) {
            return Err(Error::msg("ERR fulltext result limit exceeded"));
        }
        let vector_field = fulltext_vector_schema_field(meta, &plan.field)?;
        let query_vector =
            parse_fulltext_vector_param_for_field(&options.params, &plan.blob_param, vector_field)?;
        let vector_index =
            fulltext_vector_index_name(index, meta.generation, vector_field.attribute_name());
        let vector_budget =
            self.fulltext_config_usize("MEMORY_BUDGET_VECTOR_HEAP_BYTES", 16_777_216)?;
        let mut allow = if plan.filter.is_some()
            || !options.filters.is_empty()
            || !options.geo_filters.is_empty()
        {
            let scalar_filter = plan.filter.clone().unwrap_or(FullTextQueryAst::All);
            let hits = if options.geo_filters.is_empty() {
                runtime
                    .read()
                    .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                    .search_ast(
                        &scalar_filter,
                        options,
                        limits.result_cap.saturating_add(1),
                        limits.timeout,
                    )?
                    .into_iter()
                    .map(|hit| hit.key)
                    .collect::<Vec<_>>()
            } else {
                self.fulltext_exact_filter_hits(meta, &scalar_filter, options, limits)?
                    .into_iter()
                    .map(|hit| hit.key)
                    .collect::<Vec<_>>()
            };
            if hits.len() > limits.result_cap {
                return Err(Error::msg("ERR fulltext result limit exceeded"));
            }
            Some(hits.into_iter().collect::<HashSet<_>>())
        } else {
            None
        };
        if let Some(in_keys) = options.in_keys.as_ref() {
            allow = Some(match allow {
                Some(mut filtered) => {
                    filtered.retain(|key| in_keys.contains(key));
                    filtered
                }
                None => in_keys.clone(),
            });
        }
        let allow_bytes = allow.as_ref().map_or(0, |allow| {
            allow.iter().fold(0usize, |used, key| {
                used.saturating_add(
                    std::mem::size_of::<String>()
                        .saturating_add(key.len())
                        .saturating_add(2 * std::mem::size_of::<usize>()),
                )
            })
        });
        if allow_bytes > limits.reader_budget {
            return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
        }
        let allow = allow.map(Arc::new);
        let vector_results = if matches!(plan.kind, FullTextVectorPlanKind::Range { .. })
            || matches!(
                vector_field
                    .options
                    .vector
                    .as_ref()
                    .map(|options| options.algorithm),
                Some(FullTextVectorAlgorithm::Flat)
            ) {
            let (limit, max_score) = match plan.kind {
                FullTextVectorPlanKind::Knn { k } => (Some(k), None),
                FullTextVectorPlanKind::Range { radius } => {
                    (None, Some(radius * (1.0 + options.vector_epsilon)))
                }
            };
            self.fulltext_vector_exact_results(
                &vector_index,
                vector_field,
                &query_vector,
                allow.as_deref(),
                limit,
                max_score,
                limits.timeout.at,
                limits.timeout.fail_on_timeout,
            )?
        } else {
            let vector_limit = match plan.kind {
                FullTextVectorPlanKind::Knn { k } => k,
                FullTextVectorPlanKind::Range { .. } => self.vector_card(&vector_index)? as usize,
            }
            .max(1);
            let max_vector_results = vector_budget
                .checked_div(std::mem::size_of::<VectorSearchResult>().max(1))
                .unwrap_or(0);
            if vector_limit > max_vector_results {
                return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
            }
            let vector_card = self.vector_card(&vector_index)? as usize;
            let mut request_limit = vector_limit.min(vector_card.max(1));
            loop {
                let search_options = VectorSearchOptions {
                    k: request_limit,
                    filter: None,
                    with_scores: true,
                    with_attrs: Vec::new(),
                    with_attrs_json: false,
                    ef: options.vector_ef_runtime,
                    filter_ef: options.vector_filter_ef,
                    exact: false,
                    offset: 0,
                    limit: None,
                };
                let results = if let Some(allow) = allow.as_ref() {
                    self.vector_search_with_allow_ids(
                        &vector_index,
                        &query_vector,
                        search_options,
                        Arc::clone(allow),
                    )?
                } else {
                    self.vector_search(&vector_index, &query_vector, search_options)?
                };
                let expected_type = match meta.source_type {
                    FullTextSourceType::Hash => TYPE_HASH,
                    FullTextSourceType::Json => TYPE_JSON,
                };
                let now = current_fulltext_millis();
                let live_results = self
                    .store
                    .multi_get_raw(
                        &results
                            .iter()
                            .map(|result| self.mk(&result.id))
                            .collect::<Vec<_>>(),
                    )
                    .into_iter()
                    .filter(|raw| {
                        raw.as_deref()
                            .and_then(decode_meta_header)
                            .is_some_and(|header| {
                                header.type_tag == expected_type
                                    && (header.expire_ms == 0 || header.expire_ms > now)
                            })
                    })
                    .count();
                if live_results >= vector_limit
                    || request_limit >= vector_card
                    || results.len() < request_limit
                {
                    break results;
                }
                let next_limit = request_limit
                    .saturating_mul(2)
                    .max(request_limit.saturating_add(1))
                    .min(vector_card)
                    .min(max_vector_results);
                if next_limit == request_limit {
                    return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
                }
                request_limit = next_limit;
            }
        };
        let vector_bytes = vector_results.iter().fold(0usize, |used, result| {
            used.saturating_add(
                std::mem::size_of::<VectorSearchResult>().saturating_add(result.id.len()),
            )
        });
        if vector_bytes > vector_budget {
            return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
        }
        let mut live = Vec::new();
        let mut live_bytes = 0usize;
        for result in vector_results {
            if fulltext_search_timeout_reached(limits.timeout.at, limits.timeout.fail_on_timeout)? {
                break;
            }
            if allow
                .as_ref()
                .is_some_and(|allow| !allow.contains(&result.id))
                || options
                    .in_keys
                    .as_ref()
                    .is_some_and(|keys| !keys.contains(&result.id))
            {
                continue;
            }
            if matches!(plan.kind, FullTextVectorPlanKind::Range { radius }
                if result.score > radius * (1.0 + options.vector_epsilon))
            {
                continue;
            }
            if let Some(mut hit) =
                self.fulltext_live_hit_from_source(meta, options, result.id, result.score)?
            {
                if live.len() >= limits.result_cap {
                    return Err(Error::msg("ERR fulltext result limit exceeded"));
                }
                let score = format_fulltext_score(result.score);
                hit.fields
                    .push(("__vector_score".to_string(), score.clone()));
                hit.fields
                    .push((format!("__{}_score", vector_field.attribute_name()), score));
                live_bytes = live_bytes.saturating_add(estimate_fulltext_live_hit_bytes(&hit));
                if live_bytes > limits.reader_budget {
                    return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
                }
                live.push(hit);
                if matches!(plan.kind, FullTextVectorPlanKind::Knn { k } if live.len() >= k) {
                    break;
                }
            }
        }
        Ok(live)
    }

    pub(super) fn fulltext_vector_exact_results(
        &self,
        vector_index: &str,
        vector_field: &FullTextFieldSchema,
        query: &[f32],
        allow_doc_ids: Option<&HashSet<String>>,
        limit: Option<usize>,
        max_score: Option<f32>,
        deadline: Instant,
        fail_on_timeout: bool,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let distance = fulltext_vector_attr(
            vector_field
                .options
                .vector
                .as_ref()
                .ok_or_else(|| Error::msg("ERR missing VECTOR options"))?,
            "DISTANCE_METRIC",
        )?;
        let mut results = Vec::new();
        let mut top_k = limit.map(|limit| BinaryHeap::with_capacity(limit.min(4096)));
        let vector_budget =
            self.fulltext_config_usize("MEMORY_BUDGET_VECTOR_HEAP_BYTES", 16_777_216)?;
        let query_norm_squared = query.iter().map(|value| value * value).sum::<f32>();
        let mut used = 0usize;
        self.visit_vector_elements(vector_index, |id, vector| {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                return Ok(false);
            }
            if allow_doc_ids.is_some_and(|allow| !allow.contains(&id)) {
                return Ok(true);
            }
            let score =
                fulltext_vector_distance_prepared(&distance, query, query_norm_squared, &vector)?;
            if max_score.is_some_and(|maximum| score > maximum) {
                return Ok(true);
            }
            let result = VectorSearchResult {
                id,
                score,
                attrs: Vec::new(),
                attrs_json: None,
            };
            let result_bytes =
                std::mem::size_of::<VectorSearchResult>().saturating_add(result.id.len());
            if let Some(heap) = top_k.as_mut() {
                if limit == Some(0) {
                    return Ok(false);
                }
                let ranked = RankedFullTextVectorResult(result);
                if heap.len() >= limit.unwrap_or(0)
                    && heap.peek().is_some_and(|worst| ranked >= *worst)
                {
                    return Ok(true);
                }
                if heap.len() >= limit.unwrap_or(0)
                    && let Some(removed) = heap.pop()
                {
                    used = used.saturating_sub(
                        std::mem::size_of::<VectorSearchResult>()
                            .saturating_add(removed.0.id.len()),
                    );
                }
                if used.saturating_add(result_bytes) > vector_budget {
                    return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
                }
                used = used.saturating_add(result_bytes);
                heap.push(ranked);
            } else {
                if used.saturating_add(result_bytes) > vector_budget {
                    return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
                }
                used = used.saturating_add(result_bytes);
                results.push(result);
            }
            Ok(true)
        })?;
        if let Some(heap) = top_k {
            results = heap.into_iter().map(|ranked| ranked.0).collect();
        }
        results.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(results)
    }
}
