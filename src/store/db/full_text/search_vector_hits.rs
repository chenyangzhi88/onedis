use super::*;
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
        let query_vector = parse_fulltext_vector_param(&options.params, &plan.blob_param)?;
        let vector_field = fulltext_vector_schema_field(meta, &plan.field)?;
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
                Some(filtered) => filtered.intersection(in_keys).cloned().collect(),
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
        let vector_results = if matches!(plan.kind, FullTextVectorPlanKind::Range { .. })
            || matches!(
                vector_field
                    .options
                    .vector
                    .as_ref()
                    .map(|options| options.algorithm),
                Some(FullTextVectorAlgorithm::Flat)
            ) {
            self.fulltext_vector_exact_results(
                &vector_index,
                vector_field,
                &query_vector,
                allow.as_ref(),
                limits.timeout.at,
                limits.timeout.fail_on_timeout,
            )?
        } else {
            let vector_limit = match plan.kind {
                FullTextVectorPlanKind::Knn { k } => {
                    k.max(options.offset.saturating_add(options.limit))
                }
                FullTextVectorPlanKind::Range { .. } => self.vector_card(&vector_index)? as usize,
            }
            .max(1);
            if vector_limit.saturating_mul(std::mem::size_of::<VectorSearchResult>())
                > vector_budget
            {
                return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
            }
            let search_options = VectorSearchOptions {
                k: vector_limit,
                filter: None,
                with_scores: true,
                with_attrs: Vec::new(),
                with_attrs_json: false,
                ef: None,
                filter_ef: None,
                exact: false,
                offset: 0,
                limit: None,
            };
            if let Some(allow) = allow.clone() {
                self.vector_search_with_allow_ids(
                    &vector_index,
                    &query_vector,
                    search_options,
                    allow,
                )?
            } else {
                self.vector_search(&vector_index, &query_vector, search_options)?
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
            if matches!(plan.kind, FullTextVectorPlanKind::Range { radius } if result.score > radius)
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
        let vector_budget =
            self.fulltext_config_usize("MEMORY_BUDGET_VECTOR_HEAP_BYTES", 16_777_216)?;
        let mut used = 0usize;
        self.visit_vector_elements(vector_index, |id, vector| {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                return Ok(false);
            }
            if allow_doc_ids.is_some_and(|allow| !allow.contains(&id)) {
                return Ok(true);
            }
            let working_bytes = vector.len().saturating_mul(std::mem::size_of::<f32>());
            used = used
                .saturating_add(std::mem::size_of::<VectorSearchResult>().saturating_add(id.len()));
            if used.saturating_add(working_bytes) > vector_budget {
                return Err(Error::msg("ERR fulltext vector memory limit exceeded"));
            }
            results.push(VectorSearchResult {
                id,
                score: fulltext_vector_distance(&distance, query, &vector)?,
                attrs: Vec::new(),
                attrs_json: None,
            });
            Ok(true)
        })?;
        results.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(results)
    }
}
