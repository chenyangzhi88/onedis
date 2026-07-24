impl Db {
    fn fulltext_vector_hits(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        runtime: &Arc<RwLock<FullTextRuntime>>,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        timeout: FullTextSearchDeadline,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let plan = fulltext_vector_plan(ast)?;
        let query_vector = parse_fulltext_vector_param(&options.params, &plan.blob_param)?;
        let vector_field = fulltext_vector_schema_field(meta, &plan.field)?;
        let vector_index = fulltext_vector_index_name(index, vector_field.attribute_name());
        let allow = if let Some(filter) = plan.filter.as_ref() {
            let hits = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .search_ast(filter, options)?;
            Some(hits.into_iter().map(|hit| hit.key).collect::<HashSet<_>>())
        } else {
            None
        };
        let vector_results = if allow.is_some()
            || matches!(plan.kind, FullTextVectorPlanKind::Range { .. })
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
                timeout.at,
                timeout.fail_on_timeout,
            )?
        } else {
            let vector_limit = match plan.kind {
                FullTextVectorPlanKind::Knn { k } => {
                    k.max(options.offset.saturating_add(options.limit))
                }
                FullTextVectorPlanKind::Range { .. } => self.vector_card(&vector_index)? as usize,
            }
            .max(1);
            self.vector_search(
                &vector_index,
                &query_vector,
                VectorSearchOptions {
                    k: vector_limit,
                    filter: None,
                    with_scores: true,
                    with_attrs: Vec::new(),
                    ef: None,
                    offset: 0,
                    limit: None,
                },
            )?
        };
        let mut live = Vec::new();
        for result in vector_results {
            if fulltext_search_timeout_reached(timeout.at, timeout.fail_on_timeout)? {
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
                let score = format_fulltext_score(result.score);
                hit.fields.push(("__vector_score".to_string(), score.clone()));
                hit.fields
                    .push((format!("__{}_score", vector_field.attribute_name()), score));
                live.push(hit);
                if matches!(plan.kind, FullTextVectorPlanKind::Knn { k } if live.len() >= k) {
                    break;
                }
            }
        }
        Ok(live)
    }

    fn fulltext_vector_exact_results(
        &self,
        vector_index: &str,
        vector_field: &FullTextFieldSchema,
        query: &[f32],
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
        for id in self.vector_ids(vector_index)? {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
            let Some(element) = self.vector_element(vector_index, &id)? else {
                continue;
            };
            results.push(VectorSearchResult {
                id,
                score: fulltext_vector_distance(&distance, query, &element.vector)?,
                attrs: Vec::new(),
            });
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
