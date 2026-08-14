impl Db {
    fn vector_should_use_exact(&self, context: &VectorSearchContext<'_>) -> Result<bool, Error> {
        let runtime = self
            .vector_runtimes
            .get(self.db_index, context.index, context.version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        let candidate_count = context
            .allow_doc_ids
            .map(HashSet::len)
            .unwrap_or_else(|| runtime.len());
        drop(runtime);

        // HNSW is a nearest-neighbour index, not an exhaustive iterator.  If
        // the caller requests the whole candidate population, plan an exact
        // bounded scan up front so COUNT retains its result-count semantics.
        if context.options.k >= candidate_count && candidate_count <= vector_exact_scan_limit() {
            return Ok(true);
        }

        let Some(allow_doc_ids) = context.allow_doc_ids else {
            return Ok(false);
        };
        let exact_threshold = context
            .options
            .k
            .saturating_mul(4)
            .max(64)
            .min(vector_exact_scan_limit());
        Ok(allow_doc_ids.len() <= exact_threshold)
    }

    fn vector_approximate_results(
        &self,
        context: &VectorSearchContext<'_>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        self.ensure_vector_search_segments_loaded(
            context.index,
            context.version,
            context.meta,
        )?;
        let runtime = self
            .vector_runtimes
            .get(self.db_index, context.index, context.version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let live_count = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .len();
        if live_count == 0 || context.options.k == 0 {
            return Ok(Vec::new());
        }

        let filtered = !context.filters.is_empty() || context.allow_doc_ids.is_some();
        let max_candidates_by_memory = (vector_search_memory_budget_bytes()
            / (std::mem::size_of::<VectorCandidate>() + 64))
            .max(1);
        if context.options.k > max_candidates_by_memory {
            return Err(Error::msg("ERR vector search memory budget exceeded"));
        }
        let quantized_overfetch = match context.meta.quantization {
            VectorQuantization::F32 => 1,
            VectorQuantization::Q8 => 4,
            VectorQuantization::Binary => 32,
        };
        let candidate_multiplier = if filtered {
            quantized_overfetch.max(4)
        } else {
            quantized_overfetch
        };
        let mut candidate_limit = context
            .options
            .k
            .saturating_mul(candidate_multiplier)
            .max(if filtered { 16 } else { 1 })
            .min(live_count)
            .max(1);
        let candidate_cap = context
            .options
            .filter_ef
            .map(|filter_ef| {
                if filter_ef == 0 {
                    live_count
                } else {
                    filter_ef
                }
            })
            .unwrap_or_else(|| {
                if filtered {
                    context.options.k.saturating_mul(100).max(256)
                } else {
                    candidate_limit
                }
            })
            .max(candidate_limit)
            .min(live_count)
            .min(max_candidates_by_memory);
        let mut ef = context
            .options
            .ef
            .unwrap_or(context.meta.ef_runtime as usize)
            .max(candidate_limit)
            .max(context.options.k);

        loop {
            let candidates = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
                .search(context.query, candidate_limit, ef, context.allow_doc_ids)?;
            global_metrics().record_vector_ann_round(candidates.len());
            let mut results = self.vector_results_from_runtime_candidates(context, candidates)?;
            sort_and_limit_results(&mut results, context.options.k);
            if results.len() >= context.options.k || candidate_limit >= candidate_cap {
                return Ok(results);
            }
            candidate_limit = candidate_limit
                .saturating_mul(2)
                .max(candidate_limit.saturating_add(1))
                .min(candidate_cap);
            ef = ef
                .saturating_mul(2)
                .max(candidate_limit)
                .min(MAX_VECTOR_HNSW_EF);
        }
    }

    fn vector_results_from_runtime_candidates(
        &self,
        context: &VectorSearchContext<'_>,
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results =
            TopKVectorResults::new(context.options.k, vector_search_memory_budget_bytes())?;
        let keys = candidates
            .iter()
            .map(|candidate| {
                vector_doc_key(
                    self.key_layout,
                    self.db_index,
                    context.index,
                    context.version,
                    &candidate.id,
                )
            })
            .collect::<Vec<_>>();
        for (candidate, raw) in candidates.into_iter().zip(self.store.multi_get_raw(&keys)) {
            let Some(raw) = raw else {
                continue;
            };
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.deleted || doc.doc_version != candidate.doc_version {
                continue;
            }
            if let Some(result) = runtime_doc_to_search_result(
                &candidate.id,
                &doc,
                context.meta,
                context.query,
                None,
                &context.options.with_attrs,
                context.options.with_attrs_json,
                context.filters,
            )? {
                results.push(result)?;
            }
        }
        Ok(results.into_sorted())
    }

    fn vector_exact_results(
        &self,
        context: &VectorSearchContext<'_>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let scan_limit = vector_exact_scan_limit();
        let mut results =
            TopKVectorResults::new(context.options.k, vector_search_memory_budget_bytes())?;
        let scan_count = context
            .allow_doc_ids
            .map(HashSet::len)
            .unwrap_or(context.meta.doc_count as usize);
        if scan_count > scan_limit {
            return Err(Error::msg("ERR vector exact scan limit exceeded"));
        }
        match context.allow_doc_ids {
            Some(allow_doc_ids) => {
                let ids = allow_doc_ids.iter().collect::<Vec<_>>();
                let keys = ids
                    .iter()
                    .map(|id| {
                        vector_doc_key(
                            self.key_layout,
                            self.db_index,
                            context.index,
                            context.version,
                            id,
                        )
                    })
                    .collect::<Vec<_>>();
                for (id, raw) in ids.into_iter().zip(self.store.multi_get_raw(&keys)) {
                    let Some(raw) = raw else {
                        continue;
                    };
                    let doc = decode_record::<VectorDocRecord>(&raw)?;
                    if doc.deleted {
                        continue;
                    }
                    if let Some(result) = runtime_doc_to_search_result(
                        id,
                        &doc,
                        context.meta,
                        context.query,
                        None,
                        &context.options.with_attrs,
                        context.options.with_attrs_json,
                        context.filters,
                    )? {
                        results.push(result)?;
                    }
                }
            }
            None => {
                let prefix = vector_doc_prefix(
                    self.key_layout,
                    self.db_index,
                    context.index,
                    context.version,
                );
                for (_, raw) in self.store.scan_prefix_raw(&prefix) {
                    let doc = decode_record::<VectorDocRecord>(&raw)?;
                    if doc.deleted {
                        continue;
                    }
                    if let Some(result) = runtime_doc_to_search_result(
                        &doc.id,
                        &doc,
                        context.meta,
                        context.query,
                        None,
                        &context.options.with_attrs,
                        context.options.with_attrs_json,
                        context.filters,
                    )? {
                        results.push(result)?;
                    }
                }
            }
        }
        Ok(results.into_sorted())
    }

    fn vector_runtime_len(&self, index: &str, version: u64, fallback: u64) -> usize {
        self.vector_runtimes
            .get(self.db_index, index, version)
            .and_then(|graph| graph.read().ok().map(|graph| graph.len()))
            .unwrap_or(fallback as usize)
    }

    fn vector_runtime_stats(
        &self,
        index: &str,
        version: u64,
    ) -> (usize, usize, usize, usize, usize, usize) {
        self.vector_runtimes
            .get(self.db_index, index, version)
            .and_then(|runtime| {
                runtime.read().ok().map(|runtime| {
                    let (segments, total, deleted) = runtime.segment_stats();
                    let pending = runtime
                        .segments
                        .iter()
                        .filter(|segment| segment.meta.index_key.is_empty())
                        .count();
                    (
                        segments,
                        total,
                        deleted,
                        pending,
                        runtime.memtable_len(),
                        segments.saturating_sub(pending),
                    )
                })
            })
            .unwrap_or_default()
    }

    fn indexed_filter_doc_ids(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        filters: &[FilterPredicate],
    ) -> Result<Option<HashSet<String>>, Error> {
        let mut allow: Option<HashSet<String>> = None;
        for predicate in filters {
            let Some(field) = indexed_filter_field(meta, predicate) else {
                continue;
            };
            let doc_ids = match predicate {
                FilterPredicate::TagEq(_, value) => {
                    self.doc_ids_for_tag_value(index, version, field, value)?
                }
                FilterPredicate::TagIn(_, values) => {
                    let mut ids = HashSet::new();
                    let mut memory_used = 0usize;
                    for value in values {
                        for id in self.doc_ids_for_tag_value(index, version, field, value)? {
                            insert_bounded_doc_id(&mut ids, &mut memory_used, &id)?;
                        }
                    }
                    ids
                }
                FilterPredicate::TagNe(_, _) => unreachable!("negative tag filters are residual"),
                FilterPredicate::NumericCmp(_, op, value) => {
                    self.doc_ids_for_numeric_cmp(index, version, field, *op, *value)?
                }
            };
            allow = Some(match allow {
                Some(existing) => existing.intersection(&doc_ids).cloned().collect(),
                None => doc_ids,
            });
        }
        Ok(allow)
    }

    fn doc_ids_for_tag_value(
        &self,
        index: &str,
        version: u64,
        field: &str,
        value: &str,
    ) -> Result<HashSet<String>, Error> {
        let prefix =
            vector_tag_prefix(self.key_layout, self.db_index, index, version, field, value);
        let mut ids = HashSet::new();
        let mut result = Ok(());
        let mut memory_used = 0usize;
        let scan_limit = vector_exact_scan_limit();
        self.store.scan_range_raw_visit(
            &prefix,
            super::prefix_exclusive_upper_bound(&prefix),
            scan_limit.saturating_add(1),
            |key, _| {
                if ids.len() >= scan_limit {
                    result = Err(Error::msg("ERR vector filter scan limit exceeded"));
                    return false;
                }
                let Some(id) = key
                    .get(prefix.len()..)
                    .and_then(|id| std::str::from_utf8(id).ok())
                else {
                    return true;
                };
                if let Err(error) = insert_bounded_doc_id(&mut ids, &mut memory_used, id) {
                    result = Err(error);
                    return false;
                }
                true
            },
        );
        result?;
        Ok(ids)
    }

    fn doc_ids_for_numeric_cmp(
        &self,
        index: &str,
        version: u64,
        field: &str,
        op: NumericOp,
        expected: f64,
    ) -> Result<HashSet<String>, Error> {
        if !expected.is_finite() {
            return Err(Error::msg("ERR invalid vector numeric filter"));
        }
        let prefix =
            vector_numeric_field_prefix(self.key_layout, self.db_index, index, version, field);
        let mut score_prefix = prefix.clone();
        score_prefix.extend_from_slice(&sortable_f64(expected).to_be_bytes());
        let field_upper = super::prefix_exclusive_upper_bound(&prefix);
        let score_upper = super::prefix_exclusive_upper_bound(&score_prefix)
            .ok_or_else(|| Error::msg("ERR invalid vector numeric filter bound"))?;
        let (lower, upper) = match op {
            NumericOp::Eq => (score_prefix.clone(), Some(score_upper.clone())),
            NumericOp::Ne => unreachable!("negative numeric filters are residual"),
            NumericOp::Gt => (score_upper, field_upper),
            NumericOp::Ge => (score_prefix, field_upper),
            NumericOp::Lt => (prefix.clone(), Some(score_prefix)),
            NumericOp::Le => (prefix.clone(), Some(score_upper)),
        };
        let mut ids = HashSet::new();
        let mut result = Ok(());
        let mut memory_used = 0usize;
        let scan_limit = vector_exact_scan_limit();
        self.store
            .scan_range_raw_visit(&lower, upper, scan_limit.saturating_add(1), |key, _| {
                if ids.len() >= scan_limit {
                    result = Err(Error::msg("ERR vector filter scan limit exceeded"));
                    return false;
                }
                let Some(suffix) = key.get(prefix.len()..) else {
                    return true;
                };
                if suffix.len() < 8 {
                    return true;
                }
                if let Ok(id) = std::str::from_utf8(&suffix[8..])
                    && let Err(error) = insert_bounded_doc_id(&mut ids, &mut memory_used, id)
                {
                    result = Err(error);
                    return false;
                }
                true
            });
        result?;
        Ok(ids)
    }
}

fn insert_bounded_doc_id(
    ids: &mut HashSet<String>,
    memory_used: &mut usize,
    id: &str,
) -> Result<(), Error> {
    if ids.contains(id) {
        return Ok(());
    }
    let entry_bytes = id.len().saturating_add(std::mem::size_of::<String>() + 32);
    if memory_used.saturating_add(entry_bytes) > vector_search_memory_budget_bytes() {
        return Err(Error::msg("ERR vector search memory budget exceeded"));
    }
    ids.insert(id.to_string());
    *memory_used = memory_used.saturating_add(entry_bytes);
    Ok(())
}
