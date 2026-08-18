impl Db {
    pub(super) fn vector_exact_distance_results<F>(
        &self,
        index: &str,
        query: &[f32],
        allow_doc_ids: Option<&HashSet<String>>,
        limit: Option<usize>,
        max_score: Option<f32>,
        memory_budget: usize,
        mut should_continue: F,
    ) -> Result<Vec<VectorSearchResult>, Error>
    where
        F: FnMut() -> Result<bool, Error>,
    {
        let (_, _, meta) = self.read_vector_meta(index)?;
        validate_vector(query, meta.dim as usize)?;
        validate_vector_for_distance(query, meta.distance)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let query_norm_squared = vector_norm_squared(query);
        let mut top_k = limit
            .map(|limit| TopKVectorResults::new(limit, memory_budget))
            .transpose()?;
        let mut unbounded = Vec::new();
        let mut unbounded_bytes = 0usize;
        let mut scanned = 0usize;
        self.visit_vector_elements(index, |id, vector| {
            if !should_continue()? {
                return Ok(false);
            }
            scanned = scanned.saturating_add(1);
            if allow_doc_ids.is_some_and(|allow| !allow.contains(&id)) {
                return Ok(true);
            }
            let score = distance_score_prepared(
                meta.distance,
                query,
                query_norm_squared,
                &vector,
            )?;
            if max_score.is_some_and(|maximum| score > maximum) {
                return Ok(true);
            }
            let result = VectorSearchResult {
                id,
                score,
                attrs: Vec::new(),
                attrs_json: None,
            };
            if let Some(top_k) = top_k.as_mut() {
                top_k.push(result)?;
            } else {
                let bytes = estimated_vector_result_bytes(&result);
                if unbounded_bytes.saturating_add(bytes) > memory_budget {
                    return Err(Error::msg("ERR vector search memory budget exceeded"));
                }
                unbounded_bytes = unbounded_bytes.saturating_add(bytes);
                unbounded.push(result);
            }
            Ok(true)
        })?;
        global_metrics().record_vector_kv_doc_reads(scanned);
        if let Some(top_k) = top_k {
            Ok(top_k.into_sorted())
        } else {
            sort_and_limit_results(&mut unbounded, usize::MAX);
            Ok(unbounded)
        }
    }

    fn vector_should_use_exact(&self, context: &VectorSearchContext<'_>) -> Result<bool, Error> {
        let runtime = self
            .vector_runtimes
            .get(self.db_index, context.index, context.version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        let total_count = runtime.len();
        let candidate_count = context
            .allow_doc_ids
            .map(HashSet::len)
            .unwrap_or(total_count);
        drop(runtime);

        // HNSW is a nearest-neighbour index, not an exhaustive iterator.  If
        // the caller requests the whole candidate population, plan an exact
        // bounded scan up front so COUNT retains its result-count semantics.
        if context.options.k >= candidate_count && candidate_count <= vector_exact_scan_limit() {
            return Ok(true);
        }

        if context.allow_doc_ids.is_some()
            && candidate_count <= vector_exact_scan_limit()
            && candidate_count.saturating_mul(20) <= total_count
        {
            return Ok(true);
        }

        let exact_threshold = context
            .options
            .k
            .saturating_mul(4)
            .max(64)
            .min(vector_exact_scan_limit());
        // For small candidate populations an exact scan is both bounded and
        // deterministic. It also avoids losing the true nearest neighbour to
        // random HNSW topology before FP32 reranking gets a chance to run.
        Ok(candidate_count <= exact_threshold)
    }

    fn vector_approximate_results(
        &self,
        context: &VectorSearchContext<'_>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        self.ensure_vector_search_segments_loaded(context.index, context.version, context.meta)?;
        let runtime = self
            .vector_runtimes
            .get(self.db_index, context.index, context.version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let search_snapshot = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .search_snapshot();
        let live_count = search_snapshot.current_versions.len();
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
        // A size-proportional K-way split assumes nearest neighbours are
        // distributed like segment sizes, which is not generally true. Keep
        // one query-level budget, but give lossy quantized graphs a bounded
        // 2x competition window so one segment can contribute more than its
        // initial proportional share. This is still independent of fan-out.
        let default_candidate_limit = if context.meta.quantization == VectorQuantization::F32 {
            context.options.k
        } else {
            context.options.k.saturating_mul(2)
        };
        let mut candidate_limit = context
            .options
            .rerank
            .unwrap_or(default_candidate_limit)
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
            .max(context.options.k);

        loop {
            let mut candidates = search_snapshot.search_with_base_limit(
                context.query,
                candidate_limit,
                ef,
                context.allow_doc_ids,
            )?;
            if context.options.rerank.is_some()
                && context.meta.quantization != VectorQuantization::F32
            {
                candidates = self.vector_rerank_candidates_from_documents(context, candidates)?;
            }
            global_metrics().record_vector_ann_round(candidates.len());
            let mut results = self.vector_results_from_runtime_candidates(context, candidates)?;
            sort_and_limit_results(&mut results, context.options.k);
            if results.len() >= context.options.k {
                return Ok(results);
            }
            if candidate_limit >= candidate_cap {
                // A persisted HNSW graph can legitimately yield fewer than K nodes
                // (for example, when random level assignment leaves a small component).
                // Preserve COUNT semantics with a bounded exact fallback instead of
                // returning a short page even though enough live documents exist.
                let exact_candidate_count = context
                    .allow_doc_ids
                    .map(HashSet::len)
                    .unwrap_or(context.meta.doc_count as usize);
                if exact_candidate_count <= vector_exact_scan_limit() {
                    return self.vector_exact_results(context);
                }
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

    fn vector_rerank_candidates_from_documents(
        &self,
        context: &VectorSearchContext<'_>,
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorCandidate>, Error> {
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
        let raws = self.store.multi_get_raw(&keys);
        let mut reranked = Vec::with_capacity(candidates.len());
        let mut bytes = 0usize;
        for (mut candidate, raw) in candidates.into_iter().zip(raws) {
            let Some(raw) = raw else {
                continue;
            };
            bytes = bytes.saturating_add(raw.len());
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.deleted || doc.doc_version != candidate.doc_version {
                continue;
            }
            candidate.distance = distance_score_prepared(
                context.meta.distance,
                context.query,
                context.query_norm_squared,
                &doc.vector,
            )?;
            candidate.source_position = None;
            reranked.push(candidate);
        }
        global_metrics().record_vector_kv_doc_reads(keys.len());
        global_metrics().record_vector_kv_doc_bytes(bytes);
        global_metrics().record_vector_rerank_docs(reranked.len());
        Ok(reranked)
    }

    fn vector_results_from_runtime_candidates(
        &self,
        context: &VectorSearchContext<'_>,
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results =
            TopKVectorResults::new(context.options.k, vector_search_memory_budget_bytes())?;
        if context.filters.is_empty()
            && context.options.with_attrs.is_empty()
            && !context.options.with_attrs_json
        {
            for candidate in candidates {
                results.push(VectorSearchResult {
                    id: candidate.id,
                    score: candidate.distance,
                    attrs: Vec::new(),
                    attrs_json: None,
                })?;
            }
            return Ok(results.into_sorted());
        }
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
        global_metrics().record_vector_kv_doc_reads(keys.len());
        let mut kv_doc_bytes = 0usize;
        for (candidate, raw) in candidates.into_iter().zip(self.store.multi_get_raw(&keys)) {
            let Some(raw) = raw else {
                continue;
            };
            kv_doc_bytes = kv_doc_bytes.saturating_add(raw.len());
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if doc.deleted || doc.doc_version != candidate.doc_version {
                continue;
            }
            if let Some(result) = runtime_doc_to_search_result(
                &candidate.id,
                &doc,
                context.meta,
                context.query,
                context.query_norm_squared,
                None,
                &context.options.with_attrs,
                context.options.with_attrs_json,
                context.filters,
            )? {
                results.push(result)?;
            }
        }
        global_metrics().record_vector_kv_doc_bytes(kv_doc_bytes);
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
        if context.filters.is_empty()
            && context.options.with_attrs.is_empty()
            && !context.options.with_attrs_json
        {
            return self.vector_exact_distance_results(
                context.index,
                context.query,
                context.allow_doc_ids,
                Some(context.options.k),
                None,
                vector_search_memory_budget_bytes(),
                || Ok(true),
            );
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
                global_metrics().record_vector_kv_doc_reads(keys.len());
                let mut kv_doc_bytes = 0usize;
                for (id, raw) in ids.into_iter().zip(self.store.multi_get_raw(&keys)) {
                    let Some(raw) = raw else {
                        continue;
                    };
                    kv_doc_bytes = kv_doc_bytes.saturating_add(raw.len());
                    let doc = decode_record::<VectorDocRecord>(&raw)?;
                    if doc.deleted {
                        continue;
                    }
                    if let Some(result) = runtime_doc_to_search_result(
                        id,
                        &doc,
                        context.meta,
                        context.query,
                        context.query_norm_squared,
                        None,
                        &context.options.with_attrs,
                        context.options.with_attrs_json,
                        context.filters,
                    )? {
                        results.push(result)?;
                    }
                }
                global_metrics().record_vector_kv_doc_bytes(kv_doc_bytes);
            }
            None => {
                let prefix = vector_doc_prefix(
                    self.key_layout,
                    self.db_index,
                    context.index,
                    context.version,
                );
                let rows = self.store.scan_prefix_raw(&prefix);
                global_metrics().record_vector_kv_doc_reads(rows.len());
                global_metrics().record_vector_kv_doc_bytes(
                    rows.iter().map(|(_, raw)| raw.len()).sum(),
                );
                for (_, raw) in rows {
                    let doc = decode_record::<VectorDocRecord>(&raw)?;
                    if doc.deleted {
                        continue;
                    }
                    if let Some(result) = runtime_doc_to_search_result(
                        &doc.id,
                        &doc,
                        context.meta,
                        context.query,
                        context.query_norm_squared,
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
    ) -> (
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
    ) {
        self.vector_runtimes
            .get(self.db_index, index, version)
            .and_then(|runtime| {
                runtime.read().ok().map(|runtime| {
                    let (segments, total, deleted) = runtime.segment_stats();
                    let (delta_nodes, exact_tail_docs) = runtime.delta_stats();
                    let (rerank_source_docs, rerank_source_vector_bytes) =
                        runtime.rerank_source_stats();
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
                        delta_nodes,
                        exact_tail_docs,
                        rerank_source_docs,
                        rerank_source_vector_bytes,
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
                Some(mut existing) if existing.len() <= doc_ids.len() => {
                    existing.retain(|id| doc_ids.contains(id));
                    existing
                }
                Some(existing) => {
                    let mut doc_ids = doc_ids;
                    doc_ids.retain(|id| existing.contains(id));
                    doc_ids
                }
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
