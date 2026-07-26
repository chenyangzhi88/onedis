impl Db {
    fn hnsw_candidates(
        &self,
        context: &VectorSearchContext<'_>,
    ) -> Result<Option<Vec<VectorCandidate>>, Error> {
        let Some(runtime) =
            self.vector_runtimes
                .get(self.db_index, context.index, context.version)
        else {
            return Ok(None);
        };
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        if runtime.len() == 0 {
            return Ok(Some(Vec::new()));
        }
        let candidate_limit = context
            .options
            .k
            .saturating_mul(32)
            .max(64)
            .min(
                vector_search_memory_budget_bytes()
                    / (std::mem::size_of::<VectorCandidate>() + 64),
            )
            .min(runtime.len());
        let ef = context
            .options
            .ef
            .unwrap_or(context.meta.ef_runtime as usize)
            .max(candidate_limit)
            .max(context.options.k);
        runtime
            .search(
                context.query,
                candidate_limit,
                ef,
                context.allow_doc_ids,
            )
            .map(Some)
    }

    fn vector_results_from_candidates(
        &self,
        context: &VectorSearchContext<'_>,
        candidates: Vec<VectorCandidate>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results =
            TopKVectorResults::new(context.options.k, vector_search_memory_budget_bytes())?;
        for candidate in candidates {
            let Some(raw) = self.store.get_raw(&vector_doc_key(
                self.key_layout,
                self.db_index,
                context.index,
                context.version,
                &candidate.id,
            )) else {
                continue;
            };
            if let Some(result) = doc_to_search_result(
                &raw,
                context.meta,
                context.query,
                &context.options.with_attrs,
                context.filters,
                Some(candidate.doc_version),
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
        let mut scanned = 0usize;
        if let Some(allow_doc_ids) = context.allow_doc_ids {
            if allow_doc_ids.len() > scan_limit {
                return Err(Error::msg("ERR vector exact scan limit exceeded"));
            }
            for id in allow_doc_ids {
                scanned = scanned.saturating_add(1);
                if let Some(raw) = self.store.get_raw(&vector_doc_key(
                    self.key_layout,
                    self.db_index,
                    context.index,
                    context.version,
                    id,
                )) && let Some(result) = doc_to_search_result(
                    &raw,
                    context.meta,
                    context.query,
                    &context.options.with_attrs,
                    context.filters,
                    None,
                )?
                {
                    results.push(result)?;
                }
            }
        } else {
            let prefix = vector_doc_prefix(
                self.key_layout,
                self.db_index,
                context.index,
                context.version,
            );
            let mut visit_result = Ok(());
            self.store.scan_range_raw_visit(
                &prefix,
                super::prefix_exclusive_upper_bound(&prefix),
                scan_limit.saturating_add(1),
                |_, raw| {
                    scanned = scanned.saturating_add(1);
                    if scanned > scan_limit {
                        visit_result = Err(Error::msg("ERR vector exact scan limit exceeded"));
                        return false;
                    }
                    match doc_to_search_result(
                        raw,
                        context.meta,
                        context.query,
                        &context.options.with_attrs,
                        context.filters,
                        None,
                    ) {
                        Ok(Some(result)) => {
                            if let Err(error) = results.push(result) {
                                visit_result = Err(error);
                                return false;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            visit_result = Err(error);
                            return false;
                        }
                    }
                    true
                },
            );
            visit_result?;
        }
        Ok(results.into_sorted())
    }

    fn vector_runtime_len(&self, index: &str, version: u64, fallback: u64) -> usize {
        self.vector_runtimes
            .get(self.db_index, index, version)
            .and_then(|graph| graph.read().ok().map(|graph| graph.len()))
            .unwrap_or(fallback as usize)
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
        let prefix = vector_tag_prefix(
            self.key_layout,
            self.db_index,
            index,
            version,
            field,
            value,
        );
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
        let prefix = vector_numeric_field_prefix(
            self.key_layout,
            self.db_index,
            index,
            version,
            field,
        );
        let mut score_prefix = prefix.clone();
        score_prefix.extend_from_slice(&sortable_f64(expected).to_be_bytes());
        let field_upper = super::prefix_exclusive_upper_bound(&prefix);
        let score_upper = super::prefix_exclusive_upper_bound(&score_prefix)
            .ok_or_else(|| Error::msg("ERR invalid vector numeric filter bound"))?;
        let (lower, upper) = match op {
            NumericOp::Gt => (score_upper, field_upper),
            NumericOp::Ge => (score_prefix, field_upper),
            NumericOp::Lt => (prefix.clone(), Some(score_prefix)),
            NumericOp::Le => (prefix.clone(), Some(score_upper)),
        };
        let mut ids = HashSet::new();
        let mut result = Ok(());
        let mut memory_used = 0usize;
        let scan_limit = vector_exact_scan_limit();
        self.store.scan_range_raw_visit(
            &lower,
            upper,
            scan_limit.saturating_add(1),
            |key, _| {
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
    let entry_bytes = id
        .len()
        .saturating_add(std::mem::size_of::<String>() + 32);
    if memory_used.saturating_add(entry_bytes) > vector_search_memory_budget_bytes() {
        return Err(Error::msg("ERR vector search memory budget exceeded"));
    }
    ids.insert(id.to_string());
    *memory_used = memory_used.saturating_add(entry_bytes);
    Ok(())
}
