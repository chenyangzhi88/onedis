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
        let mut results = Vec::new();
        for candidate in candidates {
            let Some(raw) = self.store.get_raw(&vector_doc_key(
                self.db_index,
                context.index,
                context.version,
                &candidate.id,
            )) else {
                continue;
            };
            if let Some(result) = doc_to_search_result(
                raw,
                context.meta,
                context.query,
                &context.options.with_attrs,
                context.filters,
                Some(candidate.doc_version),
            )? {
                results.push(result);
            }
        }
        Ok(results)
    }

    fn vector_exact_results(
        &self,
        context: &VectorSearchContext<'_>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let mut results = Vec::new();
        if let Some(allow_doc_ids) = context.allow_doc_ids {
            for id in allow_doc_ids {
                if let Some(raw) = self.store.get_raw(&vector_doc_key(
                    self.db_index,
                    context.index,
                    context.version,
                    id,
                )) && let Some(result) = doc_to_search_result(
                    raw,
                    context.meta,
                    context.query,
                    &context.options.with_attrs,
                    context.filters,
                    None,
                )?
                {
                    results.push(result);
                }
            }
        } else {
            let prefix = vector_doc_prefix(self.db_index, context.index, context.version);
            for (_, raw) in self.store.scan_prefix_raw(&prefix) {
                if let Some(result) = doc_to_search_result(
                    raw,
                    context.meta,
                    context.query,
                    &context.options.with_attrs,
                    context.filters,
                    None,
                )?
                {
                    results.push(result);
                }
            }
        }
        sort_and_limit_results(&mut results, context.options.k);
        Ok(results)
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
                    self.doc_ids_for_tag_value(index, version, field, value)
                }
                FilterPredicate::TagIn(_, values) => {
                    let mut ids = HashSet::new();
                    for value in values {
                        ids.extend(self.doc_ids_for_tag_value(index, version, field, value));
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
    ) -> HashSet<String> {
        let prefix = vector_tag_prefix(self.db_index, index, version, field, value);
        self.store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(key, _)| String::from_utf8(key.get(prefix.len()..)?.to_vec()).ok())
            .collect()
    }

    fn doc_ids_for_numeric_cmp(
        &self,
        index: &str,
        version: u64,
        field: &str,
        op: NumericOp,
        expected: f64,
    ) -> Result<HashSet<String>, Error> {
        let prefix = vector_numeric_field_prefix(self.db_index, index, version, field);
        let mut ids = HashSet::new();
        for (key, _) in self.store.scan_prefix_raw(&prefix) {
            let Some(suffix) = key.get(prefix.len()..) else {
                continue;
            };
            if suffix.len() < 8 {
                continue;
            }
            let mut encoded = [0u8; 8];
            encoded.copy_from_slice(&suffix[..8]);
            let actual = unsortable_f64(u64::from_be_bytes(encoded));
            let matches = match op {
                NumericOp::Gt => actual > expected,
                NumericOp::Ge => actual >= expected,
                NumericOp::Lt => actual < expected,
                NumericOp::Le => actual <= expected,
            };
            if matches && let Ok(id) = String::from_utf8(suffix[8..].to_vec()) {
                ids.insert(id);
            }
        }
        Ok(ids)
    }
}
