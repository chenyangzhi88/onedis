impl Db {
    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let started = Instant::now();
        let result = self.vector_search_inner(index, query, options);
        global_metrics().record_vector_search(elapsed_us(started), result.is_err());
        result
    }

    fn vector_search_inner(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let (_, version, meta) = self.read_vector_meta(index)?;
        validate_vector(query, meta.dim as usize)?;
        validate_vector_for_distance(query, meta.distance)?;
        let filters = options
            .filter
            .as_deref()
            .map(parse_filter)
            .transpose()?
            .unwrap_or_default();
        let allow_doc_ids = self.indexed_filter_doc_ids(index, version, &meta, &filters)?;
        self.ensure_vector_runtime(index, version, &meta)?;
        let context = VectorSearchContext {
            index,
            version,
            meta: &meta,
            query,
            options: &options,
            filters: &filters,
            allow_doc_ids: allow_doc_ids.as_ref(),
        };
        let mut used_hnsw = false;
        let mut results = Vec::new();
        if let Some(candidates) = self.hnsw_candidates(&context)? {
            used_hnsw = true;
            results = self.vector_results_from_candidates(&context, candidates)?;
            sort_and_limit_results(&mut results, options.k);
        }
        if !used_hnsw || results.len() < options.k {
            results = self.vector_exact_results(&context)?;
        }
        Ok(window_results(results, &options))
    }

    pub async fn vector_search_async(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let index = index.to_string();
        let query = query.to_vec();
        self.run_blocking_store_task(move |db| db.vector_search(&index, &query, options))
            .await
    }
}
