impl Db {
    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let started = Instant::now();
        let result = self.vector_search_inner(index, query, options, None, false);
        global_metrics().record_vector_search(elapsed_us(started), result.is_err());
        result
    }

    fn vector_search_inner(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
        external_allow_doc_ids: Option<Arc<HashSet<String>>>,
        query_is_projected: bool,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let (_, version, meta) = self.read_vector_meta(index)?;
        if options
            .ef
            .is_some_and(|ef| ef == 0 || ef > MAX_VECTOR_HNSW_EF)
        {
            return Err(Error::msg("ERR invalid vector EF"));
        }
        if options.filter_ef.is_some_and(|ef| ef > MAX_VECTOR_HNSW_EF) {
            return Err(Error::msg("ERR invalid vector FILTER-EF"));
        }
        TopKVectorResults::new(options.k, vector_search_memory_budget_bytes())?;
        let projected_query;
        let query = if let Some(projection) = meta.projection {
            if query_is_projected {
                validate_vector(query, meta.dim as usize)?;
                query
            } else {
                projected_query = project_vector(query, projection, meta.dim as usize)?;
                projected_query.as_slice()
            }
        } else {
            validate_vector(query, meta.dim as usize)?;
            query
        };
        validate_vector_for_distance(query, meta.distance)?;
        let filters = options
            .filter
            .as_deref()
            .map(parse_filter)
            .transpose()?
            .unwrap_or_default();
        let mut indexed_allow_doc_ids =
            self.indexed_filter_doc_ids(index, version, &meta, &filters)?;
        if let (Some(indexed), Some(external)) =
            (indexed_allow_doc_ids.as_mut(), external_allow_doc_ids.as_ref())
        {
            indexed.retain(|id| external.contains(id));
        }
        let allow_doc_ids = indexed_allow_doc_ids
            .as_ref()
            .or_else(|| external_allow_doc_ids.as_deref());
        let context = VectorSearchContext {
            index,
            version,
            meta: &meta,
            query,
            query_norm_squared: vector_norm_squared(query),
            options: &options,
            filters: &filters,
            allow_doc_ids,
        };
        let use_exact = if options.exact || meta.algorithm == VectorIndexAlgorithm::Flat {
            true
        } else {
            self.ensure_vector_runtime(index, version, &meta)?;
            self.vector_should_use_exact(&context)?
        };
        global_metrics()
            .record_vector_search_plan(use_exact, !filters.is_empty() || allow_doc_ids.is_some());
        let results = if use_exact {
            self.vector_exact_results(&context)?
        } else {
            self.vector_approximate_results(&context)?
        };
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

    pub(crate) fn vector_search_stored(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let started = Instant::now();
        let result = self.vector_search_inner(index, query, options, None, true);
        global_metrics().record_vector_search(elapsed_us(started), result.is_err());
        result
    }

    pub(crate) async fn vector_search_stored_async(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let index = index.to_string();
        let query = query.to_vec();
        self.run_blocking_store_task(move |db| db.vector_search_stored(&index, &query, options))
            .await
    }

    pub(in crate::store::db) fn vector_search_with_allow_ids(
        &self,
        index: &str,
        query: &[f32],
        options: VectorSearchOptions,
        allow_doc_ids: Arc<HashSet<String>>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let started = Instant::now();
        let result = self.vector_search_inner(index, query, options, Some(allow_doc_ids), false);
        global_metrics().record_vector_search(elapsed_us(started), result.is_err());
        result
    }
}
