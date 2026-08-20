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
        // High-selectivity indexed predicates are cheaper as ANN-first: avoid
        // materializing and hashing most of the collection, then validate the
        // bounded ANN candidates from their documents. Low/medium selectivity
        // keeps the indexed allow-set and can switch to an exact filtered scan.
        if external_allow_doc_ids.is_none()
            && indexed_allow_doc_ids.as_ref().is_some_and(|ids| {
                ids.len().saturating_mul(2) >= meta.doc_count as usize
            })
        {
            indexed_allow_doc_ids = None;
        }
        if let (Some(indexed), Some(external)) =
            (indexed_allow_doc_ids.as_mut(), external_allow_doc_ids.as_ref())
        {
            indexed.retain(|id| external.contains(id));
        }
        let allow_doc_ids = indexed_allow_doc_ids
            .as_ref()
            .or(external_allow_doc_ids.as_deref());
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
            if self
                .vector_runtimes
                .get(self.db_index, index, version)
                .is_none()
            {
                return Err(Error::msg("INDEX_NOT_READY vector runtime is loading"));
            }
            self.vector_should_use_exact(&context)?
        };
        global_metrics()
            .record_vector_search_plan(use_exact, !filters.is_empty() || allow_doc_ids.is_some());
        if !filters.is_empty() || external_allow_doc_ids.is_some() {
            global_metrics().record_vector_filter_mode(use_exact, allow_doc_ids.is_some());
        }
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
        let db = self.shared_task_view();
        vector_search_executor()?
            .execute(move || db.vector_search(&index, &query, options))
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
        let db = self.shared_task_view();
        vector_search_executor()?
            .execute(move || db.vector_search_stored(&index, &query, options))
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

struct VectorSearchExecutor {
    _runtime: std::sync::Mutex<Option<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
    permits: Arc<tokio::sync::Semaphore>,
}

impl VectorSearchExecutor {
    fn from_env() -> Result<Self, Error> {
        let default_workers = std::thread::available_parallelism()
            .map(|parallelism| (parallelism.get() / 2).max(1))
            .unwrap_or(2);
        let workers = std::env::var("ONEDIS_VECTOR_SEARCH_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default_workers);
        let max_in_flight = std::env::var("ONEDIS_VECTOR_SEARCH_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| workers.saturating_mul(4).max(workers));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("onedis-vector")
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        Ok(Self {
            _runtime: std::sync::Mutex::new(Some(runtime)),
            handle,
            permits: Arc::new(tokio::sync::Semaphore::new(max_in_flight)),
        })
    }

    async fn execute<T, F>(&self, operation: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Error> + Send + 'static,
    {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::msg("BUSY vector search executor is saturated"))?;
        self.handle
            .spawn_blocking(move || {
                let _permit = permit;
                operation()
            })
            .await
            .map_err(|error| Error::msg(format!("vector search worker failed: {error}")))?
    }
}

fn vector_search_executor() -> Result<&'static VectorSearchExecutor, Error> {
    static EXECUTOR: OnceLock<Result<VectorSearchExecutor, String>> = OnceLock::new();
    EXECUTOR
        .get_or_init(|| VectorSearchExecutor::from_env().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| Error::msg(format!("failed to initialize vector executor: {error}")))
}
