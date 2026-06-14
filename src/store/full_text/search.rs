impl Db {
    pub fn fulltext_search(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.SEARCH")?;
        let options = self.fulltext_effective_search_options(options)?;
        let live = self.fulltext_collect_live_hits(index, query, &options)?;
        self.fulltext_search_frame(live, &options, &fulltext_display_terms(query))
    }

    fn fulltext_collect_live_hits(
        &self,
        index: &str,
        query: &str,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        fulltext_validate_search_geo_filters(&meta, &options.geo_filters)?;
        self.ensure_fulltext_runtime(&index)?;
        self.fulltext_refresh_index(&index, true)?;
        let runtime = self
            .fulltext_runtimes
            .get(self.db_index, &index)
            .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
        let ast_query = if fulltext_query_has_vector_syntax(query) {
            query.to_string()
        } else {
            substitute_fulltext_params(query, &options.params)?
        };
        let ast = FullTextQueryParser::new(&ast_query, options.dialect).parse()?;
        if contains_fulltext_vector_query(&ast) {
            return self.fulltext_vector_hits(&index, &meta, &runtime, &ast, options);
        }
        if contains_fulltext_geo_query(&ast) {
            fulltext_validate_geo_query_ast(&meta, &ast)?;
            return self.fulltext_exact_filter_hits(&meta, &ast, options);
        }
        let mut candidate_hits = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
            .search(query, options)?;
        let candidate_keys = candidate_hits
            .iter()
            .map(|hit| hit.key.clone())
            .collect::<Vec<_>>();
        if matches!(meta.source_type, FullTextSourceType::Hash)
            && self.fulltext_revalidate_hash_candidates(&runtime, &candidate_keys)?
        {
            candidate_hits = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .search(query, options)?;
        }
        let mut live = Vec::new();
        for hit in candidate_hits {
            if options
                .in_keys
                .as_ref()
                .is_some_and(|keys| !keys.contains(&hit.key))
            {
                continue;
            }
            if let Some(hit) =
                self.fulltext_live_hit_from_source(&meta, &options, hit.key, hit.score)?
            {
                live.push(hit);
            }
        }
        Ok(live)
    }

    fn fulltext_vector_hits(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        runtime: &Arc<RwLock<FullTextRuntime>>,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let plan = fulltext_vector_plan(ast)?;
        let query_vector = parse_fulltext_vector_param(&options.params, &plan.blob_param)?;
        let vector_field = fulltext_vector_schema_field(meta, &plan.field)?;
        let vector_index = fulltext_vector_index_name(index, vector_field.attribute_name());
        let allow = if let Some(filter) = plan.filter {
            let hits = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .search_ast(filter, &options)?;
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
            self.fulltext_vector_exact_results(&vector_index, vector_field, &query_vector)?
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
                self.fulltext_live_hit_from_source(meta, &options, result.id, result.score)?
            {
                let score = format_fulltext_score(result.score);
                hit.fields
                    .push(("__vector_score".to_string(), score.clone()));
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

    fn fulltext_exact_filter_hits(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let keys = self.fulltext_source_keys(meta)?;
        let mut live = Vec::new();
        for key in keys {
            if options
                .in_keys
                .as_ref()
                .is_some_and(|keys| !keys.contains(&key))
            {
                continue;
            }
            let Some(hit) = self.fulltext_live_hit_from_source(meta, options, key, 1.0)? else {
                continue;
            };
            if fulltext_eval_ast_against_fields(ast, &hit.fields, meta, options)? {
                live.push(hit);
            }
        }
        Ok(live)
    }

    fn fulltext_source_keys(&self, meta: &FullTextIndexMeta) -> Result<Vec<String>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => self.fulltext_matching_hash_keys(meta),
            FullTextSourceType::Json => {
                let mut keys = HashSet::new();
                for prefix in &meta.prefixes {
                    for (raw_key, raw_value) in
                        self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
                    {
                        if raw_key.len() < 2 || Self::decode_json_meta(&raw_value).is_err() {
                            continue;
                        }
                        let key = String::from_utf8_lossy(&raw_key[2..]).to_string();
                        if key.starts_with(prefix) {
                            keys.insert(key);
                        }
                    }
                }
                let mut keys = keys.into_iter().collect::<Vec<_>>();
                keys.sort();
                Ok(keys)
            }
        }
    }

    fn fulltext_live_hit_from_source(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        key: String,
        score: f32,
    ) -> Result<Option<FullTextLiveHit>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => {
                let fields = self.hash_get_all(&key)?;
                if fields.is_empty()
                    || !fulltext_fields_match_filters(&fields, &options.filters)
                    || !fulltext_fields_match_geo_filters(&fields, &options.geo_filters)?
                {
                    return Ok(None);
                }
                let sort_key = options
                    .sort_by
                    .as_ref()
                    .and_then(|sort_by| fulltext_field_value(&fields, &sort_by.field));
                Ok(Some(FullTextLiveHit {
                    key,
                    score,
                    fields,
                    sort_key,
                }))
            }
            FullTextSourceType::Json => {
                let Some(filter_fields) = self.fulltext_json_fields(&key, meta)? else {
                    return Ok(None);
                };
                if !fulltext_fields_match_filters(&filter_fields, &options.filters) {
                    return Ok(None);
                }
                if !fulltext_fields_match_geo_filters(&filter_fields, &options.geo_filters)? {
                    return Ok(None);
                }
                let sort_key = options
                    .sort_by
                    .as_ref()
                    .and_then(|sort_by| fulltext_field_value(&filter_fields, &sort_by.field));
                let fields = self.fulltext_json_return_fields(&key, meta, options.dialect)?;
                Ok(Some(FullTextLiveHit {
                    key,
                    score,
                    fields,
                    sort_key,
                }))
            }
        }
    }

    fn fulltext_search_frame(
        &self,
        mut live: Vec<FullTextLiveHit>,
        options: &FullTextSearchOptions,
        display_terms: &[String],
    ) -> Result<Frame, Error> {
        if let Some(sort_by) = &options.sort_by {
            live.sort_by(|left, right| compare_fulltext_sort_keys(left, right, sort_by.asc));
        }
        let total = live.len();
        let mut out = Vec::new();
        out.push(Frame::Integer(total as i64));
        if options.limit == 0 {
            return Ok(Frame::Array(out));
        }
        for hit in live.into_iter().skip(options.offset).take(options.limit) {
            out.push(Frame::bulk_string(hit.key));
            if options.with_scores {
                out.push(Frame::bulk_string(format_fulltext_score(hit.score)));
                if options.explain_score {
                    out.push(Frame::Array(vec![
                        Frame::bulk_string("score"),
                        Frame::bulk_string(format_fulltext_score(hit.score)),
                    ]));
                }
            }
            if options.with_payloads {
                out.push(Frame::Null);
            }
            if options.with_sort_keys {
                out.push(
                    hit.sort_key
                        .clone()
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                );
            }
            if !options.no_content {
                out.push(fulltext_fields_frame(
                    hit.fields,
                    options.return_fields.as_deref(),
                    options,
                    display_terms,
                ));
            }
        }
        Ok(Frame::Array(out))
    }

    pub async fn fulltext_search_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_search(index, query, options)
    }

    pub fn fulltext_explain(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
        cli: bool,
    ) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        let options = self.fulltext_effective_search_options(options)?;
        let ast_query = substitute_fulltext_params(query, &options.params)?;
        let ast = FullTextQueryParser::new(&ast_query, options.dialect).parse()?;
        if contains_fulltext_geo_query(&ast) {
            fulltext_validate_geo_query_ast(&meta, &ast)?;
        }
        let lines = fulltext_explain_ast_lines(&ast);
        if cli {
            Ok(Frame::Array(
                lines.into_iter().map(Frame::bulk_string).collect(),
            ))
        } else {
            Ok(Frame::bulk_string(lines.join("\n")))
        }
    }

    pub async fn fulltext_explain_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
        cli: bool,
    ) -> Result<Frame, Error> {
        self.fulltext_explain(index, query, options, cli)
    }

    pub fn fulltext_tagvals(&self, index: &str, field: &str) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        let Some(schema) = fulltext_schema_field(&meta, field) else {
            return Err(Error::msg("ERR invalid tag field"));
        };
        if !matches!(schema.kind, FullTextFieldKind::Tag) {
            return Err(Error::msg("ERR invalid tag field"));
        }
        let attribute = schema.attribute_name().to_string();
        let mut values = BTreeSet::new();
        for key in self.fulltext_source_keys(&meta)? {
            let fields = match meta.source_type {
                FullTextSourceType::Hash => self.hash_get_all(&key)?,
                FullTextSourceType::Json => {
                    self.fulltext_json_fields(&key, &meta)?.unwrap_or_default()
                }
            };
            for (_, value) in fields.iter().filter(|(name, _)| name == &attribute) {
                for tag in split_tag_values(value) {
                    values.insert(tag);
                }
            }
        }
        Ok(Frame::Array(
            values.into_iter().map(Frame::bulk_string).collect(),
        ))
    }

    pub async fn fulltext_tagvals_async(&self, index: &str, field: &str) -> Result<Frame, Error> {
        self.fulltext_tagvals(index, field)
    }


}
