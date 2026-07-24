impl FtSearch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.search' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        let query = arg(&frame, 2, "ERR invalid fulltext query")?;
        let mut options = default_fulltext_search_options();
        let mut idx = 3;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "LIMIT" => {
                    options.offset = parse_usize_arg(&frame, idx + 1, "ERR invalid LIMIT offset")?;
                    options.limit = parse_usize_arg(&frame, idx + 2, "ERR invalid LIMIT count")?;
                    idx += 3;
                }
                "NOCONTENT" => {
                    options.no_content = true;
                    idx += 1;
                }
                "WITHSCORES" => {
                    options.with_scores = true;
                    idx += 1;
                }
                "WITHPAYLOADS" => {
                    options.with_payloads = true;
                    idx += 1;
                }
                "WITHSORTKEYS" => {
                    options.with_sort_keys = true;
                    idx += 1;
                }
                "RETURN" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid RETURN count")?;
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    checked_count_end(&frame, start, count)?;
                    idx = start;
                    let mut fields = Vec::with_capacity(count);
                    for _ in 0..count {
                        let identifier = arg(&frame, idx, "ERR invalid RETURN field")?;
                        idx += 1;
                        let alias = if idx + 1 < frame.arg_len()
                            && upper_arg(&frame, idx)?.as_str() == "AS"
                        {
                            let alias = arg(&frame, idx + 1, "ERR invalid RETURN alias")?;
                            idx += 2;
                            Some(alias)
                        } else {
                            None
                        };
                        fields.push(FullTextReturnField { identifier, alias });
                    }
                    options.return_fields = Some(fields);
                    if count == 0 {
                        options.no_content = true;
                    }
                }
                "FILTER" => {
                    options.filters.push(FullTextSearchNumericFilter {
                        field: arg(&frame, idx + 1, "ERR invalid FILTER field")?,
                        min: parse_search_bound_arg(&frame, idx + 2, "ERR invalid FILTER min")?,
                        max: parse_search_bound_arg(&frame, idx + 3, "ERR invalid FILTER max")?,
                    });
                    idx += 4;
                }
                "GEOFILTER" => {
                    options.geo_filters.push(FullTextSearchGeoFilter {
                        field: arg(&frame, idx + 1, "ERR invalid GEOFILTER field")?,
                        lon: parse_f64_arg(&frame, idx + 2, "ERR invalid GEOFILTER lon")?,
                        lat: parse_f64_arg(&frame, idx + 3, "ERR invalid GEOFILTER lat")?,
                        radius: parse_f64_arg(&frame, idx + 4, "ERR invalid GEOFILTER radius")?,
                        unit: arg(&frame, idx + 5, "ERR invalid GEOFILTER unit")?,
                    });
                    idx += 6;
                }
                "INKEYS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid INKEYS count")?;
                    if count == 0 {
                        return Err(Error::msg("ERR invalid INKEYS count"));
                    }
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    let mut keys = HashSet::with_capacity(count);
                    while idx < end {
                        keys.insert(arg(&frame, idx, "ERR invalid INKEYS key")?);
                        idx += 1;
                    }
                    options.in_keys = Some(keys);
                }
                "INFIELDS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid INFIELDS count")?;
                    if count == 0 {
                        return Err(Error::msg("ERR invalid INFIELDS count"));
                    }
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    let mut fields = Vec::with_capacity(count);
                    while idx < end {
                        fields.push(arg(&frame, idx, "ERR invalid INFIELDS field")?);
                        idx += 1;
                    }
                    options.in_fields = Some(fields);
                }
                "SORTBY" => {
                    let field = arg(&frame, idx + 1, "ERR invalid SORTBY field")?;
                    idx += 2;
                    let asc = if idx < frame.arg_len() {
                        match upper_arg(&frame, idx)?.as_str() {
                            "ASC" => {
                                idx += 1;
                                true
                            }
                            "DESC" => {
                                idx += 1;
                                false
                            }
                            _ => true,
                        }
                    } else {
                        true
                    };
                    options.sort_by = Some(FullTextSortBy { field, asc });
                }
                "SUMMARIZE" => {
                    let (summarize, next) = parse_search_summarize_options(&frame, idx + 1)?;
                    options.summarize = Some(summarize);
                    idx = next;
                }
                "HIGHLIGHT" => {
                    let (highlight, next) = parse_search_highlight_options(&frame, idx + 1)?;
                    options.highlight = Some(highlight);
                    idx = next;
                }
                "SLOP" => {
                    options.slop = Some(
                        u32::try_from(parse_u64_arg(&frame, idx + 1, "ERR invalid SLOP")?)
                            .map_err(|_| Error::msg("ERR invalid SLOP"))?,
                    );
                    idx += 2;
                }
                "TIMEOUT" => {
                    options.timeout_ms =
                        Some(parse_u64_arg(&frame, idx + 1, "ERR invalid TIMEOUT")?);
                    idx += 2;
                }
                "INORDER" => {
                    options.inorder = true;
                    idx += 1;
                }
                "LANGUAGE" => {
                    options.language = Some(arg(&frame, idx + 1, "ERR invalid LANGUAGE")?);
                    idx += 2;
                }
                "EXPANDER" => {
                    let expander = upper_arg(&frame, idx + 1)?;
                    if expander != "DEFAULT" {
                        return Err(Error::msg("ERR unsupported fulltext expander"));
                    }
                    idx += 2;
                }
                "SCORER" => {
                    let scorer = upper_arg(&frame, idx + 1)?;
                    options.scorer = match scorer.as_str() {
                        "BM25" => FullTextScorer::Bm25,
                        "BM25STD" => FullTextScorer::Bm25Std,
                        "DISMAX" => FullTextScorer::DisMax,
                        "DOCSCORE" => FullTextScorer::DocScore,
                        _ => return Err(Error::msg("ERR unsupported fulltext scorer")),
                    };
                    idx += 2;
                }
                "EXPLAINSCORE" => {
                    options.explain_score = true;
                    idx += 1;
                }
                "PAYLOAD" => {
                    options.payload = Some(
                        frame
                            .get_arg_bytes(idx + 1)
                            .ok_or_else(|| Error::msg("ERR invalid PAYLOAD"))?,
                    );
                    idx += 2;
                }
                "PARAMS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid PARAMS count")?;
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    if count % 2 != 0 {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    while idx < end {
                        let name = arg(&frame, idx, "ERR invalid PARAMS name")?;
                        let value = frame
                            .get_arg_bytes(idx + 1)
                            .ok_or_else(|| Error::msg("ERR invalid PARAMS value"))?;
                        options.params.insert(name, value);
                        idx += 2;
                    }
                }
                "DIALECT" => {
                    let dialect = parse_u64_arg(&frame, idx + 1, "ERR invalid DIALECT")?;
                    if !(1..=4).contains(&dialect) {
                        return Err(Error::msg("ERR invalid DIALECT"));
                    }
                    options.dialect = dialect as u8;
                    options.dialect_explicit = true;
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        if options.explain_score && !options.with_scores {
            return Err(Error::msg(
                "ERR EXPLAINSCORE requires WITHSCORES",
            ));
        }
        Ok(Self {
            index,
            query,
            options,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_search(&self.index, &self.query, self.options)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_search_async(&self.index, &self.query, self.options)
            .await
    }
}

impl FtHybrid {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 7 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ft.hybrid' command",
            ));
        }
        let index = arg(&frame, 1, "ERR invalid fulltext index")?;
        if upper_arg(&frame, 2)? != "SEARCH" {
            return Err(Error::msg("ERR FT.HYBRID requires SEARCH"));
        }
        let search_query = arg(&frame, 3, "ERR invalid hybrid search query")?;
        let mut search = default_fulltext_search_options();
        search.no_content = true;
        search.with_scores = true;
        let mut search_score_alias = None;
        let mut vector_score_alias = None;
        let mut combined_score_alias = None;
        let mut idx = 4;
        while idx < frame.arg_len() && upper_arg(&frame, idx)? != "VSIM" {
            match upper_arg(&frame, idx)?.as_str() {
                "SCORER" => {
                    search.scorer = match upper_arg(&frame, idx + 1)?.as_str() {
                        "BM25" => FullTextScorer::Bm25,
                        "BM25STD" => FullTextScorer::Bm25Std,
                        "DISMAX" => FullTextScorer::DisMax,
                        "DOCSCORE" => FullTextScorer::DocScore,
                        _ => return Err(Error::msg("ERR unsupported fulltext scorer")),
                    };
                    idx += 2;
                }
                "YIELD_SCORE_AS" => {
                    search_score_alias =
                        Some(arg(&frame, idx + 1, "ERR invalid search score alias")?);
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        if idx >= frame.arg_len() || upper_arg(&frame, idx)? != "VSIM" {
            return Err(Error::msg("ERR FT.HYBRID requires VSIM"));
        }
        let vector_field = arg(&frame, idx + 1, "ERR invalid vector field")?
            .trim_start_matches('@')
            .to_string();
        let vector_arg = frame
            .get_arg_bytes(idx + 2)
            .ok_or_else(|| Error::msg("ERR invalid vector value"))?;
        let (vector_param, inline_vector) = match std::str::from_utf8(&vector_arg)
            .ok()
            .and_then(|value| value.strip_prefix('$'))
        {
            Some(name) if !name.is_empty() => (name.to_string(), None),
            _ => ("__hybrid_vector".to_string(), Some(vector_arg)),
        };
        if let Some(vector) = inline_vector {
            search.params.insert(vector_param.clone(), vector);
        }
        idx += 3;

        enum VectorClause {
            Knn(usize),
            Range(f64),
        }
        let vector_clause = match upper_arg(&frame, idx)
            .unwrap_or_else(|_| String::new())
            .as_str()
        {
            "KNN" => {
                let count = parse_usize_arg(&frame, idx + 1, "ERR invalid KNN argument count")?;
                let start = idx
                    .checked_add(2)
                    .ok_or_else(|| Error::msg("ERR syntax error"))?;
                let end = checked_count_end(&frame, start, count)?;
                let mut k = None;
                idx = start;
                while idx < end {
                    match upper_arg(&frame, idx)?.as_str() {
                        "K" => {
                            k = Some(parse_usize_arg(&frame, idx + 1, "ERR invalid K")?);
                            idx += 2;
                        }
                        "EF_RUNTIME" => {
                            parse_usize_arg(&frame, idx + 1, "ERR invalid EF_RUNTIME")?;
                            idx += 2;
                        }
                        "SHARD_K_RATIO" => {
                            let ratio =
                                parse_f64_arg(&frame, idx + 1, "ERR invalid SHARD_K_RATIO")?;
                            if !ratio.is_finite() || ratio <= 0.0 {
                                return Err(Error::msg("ERR invalid SHARD_K_RATIO"));
                            }
                            idx += 2;
                        }
                        "YIELD_SCORE_AS" | "YIELD_DISTANCE_AS" => {
                            vector_score_alias =
                                Some(arg(&frame, idx + 1, "ERR invalid vector score alias")?);
                            idx += 2;
                        }
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                    if idx > end {
                        return Err(Error::msg("ERR syntax error"));
                    }
                }
                let k = k.ok_or_else(|| Error::msg("ERR KNN requires K"))?;
                if k == 0 {
                    return Err(Error::msg("ERR K must be greater than zero"));
                }
                VectorClause::Knn(k)
            }
            "RANGE" => {
                let count =
                    parse_usize_arg(&frame, idx + 1, "ERR invalid RANGE argument count")?;
                let start = idx
                    .checked_add(2)
                    .ok_or_else(|| Error::msg("ERR syntax error"))?;
                let end = checked_count_end(&frame, start, count)?;
                let mut radius = None;
                idx = start;
                while idx < end {
                    match upper_arg(&frame, idx)?.as_str() {
                        "RADIUS" => {
                            let value =
                                parse_f64_arg(&frame, idx + 1, "ERR invalid RADIUS")?;
                            if !value.is_finite() || value < 0.0 {
                                return Err(Error::msg("ERR invalid RADIUS"));
                            }
                            radius = Some(value);
                            idx += 2;
                        }
                        "EPSILON" => {
                            let value =
                                parse_f64_arg(&frame, idx + 1, "ERR invalid EPSILON")?;
                            if !value.is_finite() || value < 0.0 {
                                return Err(Error::msg("ERR invalid EPSILON"));
                            }
                            idx += 2;
                        }
                        "YIELD_SCORE_AS" | "YIELD_DISTANCE_AS" => {
                            vector_score_alias =
                                Some(arg(&frame, idx + 1, "ERR invalid vector score alias")?);
                            idx += 2;
                        }
                        _ => return Err(Error::msg("ERR syntax error")),
                    }
                    if idx > end {
                        return Err(Error::msg("ERR syntax error"));
                    }
                }
                VectorClause::Range(
                    radius.ok_or_else(|| Error::msg("ERR RANGE requires RADIUS"))?,
                )
            }
            _ => VectorClause::Knn(10),
        };

        let mut filter = "*".to_string();
        let mut combine = FullTextHybridCombine::Rrf {
            window: 20,
            constant: 60.0,
        };
        let mut post_filter = None;
        let mut combine_seen = false;
        let mut no_sort = false;
        while idx < frame.arg_len() {
            match upper_arg(&frame, idx)?.as_str() {
                "FILTER" => {
                    let expression = arg(&frame, idx + 1, "ERR invalid hybrid FILTER")?;
                    idx += 2;
                    if combine_seen {
                        post_filter = Some(expression);
                    } else {
                        filter = expression;
                        if idx < frame.arg_len() && upper_arg(&frame, idx)? == "POLICY" {
                            match upper_arg(&frame, idx + 1)?.as_str() {
                                "ADHOC_BF" | "BATCHES" => {}
                                _ => return Err(Error::msg("ERR invalid hybrid FILTER policy")),
                            }
                            idx += 2;
                            if idx < frame.arg_len() && upper_arg(&frame, idx)? == "BATCH_SIZE" {
                                let batch_size = parse_usize_arg(
                                    &frame,
                                    idx + 1,
                                    "ERR invalid BATCH_SIZE",
                                )?;
                                if batch_size == 0 {
                                    return Err(Error::msg("ERR invalid BATCH_SIZE"));
                                }
                                idx += 2;
                            }
                        }
                    }
                }
                "YIELD_SCORE_AS" => {
                    let alias = arg(&frame, idx + 1, "ERR invalid score alias")?;
                    if combine_seen {
                        combined_score_alias = Some(alias);
                    } else {
                        vector_score_alias = Some(alias);
                    }
                    idx += 2;
                }
                "COMBINE" => {
                    combine_seen = true;
                    let kind = upper_arg(&frame, idx + 1)?;
                    let count =
                        parse_usize_arg(&frame, idx + 2, "ERR invalid COMBINE argument count")?;
                    let start = idx
                        .checked_add(3)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    let mut window = 20usize;
                    let mut constant = 60.0f32;
                    let mut alpha = 0.5f32;
                    let mut beta = 0.5f32;
                    idx = start;
                    while idx < end {
                        match upper_arg(&frame, idx)?.as_str() {
                            "WINDOW" => {
                                window =
                                    parse_usize_arg(&frame, idx + 1, "ERR invalid WINDOW")?;
                                if window == 0 {
                                    return Err(Error::msg(
                                        "ERR WINDOW must be greater than zero",
                                    ));
                                }
                                idx += 2;
                            }
                            "CONSTANT" => {
                                constant =
                                    parse_f64_arg(&frame, idx + 1, "ERR invalid CONSTANT")?
                                        as f32;
                                if !constant.is_finite() || constant <= 0.0 {
                                    return Err(Error::msg("ERR invalid CONSTANT"));
                                }
                                idx += 2;
                            }
                            "ALPHA" => {
                                alpha =
                                    parse_f64_arg(&frame, idx + 1, "ERR invalid ALPHA")? as f32;
                                if !alpha.is_finite() || alpha < 0.0 {
                                    return Err(Error::msg("ERR invalid ALPHA"));
                                }
                                idx += 2;
                            }
                            "BETA" => {
                                beta =
                                    parse_f64_arg(&frame, idx + 1, "ERR invalid BETA")? as f32;
                                if !beta.is_finite() || beta < 0.0 {
                                    return Err(Error::msg("ERR invalid BETA"));
                                }
                                idx += 2;
                            }
                            "YIELD_SCORE_AS" => {
                                combined_score_alias =
                                    Some(arg(&frame, idx + 1, "ERR invalid combined score alias")?);
                                idx += 2;
                            }
                            _ => return Err(Error::msg("ERR syntax error")),
                        }
                        if idx > end {
                            return Err(Error::msg("ERR syntax error"));
                        }
                    }
                    combine = match kind.as_str() {
                        "RRF" => FullTextHybridCombine::Rrf { window, constant },
                        "LINEAR" => FullTextHybridCombine::Linear {
                            window,
                            alpha,
                            beta,
                        },
                        _ => return Err(Error::msg("ERR unsupported hybrid combiner")),
                    };
                }
                "LOAD" => {
                    if arg(&frame, idx + 1, "ERR invalid LOAD")? == "*" {
                        search.return_fields = None;
                        search.no_content = false;
                        idx += 2;
                        continue;
                    }
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid LOAD count")?;
                    idx = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    checked_count_end(&frame, idx, count)?;
                    let mut fields = Vec::new();
                    for _ in 0..count {
                        let identifier = arg(&frame, idx, "ERR invalid LOAD field")?
                            .trim_start_matches('@')
                            .to_string();
                        idx += 1;
                        let alias = if idx + 1 < frame.arg_len()
                            && upper_arg(&frame, idx)? == "AS"
                        {
                            let alias = arg(&frame, idx + 1, "ERR invalid LOAD alias")?;
                            idx += 2;
                            Some(alias)
                        } else {
                            None
                        };
                        fields.push(FullTextReturnField { identifier, alias });
                    }
                    search.return_fields = Some(fields);
                    search.no_content = count == 0;
                }
                "SORTBY" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid SORTBY count")?;
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    if count == 0 {
                        return Err(Error::msg("ERR invalid SORTBY count"));
                    }
                    let field = arg(&frame, start, "ERR invalid SORTBY field")?
                        .trim_start_matches('@')
                        .to_string();
                    let asc = if start + 1 < end {
                        match upper_arg(&frame, start + 1)?.as_str() {
                            "ASC" => true,
                            "DESC" => false,
                            _ => return Err(Error::msg("ERR syntax error")),
                        }
                    } else {
                        true
                    };
                    if count > 2 {
                        return Err(Error::msg(
                            "ERR multiple hybrid SORTBY fields are not supported",
                        ));
                    }
                    search.sort_by = Some(FullTextSortBy { field, asc });
                    idx = end;
                }
                "NOSORT" => {
                    no_sort = true;
                    search.sort_by = None;
                    idx += 1;
                }
                "LIMIT" => {
                    search.offset =
                        parse_usize_arg(&frame, idx + 1, "ERR invalid LIMIT offset")?;
                    search.limit = parse_usize_arg(&frame, idx + 2, "ERR invalid LIMIT count")?;
                    idx += 3;
                }
                "PARAMS" => {
                    let count = parse_usize_arg(&frame, idx + 1, "ERR invalid PARAMS count")?;
                    if count % 2 != 0 {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let start = idx
                        .checked_add(2)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    let end = checked_count_end(&frame, start, count)?;
                    idx = start;
                    while idx < end {
                        let name = arg(&frame, idx, "ERR invalid PARAMS name")?
                            .trim_start_matches('$')
                            .to_string();
                        let value = frame
                            .get_arg_bytes(idx + 1)
                            .ok_or_else(|| Error::msg("ERR invalid PARAMS value"))?;
                        search.params.insert(name, value);
                        idx += 2;
                    }
                }
                "TIMEOUT" => {
                    search.timeout_ms =
                        Some(parse_u64_arg(&frame, idx + 1, "ERR invalid TIMEOUT")?);
                    idx += 2;
                }
                "DIALECT" => {
                    let dialect = parse_u64_arg(&frame, idx + 1, "ERR invalid DIALECT")?;
                    if !(2..=4).contains(&dialect) {
                        return Err(Error::msg("ERR invalid DIALECT"));
                    }
                    search.dialect = dialect as u8;
                    search.dialect_explicit = true;
                    idx += 2;
                }
                "WITHSCORES" => {
                    search.with_scores = true;
                    idx += 1;
                }
                "NOCONTENT" => {
                    search.no_content = true;
                    idx += 1;
                }
                "FORMAT" => {
                    match upper_arg(&frame, idx + 1)?.as_str() {
                        "STRING" | "EXPAND" => {}
                        _ => return Err(Error::msg("ERR invalid FORMAT")),
                    }
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        if !search.params.contains_key(&vector_param) {
            return Err(Error::msg("ERR missing vector parameter"));
        }
        let vector_query = match vector_clause {
            VectorClause::Knn(k) => format!(
                "({filter})=>[KNN {k} @{vector_field} ${vector_param}]"
            ),
            VectorClause::Range(radius) => {
                format!("({filter}) @{vector_field}:[VECTOR_RANGE {radius} ${vector_param}]")
            }
        };
        Ok(Self {
            index,
            search_query,
            vector_query,
            options: FullTextHybridOptions {
                search,
                combine,
                search_score_alias,
                vector_score_alias,
                combined_score_alias,
                post_filter,
                no_sort,
            },
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_hybrid(
            &self.index,
            &self.search_query,
            &self.vector_query,
            self.options,
        )
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.fulltext_hybrid_async(
            &self.index,
            &self.search_query,
            &self.vector_query,
            self.options,
        )
        .await
    }
}
