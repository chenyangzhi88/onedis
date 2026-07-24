impl Db {
    fn fulltext_collect_live_hits(
        &self,
        index: &str,
        query: &str,
        options: &FullTextSearchOptions,
        mode: FullTextCollectMode,
    ) -> Result<FullTextCollectedHits, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        if (options.highlight || options.summarize)
            && (meta.index_options.no_hl || meta.index_options.no_offsets)
        {
            return Err(Error::msg(
                "ERR highlighting is disabled for this fulltext index",
            ));
        }
        fulltext_validate_search_geo_filters(&meta, &options.geo_filters)?;
        self.ensure_fulltext_runtime(&index)?;
        let query_timeout_ms = options.timeout_ms.unwrap_or(500);
        let refresh_started = Instant::now();
        let refresh_deadline = refresh_started
            .checked_add(Duration::from_millis(
                self.fulltext_search_refresh_timeout_ms(query_timeout_ms)?,
            ))
            .unwrap_or_else(|| {
                refresh_started + Duration::from_secs(100 * 365 * 24 * 60 * 60)
            });
        let fail_on_timeout = self
            .fulltext_config_string("ON_TIMEOUT", "RETURN")?
            .eq_ignore_ascii_case("FAIL");
        let caught_up =
            self.fulltext_refresh_index_until_caught_up(&index, refresh_deadline)?;
        if !caught_up && fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        // RedisSearch's TIMEOUT applies to query execution. Durable index
        // catch-up has its own REFRESH_TIMEOUT_MS budget and must not consume
        // the client's query budget.
        let query_started = Instant::now();
        let deadline = query_started
            .checked_add(Duration::from_millis(query_timeout_ms))
            .unwrap_or_else(|| {
                query_started + Duration::from_secs(100 * 365 * 24 * 60 * 60)
            });
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
            let hits = self.fulltext_vector_hits(
                &index,
                &meta,
                &runtime,
                &ast,
                options,
                FullTextSearchDeadline {
                    at: deadline,
                    fail_on_timeout,
                },
            )?;
            return Ok(FullTextCollectedHits {
                total: hits.len(),
                hits,
            });
        }
        if contains_fulltext_geo_query(&ast) {
            fulltext_validate_geo_query_ast(&meta, &ast)?;
            let hits = self.fulltext_exact_filter_hits(
                &meta,
                &ast,
                options,
                deadline,
                fail_on_timeout,
            )?;
            return Ok(FullTextCollectedHits {
                total: hits.len(),
                hits,
            });
        }
        let fetch_all = matches!(mode, FullTextCollectMode::All)
            || options.sort_by.is_some()
            || options.in_keys.is_some()
            || !options.filters.is_empty()
            || !options.geo_filters.is_empty()
            || options.inorder
            || !matches!(options.scorer, FullTextScorer::Bm25Std)
            || meta.index_options.score_field.is_some();
        let fetch_limit = (!fetch_all).then_some(options.offset.saturating_add(options.limit));
        let candidate_hits = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
            .search(query, options, fetch_limit)?;
        let mut live = Vec::new();
        for hit in candidate_hits.hits {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
            if options
                .in_keys
                .as_ref()
                .is_some_and(|keys| !keys.contains(&hit.key))
            {
                continue;
            }
            if let Some(hit) =
                self.fulltext_live_hit_from_source(&meta, options, hit.key, hit.score)?
            {
                if options.inorder
                    && !fulltext_eval_ast_against_fields(
                        &ast,
                        &hit.fields,
                        &meta,
                        options,
                    )?
                {
                    continue;
                }
                live.push(hit);
            }
        }
        self.fulltext_apply_selected_scorer(
            &meta,
            &ast,
            options,
            &mut live,
            deadline,
            fail_on_timeout,
        )?;
        if fetch_all && options.sort_by.is_none() {
            live.sort_by(|left, right| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.key.cmp(&right.key))
            });
        }
        Ok(FullTextCollectedHits {
            total: if fetch_all {
                live.len()
            } else {
                candidate_hits.total
            },
            hits: live,
        })
    }
}

fn fulltext_search_timeout_reached(
    deadline: Instant,
    fail_on_timeout: bool,
) -> Result<bool, Error> {
    if Instant::now() < deadline {
        return Ok(false);
    }
    if fail_on_timeout {
        return Err(Error::msg("Timeout limit was reached"));
    }
    Ok(true)
}
