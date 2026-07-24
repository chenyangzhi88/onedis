use super::*;
impl Db {
    pub(super) fn fulltext_collect_live_hits(
        &self,
        index: &str,
        query: &str,
        options: &FullTextSearchOptions,
        mode: FullTextCollectMode,
    ) -> Result<FullTextCollectedHits, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, &index);
        let result_cap = match mode {
            FullTextCollectMode::Page => self.fulltext_config_usize("MAXSEARCHRESULTS", 10_000)?,
            FullTextCollectMode::All => {
                self.fulltext_config_usize("MAXAGGREGATERESULTS", 10_000)?
            }
        };
        let reader_budget = self.fulltext_config_usize("MEMORY_BUDGET_READER_BYTES", 67_108_864)?;
        let sort_budget = self.fulltext_config_usize("MEMORY_BUDGET_SORT_BYTES", 16_777_216)?;
        let query_timeout_ms = options.timeout_ms.unwrap_or(500);
        let refresh_started = Instant::now();
        let refresh_deadline = refresh_started
            .checked_add(Duration::from_millis(
                self.fulltext_search_refresh_timeout_ms(query_timeout_ms)?,
            ))
            .unwrap_or_else(|| refresh_started + Duration::from_secs(100 * 365 * 24 * 60 * 60));
        let fail_on_timeout = self
            .fulltext_config_string("ON_TIMEOUT", "RETURN")?
            .eq_ignore_ascii_case("FAIL");
        let caught_up = {
            let _refresh_guard = lifecycle_lock
                .write()
                .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
            self.fulltext_refresh_index_until_caught_up(&index, refresh_deadline)?
        };
        if !caught_up && fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        // Catch-up mutates the active generation and therefore needs the
        // lifecycle write lock. Queries only retain a read lock, so searches
        // can run concurrently while ALTER/DROP cannot reclaim their storage.
        let _lifecycle_guard = lifecycle_lock
            .read()
            .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        if (options.highlight.is_some() || options.summarize.is_some())
            && (meta.index_options.no_hl || meta.index_options.no_offsets)
        {
            return Err(Error::msg(
                "ERR highlighting is disabled for this fulltext index",
            ));
        }
        fulltext_validate_search_geo_filters(&meta, &options.geo_filters)?;
        // RedisSearch's TIMEOUT applies to query execution. Durable index
        // catch-up has its own REFRESH_TIMEOUT_MS budget and must not consume
        // the client's query budget.
        let query_started = Instant::now();
        let deadline = query_started
            .checked_add(Duration::from_millis(query_timeout_ms))
            .unwrap_or_else(|| query_started + Duration::from_secs(100 * 365 * 24 * 60 * 60));
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
                FullTextSearchLimits {
                    timeout: FullTextSearchDeadline {
                        at: deadline,
                        fail_on_timeout,
                    },
                    result_cap,
                    reader_budget,
                },
            )?;
            fulltext_validate_collected_hit_budget(&hits, result_cap, reader_budget)?;
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
                FullTextSearchLimits {
                    timeout: FullTextSearchDeadline {
                        at: deadline,
                        fail_on_timeout,
                    },
                    result_cap,
                    reader_budget,
                },
            )?;
            fulltext_validate_collected_hit_budget(&hits, result_cap, reader_budget)?;
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
        let fetch_limit = if fetch_all {
            Some(result_cap.saturating_add(1))
        } else {
            Some(options.offset.saturating_add(options.limit))
        };
        let runtime_guard = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
        let segment_count = runtime_guard
            .reader
            .searcher()
            .segment_readers()
            .len()
            .max(1);
        if fetch_limit
            .unwrap_or(usize::MAX)
            .saturating_mul(std::mem::size_of::<super::runtime_search::FullTextScoredDoc>())
            .saturating_mul(segment_count)
            > reader_budget
        {
            return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
        }
        let candidate_hits = runtime_guard.search(
            query,
            options,
            fetch_limit,
            FullTextSearchDeadline {
                at: deadline,
                fail_on_timeout,
            },
        )?;
        drop(runtime_guard);
        if fetch_all && candidate_hits.hits.len() > result_cap {
            return Err(Error::msg("ERR fulltext result limit exceeded"));
        }
        let candidate_bytes = candidate_hits.hits.iter().fold(0usize, |used, hit| {
            used.saturating_add(
                std::mem::size_of::<FullTextSearchHit>().saturating_add(hit.key.len()),
            )
        });
        if candidate_bytes > reader_budget {
            return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
        }
        let mut live = Vec::new();
        let mut live_bytes = 0usize;
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
                    && !fulltext_eval_ast_against_fields(&ast, &hit.fields, &meta, options)?
                {
                    continue;
                }
                live_bytes = live_bytes.saturating_add(estimate_fulltext_live_hit_bytes(&hit));
                if live_bytes > reader_budget {
                    return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
                }
                if options.sort_by.is_some() && live_bytes > sort_budget {
                    return Err(Error::msg("ERR fulltext sort memory limit exceeded"));
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

pub(super) fn estimate_fulltext_live_hit_bytes(hit: &FullTextLiveHit) -> usize {
    std::mem::size_of::<FullTextLiveHit>()
        .saturating_add(hit.key.len())
        .saturating_add(
            hit.fields
                .iter()
                .map(|(name, value)| name.len().saturating_add(value.len()))
                .sum::<usize>(),
        )
        .saturating_add(hit.sort_key.as_ref().map_or(0, String::len))
        .saturating_add(hit.payload.as_ref().map_or(0, String::len))
}

pub(super) fn fulltext_validate_collected_hit_budget(
    hits: &[FullTextLiveHit],
    result_cap: usize,
    reader_budget: usize,
) -> Result<(), Error> {
    if hits.len() > result_cap {
        return Err(Error::msg("ERR fulltext result limit exceeded"));
    }
    let used = hits.iter().fold(0usize, |used, hit| {
        used.saturating_add(estimate_fulltext_live_hit_bytes(hit))
    });
    if used > reader_budget {
        Err(Error::msg("ERR fulltext reader memory limit exceeded"))
    } else {
        Ok(())
    }
}

pub(super) fn fulltext_search_timeout_reached(
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
