use super::*;
impl Db {
    pub(super) fn fulltext_collect_live_hits(
        &self,
        index: &str,
        query: &str,
        options: &FullTextSearchOptions,
        mode: FullTextCollectMode,
    ) -> Result<FullTextCollectedHits, Error> {
        let resolve_started = Instant::now();
        let index = self.resolve_fulltext_index(index)?;
        let lifecycle_lock = self.fulltext_runtimes.lifecycle_lock(self.db_index, &index);
        {
            let _lifecycle_guard = lifecycle_lock
                .read()
                .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
            if self.fulltext_runtimes.get(self.db_index, &index).is_none() {
                return Err(Error::msg("INDEX_NOT_READY fulltext runtime is loading"));
            }
        }
        let result_cap = match mode {
            FullTextCollectMode::Page => self.fulltext_config_usize("MAXSEARCHRESULTS", 10_000)?,
            FullTextCollectMode::All | FullTextCollectMode::Window(_) => {
                self.fulltext_config_usize("MAXAGGREGATERESULTS", 10_000)?
            }
        };
        if matches!(mode, FullTextCollectMode::Window(window) if window > result_cap) {
            return Err(Error::msg("ERR fulltext result limit exceeded"));
        }
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
        let consistent = self
            .fulltext_config_string("CONSISTENCY", "CONSISTENT")?
            .eq_ignore_ascii_case("CONSISTENT");
        global_metrics().record_fulltext_search_stage(
            FullTextSearchStage::Resolve,
            elapsed_us(resolve_started),
        );
        let refresh_started = Instant::now();
        let caught_up = if consistent {
            self.fulltext_refresh_index_until_caught_up(&index, refresh_deadline)?
        } else {
            if self.fulltext_runtimes.get(self.db_index, &index).is_none() {
                return Err(Error::msg("INDEX_NOT_READY fulltext runtime is loading"));
            }
            true
        };
        if !caught_up && fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        global_metrics().record_fulltext_search_stage(
            FullTextSearchStage::RefreshWait,
            elapsed_us(refresh_started),
        );
        // Capture one immutable generation atomically under the lifecycle
        // lease. Query planning, collection and source materialization never
        // acquire or retain the mutable writer lock.
        let generation = {
            let _lifecycle_guard = lifecycle_lock
                .read()
                .map_err(|_| Error::msg("ERR fulltext lifecycle lock poisoned"))?;
            let generation = self
                .fulltext_runtimes
                .get_search_generation(self.db_index, &index)
                .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
            generation.ensure_active()?;
            generation
        };
        let meta = generation.search_meta.clone();
        if (options.highlight.is_some() || options.summarize.is_some())
            && (meta.index_options.no_hl || meta.index_options.no_offsets)
        {
            return Err(Error::msg(
                "ERR highlighting is disabled for this fulltext index",
            ));
        }
        fulltext_validate_search_geo_filters(&meta, &options.geo_filters)?;
        let fast_sort = if let Some(sort_by) = &options.sort_by {
            let field = fulltext_schema_field(&meta, &sort_by.field)
                .ok_or_else(|| Error::msg("ERR invalid SORTBY field"))?;
            if !field.options.sortable {
                return Err(Error::msg("ERR SORTBY field is not SORTABLE"));
            }
            options.in_keys.is_none()
                && options.filters.is_empty()
                && options.geo_filters.is_empty()
                && !options.inorder
                && !matches!(options.scorer, FullTextScorer::DocScore)
                && meta.index_options.score_field.is_none()
                && !options.with_scores
                && !options.explain_score
        } else {
            false
        };
        // RedisSearch's TIMEOUT applies to query execution. Near-real-time
        // publication has its own REFRESH_TIMEOUT_MS budget and must not
        // consume the client's query budget.
        let query_started = Instant::now();
        let deadline = query_started
            .checked_add(Duration::from_millis(query_timeout_ms))
            .unwrap_or_else(|| query_started + Duration::from_secs(100 * 365 * 24 * 60 * 60));
        let plan_started = Instant::now();
        let initial_ast = match self.fulltext_runtimes.query_ast(
            self.db_index,
            &index,
            meta.incarnation,
            options.dialect,
            query,
        ) {
            Ok(ast) => ast,
            Err(error) if !options.params.is_empty() => {
                let substituted = substitute_fulltext_params(query, &options.params)?;
                self.fulltext_runtimes
                    .query_ast(
                        self.db_index,
                        &index,
                        meta.incarnation,
                        options.dialect,
                        &substituted,
                    )
                    .map_err(|_| error)?
            }
            Err(error) => return Err(error),
        };
        let ast = if contains_fulltext_vector_query(&initial_ast) {
            initial_ast
        } else {
            let substituted = substitute_fulltext_params(query, &options.params)?;
            self.fulltext_runtimes.query_ast(
                self.db_index,
                &index,
                meta.incarnation,
                options.dialect,
                &substituted,
            )?
        };
        global_metrics()
            .record_fulltext_search_stage(FullTextSearchStage::ParsePlan, elapsed_us(plan_started));
        if contains_fulltext_vector_query(&ast) {
            let hits = self.fulltext_vector_hits(
                &index,
                &meta,
                &generation,
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
            generation.ensure_active()?;
            return Ok(FullTextCollectedHits {
                total: hits.len(),
                hits,
                page_offset_applied: false,
            });
        }
        if contains_fulltext_geo_query(&ast) || !options.geo_filters.is_empty() {
            let mut geo_children = vec![ast.as_ref().clone()];
            geo_children.extend(
                options
                    .geo_filters
                    .iter()
                    .map(|filter| FullTextQueryAst::Geo {
                        field: filter.field.clone(),
                        lon: filter.lon,
                        lat: filter.lat,
                        radius: filter.radius,
                        unit: filter.unit.clone(),
                    }),
            );
            let geo_ast = if geo_children.len() == 1 {
                geo_children.pop().expect("one geo query")
            } else {
                FullTextQueryAst::And(geo_children)
            };
            fulltext_validate_geo_query_ast(&meta, &geo_ast)?;
            let candidate_limit = reader_budget
                .checked_div(std::mem::size_of::<FullTextSearchHit>().max(1))
                .unwrap_or(0)
                .max(result_cap);
            let candidates = generation.search_ast(
                &geo_ast,
                options,
                candidate_limit.saturating_add(1),
                FullTextSearchDeadline {
                    at: deadline,
                    fail_on_timeout,
                },
            )?;
            let candidate_bytes = candidates.iter().fold(0usize, |used, hit| {
                used.saturating_add(
                    std::mem::size_of::<FullTextSearchHit>().saturating_add(hit.key.len()),
                )
            });
            if candidates.len() > candidate_limit || candidate_bytes > reader_budget {
                return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
            }
            let candidates = candidates
                .into_iter()
                .map(|candidate| {
                    let exact = generation.fast_geo_matches(&geo_ast, candidate.address)?;
                    Ok((candidate, exact))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let mut hits = Vec::new();
            for (candidate, fast_exact) in candidates {
                if let Some(exact) = fast_exact {
                    if !exact {
                        continue;
                    }
                    if let Some(hit) = self.fulltext_live_hit_from_source(
                        &meta,
                        options,
                        candidate.key,
                        candidate.score,
                    )? {
                        hits.push(hit);
                        if hits.len() > result_cap {
                            return Err(Error::msg("ERR fulltext result limit exceeded"));
                        }
                    }
                    continue;
                }
                let Some(fields) =
                    self.fulltext_filter_fields_from_source(&meta, &candidate.key)?
                else {
                    continue;
                };
                if !fulltext_eval_ast_against_fields(&geo_ast, &fields, &meta, options)? {
                    continue;
                }
                if let Some(hit) = self.fulltext_live_hit_from_source(
                    &meta,
                    options,
                    candidate.key,
                    candidate.score,
                )? {
                    hits.push(hit);
                    if hits.len() > result_cap {
                        return Err(Error::msg("ERR fulltext result limit exceeded"));
                    }
                }
            }
            fulltext_validate_collected_hit_budget(&hits, result_cap, reader_budget)?;
            generation.ensure_active()?;
            return Ok(FullTextCollectedHits {
                total: hits.len(),
                hits,
                page_offset_applied: false,
            });
        }
        let requires_source_validation = fulltext_query_requires_source_validation(&ast, options);
        let bounded_window = match mode {
            FullTextCollectMode::Window(window) if !requires_source_validation => Some(window),
            _ => None,
        };
        let fetch_all = bounded_window.is_none()
            && (matches!(mode, FullTextCollectMode::All)
                || (options.sort_by.is_some() && !fast_sort)
                || requires_source_validation
                || matches!(options.scorer, FullTextScorer::DocScore)
                || meta.index_options.score_field.is_some());
        let fetch_limit = if let Some(window) = bounded_window {
            Some(window)
        } else if fetch_all {
            Some(result_cap.saturating_add(1))
        } else {
            Some(options.offset.saturating_add(options.limit))
        };
        let segment_count = generation.reader.searcher().segment_readers().len().max(1);
        if fetch_limit
            .unwrap_or(usize::MAX)
            .saturating_mul(std::mem::size_of::<super::runtime_search::FullTextScoredDoc>())
            .saturating_mul(segment_count)
            > reader_budget
        {
            return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
        }
        let index_search_started = Instant::now();
        let page_offset_applied =
            matches!(mode, FullTextCollectMode::Page) && !fetch_all && bounded_window.is_none();
        let key_offset = if page_offset_applied {
            options.offset
        } else {
            0
        };
        let candidate_hits = if fast_sort {
            generation.search_sorted_ast(
                &ast,
                options,
                fetch_limit.unwrap_or(0),
                key_offset,
                FullTextSearchDeadline {
                    at: deadline,
                    fail_on_timeout,
                },
            )?
        } else if page_offset_applied {
            generation.search_ast_page_hits(
                &ast,
                options,
                fetch_limit.unwrap_or(0),
                key_offset,
                FullTextSearchDeadline {
                    at: deadline,
                    fail_on_timeout,
                },
            )?
        } else {
            generation.search_ast_hits(
                &ast,
                options,
                fetch_limit,
                FullTextSearchDeadline {
                    at: deadline,
                    fail_on_timeout,
                },
            )?
        };
        global_metrics().record_fulltext_search_stage(
            FullTextSearchStage::IndexSearch,
            elapsed_us(index_search_started),
        );
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
        let candidate_count = if page_offset_applied {
            candidate_hits.total.min(fetch_limit.unwrap_or(0))
        } else {
            candidate_hits.hits.len()
        };
        let candidate_total = candidate_hits.total;
        let candidate_hits = candidate_hits.hits;

        let source_free = options.no_content
            && options.sort_by.is_none()
            && !options.with_payloads
            && !options.with_sort_keys
            && !requires_source_validation
            && !matches!(options.scorer, FullTextScorer::DocScore)
            && meta.index_options.score_field.is_none();
        if source_free || candidate_hits.is_empty() {
            let document_score = meta.index_options.score.unwrap_or(1.0) as f32;
            let live = candidate_hits
                .into_iter()
                .map(|hit| {
                    let score = hit.score * document_score;
                    FullTextLiveHit {
                        key: hit.key,
                        score: if matches!(options.scorer, FullTextScorer::Bm25) {
                            search_source_scoring::fulltext_legacy_bm25_score(score)
                        } else {
                            score
                        },
                        fields: Vec::new(),
                        sort_key: None,
                        payload: None,
                    }
                })
                .collect::<Vec<_>>();
            global_metrics().record_fulltext_search_work(
                candidate_count,
                0,
                0,
                segment_count,
                fetch_all,
            );
            generation.ensure_active()?;
            return Ok(FullTextCollectedHits {
                total: if fetch_all || bounded_window.is_some() {
                    live.len()
                } else {
                    candidate_total
                },
                hits: live,
                page_offset_applied,
            });
        }

        let source_started = Instant::now();
        let mut live = Vec::new();
        let mut live_bytes = 0usize;
        let mut source_candidates = Vec::with_capacity(candidate_hits.len());
        for hit in candidate_hits {
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
            source_candidates.push(hit);
        }
        for hit in self.fulltext_live_hits_from_source(&meta, options, source_candidates)? {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
            if requires_source_validation
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
        global_metrics().record_fulltext_search_stage(
            FullTextSearchStage::SourceLoad,
            elapsed_us(source_started),
        );
        let post_process_started = Instant::now();
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
        global_metrics().record_fulltext_search_stage(
            FullTextSearchStage::PostProcess,
            elapsed_us(post_process_started),
        );
        global_metrics().record_fulltext_search_work(
            candidate_count,
            live.len(),
            live_bytes,
            segment_count,
            fetch_all,
        );
        generation.ensure_active()?;
        Ok(FullTextCollectedHits {
            total: if fetch_all || bounded_window.is_some() {
                live.len()
            } else {
                candidate_total
            },
            hits: live,
            page_offset_applied,
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
