use super::*;
impl Db {
    pub fn fulltext_search(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        let started = Instant::now();
        let result = self.fulltext_search_inner(index, query, options);
        global_metrics().record_fulltext_search(elapsed_us(started));
        result
    }

    pub(super) fn fulltext_search_inner(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.SEARCH")?;
        let options = self.fulltext_effective_search_options(options)?;
        let live =
            self.fulltext_collect_live_hits(index, query, &options, FullTextCollectMode::Page)?;
        self.fulltext_search_frame(live, &options, &fulltext_display_terms(query))
    }

    pub async fn fulltext_search_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        let query = query.to_string();
        self.run_fulltext_search_task(move |db| db.fulltext_search(&index, &query, options))
            .await
    }

    pub fn fulltext_hybrid(
        &self,
        index: &str,
        search_query: &str,
        vector_query: &str,
        options: FullTextHybridOptions,
    ) -> Result<Frame, Error> {
        let started = Instant::now();
        let result = self.fulltext_hybrid_inner(index, search_query, vector_query, options);
        global_metrics().record_fulltext_search(elapsed_us(started));
        result
    }

    fn fulltext_hybrid_inner(
        &self,
        index: &str,
        search_query: &str,
        vector_query: &str,
        options: FullTextHybridOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.HYBRID")?;
        let search_options = self.fulltext_effective_search_options(options.search.clone())?;
        let window = match options.combine {
            FullTextHybridCombine::Rrf { window, .. }
            | FullTextHybridCombine::Linear { window, .. } => window,
        };
        // Both branches only need IDs and scores for fusion. Source fields are
        // materialized once, after fusion and final pagination.
        let mut candidate_options = search_options.clone();
        candidate_options.no_content = true;
        candidate_options.return_fields = None;
        candidate_options.highlight = None;
        candidate_options.summarize = None;
        candidate_options.with_payloads = false;
        candidate_options.with_sort_keys = false;
        let text = self.fulltext_collect_live_hits(
            index,
            search_query,
            &candidate_options,
            FullTextCollectMode::Window(window),
        )?;
        let vector = self.fulltext_collect_live_hits(
            index,
            vector_query,
            &candidate_options,
            FullTextCollectMode::Window(window),
        )?;
        let mut combined = combine_fulltext_hybrid_hits(text, vector, &options)?;
        if let Some(post_filter) = &options.post_filter {
            let candidate_keys = combined
                .hits
                .iter()
                .map(|hit| hit.key.clone())
                .collect::<HashSet<_>>();
            let mut post_options = search_options.clone();
            post_options.in_keys = Some(candidate_keys);
            post_options.offset = 0;
            post_options.limit = combined.hits.len();
            post_options.no_content = true;
            post_options.return_fields = None;
            post_options.highlight = None;
            post_options.summarize = None;
            post_options.with_payloads = false;
            post_options.with_sort_keys = false;
            post_options.sort_by = None;
            post_options.scorer = FullTextScorer::Bm25Std;
            let allowed = self.fulltext_collect_live_hits(
                index,
                post_filter,
                &post_options,
                FullTextCollectMode::Window(combined.hits.len()),
            )?;
            let allowed = allowed
                .hits
                .into_iter()
                .map(|hit| hit.key)
                .collect::<HashSet<_>>();
            combined.hits.retain(|hit| allowed.contains(&hit.key));
            combined.total = combined.hits.len();
        }
        let total = combined.hits.len();
        let selected = combined
            .hits
            .into_iter()
            .skip(search_options.offset)
            .take(search_options.limit)
            .collect::<Vec<_>>();
        let hits = if search_options.no_content {
            selected
        } else {
            let resolved = self.resolve_fulltext_index(index)?;
            let runtime = self
                .fulltext_runtimes
                .get(self.db_index, &resolved)
                .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
            let meta = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .search_meta
                .clone();
            let mut materialized = Vec::with_capacity(selected.len());
            for candidate in selected {
                let score = candidate.score;
                let synthetic_fields = candidate.fields;
                if let Some(mut hit) = self.fulltext_live_hit_from_source(
                    &meta,
                    &search_options,
                    candidate.key,
                    score,
                )? {
                    // Hybrid fusion owns the final score. Source materialization
                    // must not apply a text scorer or document-score multiplier.
                    hit.score = score;
                    for field in synthetic_fields {
                        if !hit.fields.iter().any(|existing| existing.0 == field.0) {
                            hit.fields.push(field);
                        }
                    }
                    materialized.push(hit);
                }
            }
            materialized
        };
        combined = FullTextCollectedHits {
            total,
            hits,
            page_offset_applied: true,
        };
        self.fulltext_search_frame(
            combined,
            &search_options,
            &fulltext_display_terms(search_query),
        )
    }

    pub async fn fulltext_hybrid_async(
        &self,
        index: &str,
        search_query: &str,
        vector_query: &str,
        options: FullTextHybridOptions,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        let search_query = search_query.to_string();
        let vector_query = vector_query.to_string();
        self.run_fulltext_search_task(move |db| {
            db.fulltext_hybrid(&index, &search_query, &vector_query, options)
        })
        .await
    }
}

fn combine_fulltext_hybrid_hits(
    text: FullTextCollectedHits,
    vector: FullTextCollectedHits,
    options: &FullTextHybridOptions,
) -> Result<FullTextCollectedHits, Error> {
    struct HybridHit {
        hit: FullTextLiveHit,
        text_score: Option<f32>,
        text_rank: Option<usize>,
        vector_score: Option<f32>,
        vector_rank: Option<usize>,
    }

    let window = match options.combine {
        FullTextHybridCombine::Rrf { window, .. }
        | FullTextHybridCombine::Linear { window, .. } => window,
    };
    let text_hits = text.hits.into_iter().take(window).collect::<Vec<_>>();
    let vector_hits = vector.hits.into_iter().take(window).collect::<Vec<_>>();
    let text_range = fulltext_score_range(&text_hits);
    let vector_range = fulltext_score_range(&vector_hits);
    let mut hits = HashMap::<String, HybridHit>::new();
    for (rank, hit) in text_hits.into_iter().enumerate() {
        let score = hit.score;
        if let Some(alias) = &options.search_score_alias {
            let mut hit = hit;
            hit.fields
                .push((alias.clone(), format_fulltext_score(score)));
            hits.insert(
                hit.key.clone(),
                HybridHit {
                    hit,
                    text_score: Some(score),
                    text_rank: Some(rank + 1),
                    vector_score: None,
                    vector_rank: None,
                },
            );
        } else {
            hits.insert(
                hit.key.clone(),
                HybridHit {
                    hit,
                    text_score: Some(score),
                    text_rank: Some(rank + 1),
                    vector_score: None,
                    vector_rank: None,
                },
            );
        }
    }
    for (rank, mut hit) in vector_hits.into_iter().enumerate() {
        let score = hit.score;
        if let Some(alias) = &options.vector_score_alias {
            hit.fields
                .push((alias.clone(), format_fulltext_score(score)));
        }
        if let Some(existing) = hits.get_mut(&hit.key) {
            existing.vector_score = Some(score);
            existing.vector_rank = Some(rank + 1);
            if let Some(alias) = &options.vector_score_alias {
                existing
                    .hit
                    .fields
                    .push((alias.clone(), format_fulltext_score(score)));
            }
        } else {
            hits.insert(
                hit.key.clone(),
                HybridHit {
                    hit,
                    text_score: None,
                    text_rank: None,
                    vector_score: Some(score),
                    vector_rank: Some(rank + 1),
                },
            );
        }
    }

    let mut combined = hits
        .into_values()
        .map(|mut candidate| {
            let score = match options.combine {
                FullTextHybridCombine::Rrf { constant, .. } => {
                    candidate
                        .text_rank
                        .map_or(0.0, |rank| 1.0 / (constant + rank as f32))
                        + candidate
                            .vector_rank
                            .map_or(0.0, |rank| 1.0 / (constant + rank as f32))
                }
                FullTextHybridCombine::Linear { alpha, beta, .. } => {
                    alpha
                        * candidate.text_score.map_or(0.0, |score| {
                            normalize_fulltext_score(score, text_range, false)
                        })
                        + beta
                            * candidate.vector_score.map_or(0.0, |score| {
                                normalize_fulltext_score(score, vector_range, true)
                            })
                }
            };
            if !score.is_finite() {
                return Err(Error::msg("ERR invalid hybrid score"));
            }
            candidate.hit.score = score;
            if let Some(alias) = &options.combined_score_alias {
                candidate
                    .hit
                    .fields
                    .push((alias.clone(), format_fulltext_score(score)));
            }
            Ok(candidate.hit)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    if !options.no_sort {
        combined.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.key.cmp(&right.key))
        });
    }
    combined.truncate(window);
    Ok(FullTextCollectedHits {
        total: combined.len(),
        hits: combined,
        page_offset_applied: false,
    })
}

fn fulltext_score_range(hits: &[FullTextLiveHit]) -> Option<(f32, f32)> {
    let mut scores = hits.iter().map(|hit| hit.score);
    let first = scores.next()?;
    Some(scores.fold((first, first), |(min, max), score| {
        (min.min(score), max.max(score))
    }))
}

fn normalize_fulltext_score(score: f32, range: Option<(f32, f32)>, lower_is_better: bool) -> f32 {
    let Some((min, max)) = range else {
        return 0.0;
    };
    if (max - min).abs() <= f32::EPSILON {
        return 1.0;
    }
    if lower_is_better {
        (max - score) / (max - min)
    } else {
        (score - min) / (max - min)
    }
}
