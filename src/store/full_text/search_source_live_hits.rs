impl Db {
    fn fulltext_exact_filter_hits(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        deadline: Instant,
        fail_on_timeout: bool,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let keys = self.fulltext_source_keys(meta)?;
        let mut live = Vec::new();
        for key in keys {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
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
            if !fulltext_index_filter_matches(meta, &hit.fields)? {
                continue;
            }
            if fulltext_eval_ast_against_fields(ast, &hit.fields, meta, options)? {
                live.push(hit);
            }
        }
        Ok(live)
    }

    fn fulltext_source_keys(&self, meta: &FullTextIndexMeta) -> Result<Vec<String>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => self.fulltext_matching_hash_keys(meta),
            FullTextSourceType::Json => self.fulltext_matching_source_keys(meta, TYPE_JSON),
        }
    }

    fn fulltext_live_hit_from_source(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        key: String,
        score: f32,
    ) -> Result<Option<FullTextLiveHit>, Error> {
        self.expire_if_needed(&key);
        let expected_type = match meta.source_type {
            FullTextSourceType::Hash => TYPE_HASH,
            FullTextSourceType::Json => TYPE_JSON,
        };
        if !self
            .store
            .get_raw(&self.mk(&key))
            .and_then(|raw| decode_meta_header(&raw))
            .is_some_and(|header| header.type_tag == expected_type)
        {
            return Ok(None);
        }
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
                let document_score = fulltext_document_score(meta, &fields);
                let payload = fulltext_document_payload(meta, &fields);
                Ok(Some(FullTextLiveHit {
                    key,
                    score: fulltext_effective_hit_score(score, document_score, options.scorer),
                    fields,
                    sort_key,
                    payload,
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
                let document_score = fulltext_document_score(meta, &filter_fields);
                let payload = fulltext_document_payload(meta, &filter_fields);
                let fields = self.fulltext_json_return_fields(&key, meta, options.dialect)?;
                Ok(Some(FullTextLiveHit {
                    key,
                    score: fulltext_effective_hit_score(score, document_score, options.scorer),
                    fields,
                    sort_key,
                    payload,
                }))
            }
        }
    }
}

fn fulltext_document_score(meta: &FullTextIndexMeta, fields: &[(String, String)]) -> f32 {
    meta.index_options
        .score_field
        .as_deref()
        .and_then(|field| fulltext_field_value(fields, field))
        .and_then(|value| value.parse::<f32>().ok())
        .filter(|score| score.is_finite() && *score >= 0.0)
        .or_else(|| meta.index_options.score.map(|score| score as f32))
        .unwrap_or(1.0)
}

fn fulltext_document_payload(
    meta: &FullTextIndexMeta,
    fields: &[(String, String)],
) -> Option<String> {
    meta.index_options
        .payload_field
        .as_deref()
        .and_then(|field| fulltext_field_value(fields, field))
}

fn fulltext_effective_hit_score(
    relevance_score: f32,
    document_score: f32,
    scorer: FullTextScorer,
) -> f32 {
    match scorer {
        FullTextScorer::DocScore => document_score,
        FullTextScorer::Bm25 | FullTextScorer::Bm25Std => relevance_score * document_score,
        FullTextScorer::DisMax => relevance_score,
    }
}
