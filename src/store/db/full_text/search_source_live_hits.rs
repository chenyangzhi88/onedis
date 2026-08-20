use super::*;
impl Db {
    pub(super) fn fulltext_filter_fields_from_source(
        &self,
        meta: &FullTextIndexMeta,
        key: &str,
    ) -> Result<Option<Vec<(String, String)>>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => {
                let fields = self.hash_get_all(key)?;
                Ok((!fields.is_empty()).then_some(fields))
            }
            FullTextSourceType::Json => self.fulltext_json_fields(key, meta),
        }
    }

    pub(super) fn fulltext_live_hits_from_source(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        candidates: Vec<FullTextSearchHit>,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let meta_keys = candidates
            .iter()
            .map(|candidate| self.mk(&candidate.key))
            .collect::<Vec<_>>();
        let raw_metas = self.store.multi_get_raw(&meta_keys)?;
        let expected_type = match meta.source_type {
            FullTextSourceType::Hash => TYPE_HASH,
            FullTextSourceType::Json => TYPE_JSON,
        };
        let now = current_fulltext_millis();
        let projected_hash_fields = fulltext_hash_projection(meta, options);
        let mut live = Vec::with_capacity(candidates.len());
        for (candidate, raw_meta) in candidates.into_iter().zip(raw_metas) {
            let Some(header) = raw_meta.as_deref().and_then(decode_meta_header) else {
                continue;
            };
            if header.type_tag != expected_type {
                continue;
            }
            if header.expire_ms != 0 && header.expire_ms <= now {
                self.expire_if_needed(&candidate.key)?;
                continue;
            }
            match meta.source_type {
                FullTextSourceType::Hash => {
                    let fields = if let Some(projected) = &projected_hash_fields {
                        let mut fields = Vec::with_capacity(projected.len());
                        for (source, output) in projected {
                            let Some(value) =
                                self.hash_live_field_value(&candidate.key, header.version, source)?
                            else {
                                continue;
                            };
                            if let Ok(value) = String::from_utf8(value) {
                                fields.push((output.clone(), value));
                            }
                        }
                        fields
                    } else {
                        self.hash_live_entries_raw(&candidate.key, header.version)?
                            .into_iter()
                            .filter_map(|(field, value)| {
                                Some((
                                    String::from_utf8(field).ok()?,
                                    String::from_utf8(value).ok()?,
                                ))
                            })
                            .collect::<Vec<_>>()
                    };
                    if let Some(hit) = self.fulltext_hash_live_hit(
                        meta,
                        options,
                        candidate.key,
                        candidate.score,
                        fields,
                    )? {
                        live.push(hit);
                    }
                }
                FullTextSourceType::Json => {
                    if let Some(hit) = self.fulltext_live_hit_from_source(
                        meta,
                        options,
                        candidate.key,
                        candidate.score,
                    )? {
                        live.push(hit);
                    }
                }
            }
        }
        Ok(live)
    }

    pub(super) fn fulltext_exact_filter_hits(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        limits: FullTextSearchLimits,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        if let Some(keys) = options.in_keys.as_ref() {
            let mut live = Vec::new();
            let mut live_bytes = 0usize;
            for key in keys {
                if fulltext_search_timeout_reached(
                    limits.timeout.at,
                    limits.timeout.fail_on_timeout,
                )? {
                    return Ok(live);
                }
                let Some(hit) =
                    self.fulltext_live_hit_from_source(meta, options, key.clone(), 1.0)?
                else {
                    continue;
                };
                if fulltext_index_filter_matches(meta, &hit.fields)?
                    && fulltext_eval_ast_against_fields(ast, &hit.fields, meta, options)?
                {
                    if live.len() >= limits.result_cap {
                        return Err(Error::msg("ERR fulltext result limit exceeded"));
                    }
                    live_bytes = live_bytes.saturating_add(estimate_fulltext_live_hit_bytes(&hit));
                    if live_bytes > limits.reader_budget {
                        return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
                    }
                    live.push(hit);
                }
            }
            return Ok(live);
        }
        let mut live = Vec::new();
        let mut live_bytes = 0usize;
        let mut cursor = None;
        loop {
            let (keys, has_more) = self.fulltext_source_keys_page(meta, cursor.as_deref(), 256)?;
            for key in keys {
                cursor = Some(key.clone());
                if fulltext_search_timeout_reached(
                    limits.timeout.at,
                    limits.timeout.fail_on_timeout,
                )? {
                    return Ok(live);
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
                    if live.len() >= limits.result_cap {
                        return Err(Error::msg("ERR fulltext result limit exceeded"));
                    }
                    live_bytes = live_bytes.saturating_add(estimate_fulltext_live_hit_bytes(&hit));
                    if live_bytes > limits.reader_budget {
                        return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
                    }
                    live.push(hit);
                }
            }
            if !has_more {
                return Ok(live);
            }
            if cursor.is_none() {
                return Err(Error::msg("ERR fulltext source scan made no progress"));
            }
        }
    }

    pub(super) fn fulltext_live_hit_from_source(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        key: String,
        score: f32,
    ) -> Result<Option<FullTextLiveHit>, Error> {
        self.expire_if_needed(&key)?;
        let expected_type = match meta.source_type {
            FullTextSourceType::Hash => TYPE_HASH,
            FullTextSourceType::Json => TYPE_JSON,
        };
        if self
            .store
            .get_raw(&self.mk(&key))?
            .and_then(|raw| decode_meta_header(&raw))
            .is_none_or(|header| header.type_tag != expected_type)
        {
            return Ok(None);
        }
        match meta.source_type {
            FullTextSourceType::Hash => {
                let fields = self.hash_get_all(&key)?;
                self.fulltext_hash_live_hit(meta, options, key, score, fields)
            }
            FullTextSourceType::Json => {
                let Some(root) = self.fulltext_json_root(&key)? else {
                    return Ok(None);
                };
                let filter_fields = self.fulltext_json_fields_from_root(&root, meta)?;
                if !fulltext_fields_match_filters(&filter_fields, &options.filters) {
                    return Ok(None);
                }
                if !fulltext_fields_match_geo_filters(&filter_fields, &options.geo_filters)? {
                    return Ok(None);
                }
                let sort_key = options.sort_by.as_ref().and_then(|sort_by| {
                    fulltext_sort_field_value(&filter_fields, meta, &sort_by.field)
                });
                let document_score = fulltext_document_score(meta, &filter_fields);
                let payload = fulltext_document_payload(meta, &filter_fields);
                let fields =
                    self.fulltext_json_return_fields_from_root(&root, meta, options.dialect)?;
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

    fn fulltext_hash_live_hit(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        key: String,
        score: f32,
        fields: Vec<(String, String)>,
    ) -> Result<Option<FullTextLiveHit>, Error> {
        if fields.is_empty()
            || !fulltext_fields_match_filters(&fields, &options.filters)
            || !fulltext_fields_match_geo_filters(&fields, &options.geo_filters)?
        {
            return Ok(None);
        }
        let sort_key = options
            .sort_by
            .as_ref()
            .and_then(|sort_by| fulltext_sort_field_value(&fields, meta, &sort_by.field));
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
}

fn fulltext_hash_projection(
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> Option<Vec<(String, String)>> {
    let requested = options.return_fields.as_ref()?;
    if options.no_content
        || !options.filters.is_empty()
        || !options.geo_filters.is_empty()
        || options.sort_by.is_some()
        || options.summarize.is_some()
        || options.highlight.is_some()
        || matches!(options.scorer, FullTextScorer::DocScore)
        || meta.index_options.score_field.is_some()
        || options.with_payloads
    {
        return None;
    }
    let mut projection = Vec::with_capacity(requested.len());
    for field in requested {
        let source = meta
            .schema
            .iter()
            .find(|schema| schema.attribute_name() == field.identifier)
            .map(|schema| schema.name.clone())
            .unwrap_or_else(|| field.identifier.clone());
        if !projection
            .iter()
            .any(|(_, output)| output == &field.identifier)
        {
            projection.push((source, field.identifier.clone()));
        }
    }
    Some(projection)
}

pub(super) fn fulltext_document_score(
    meta: &FullTextIndexMeta,
    fields: &[(String, String)],
) -> f32 {
    meta.index_options
        .score_field
        .as_deref()
        .and_then(|field| fulltext_field_value(fields, field))
        .and_then(|value| value.parse::<f32>().ok())
        .filter(|score| score.is_finite() && *score >= 0.0)
        .or_else(|| meta.index_options.score.map(|score| score as f32))
        .unwrap_or(1.0)
}

pub(super) fn fulltext_document_payload(
    meta: &FullTextIndexMeta,
    fields: &[(String, String)],
) -> Option<String> {
    meta.index_options
        .payload_field
        .as_deref()
        .and_then(|field| fulltext_field_value(fields, field))
}

pub(super) fn fulltext_effective_hit_score(
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
