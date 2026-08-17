use super::*;
impl Db {
    pub(super) fn fulltext_apply_selected_scorer(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        hits: &mut [FullTextLiveHit],
        deadline: Instant,
        fail_on_timeout: bool,
    ) -> Result<(), Error> {
        match options.scorer {
            FullTextScorer::Bm25 => {
                self.fulltext_apply_legacy_bm25(meta, ast, options, hits, deadline, fail_on_timeout)
            }
            FullTextScorer::DisMax => {
                for hit in hits {
                    if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                        break;
                    }
                    let fields = self
                        .fulltext_scoring_fields(meta, &hit.key)?
                        .unwrap_or_else(|| hit.fields.clone());
                    hit.score = fulltext_dismax_score(ast, &fields, meta, options, None)?.max(0.0);
                }
                Ok(())
            }
            FullTextScorer::Bm25Std | FullTextScorer::DocScore => Ok(()),
        }
    }

    pub(super) fn fulltext_apply_legacy_bm25(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        hits: &mut [FullTextLiveHit],
        deadline: Instant,
        fail_on_timeout: bool,
    ) -> Result<(), Error> {
        let language = options
            .language
            .as_deref()
            .or(meta.index_options.language.as_deref())
            .unwrap_or("english");
        let mut terms = Vec::new();
        fulltext_collect_legacy_terms(ast, None, 1.0, language, &mut terms);
        if terms.is_empty() {
            return Ok(());
        }
        if terms.len() > self.fulltext_config_usize("MAXEXPANSIONS", 200)? {
            return Err(Error::msg("ERR fulltext expansion limit exceeded"));
        }

        let mut total_docs = 0usize;
        let mut total_document_len = 0usize;
        let mut document_frequencies = vec![0usize; terms.len()];
        let mut cursor = None;
        'pages: loop {
            let (keys, has_more) = self.fulltext_source_keys_page(meta, cursor.as_deref(), 256)?;
            for key in keys {
                cursor = Some(key.clone());
                if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                    break 'pages;
                }
                let Some(fields) = self.fulltext_scoring_fields(meta, &key)? else {
                    continue;
                };
                if !fulltext_index_filter_matches(meta, &fields)? {
                    continue;
                }
                total_docs = total_docs.saturating_add(1);
                total_document_len = total_document_len
                    .saturating_add(fulltext_text_document_len(&fields, meta, options));
                for (term_index, (term, scope, _)) in terms.iter().enumerate() {
                    if fulltext_weighted_term_frequency(
                        term,
                        scope.as_deref(),
                        &fields,
                        meta,
                        options,
                    ) > 0.0
                    {
                        document_frequencies[term_index] =
                            document_frequencies[term_index].saturating_add(1);
                    }
                }
            }
            if !has_more {
                break;
            }
            if cursor.is_none() {
                return Err(Error::msg("ERR fulltext source scan made no progress"));
            }
        }
        if total_docs == 0 {
            return Ok(());
        }
        let average_document_len = total_document_len as f32 / total_docs as f32;
        let term_stats = terms
            .iter()
            .zip(document_frequencies)
            .map(|((term, scope, query_weight), document_frequency)| {
                let document_frequency = document_frequency.max(1);
                let ratio = 1.0 + (total_docs + 1) as f64 / document_frequency as f64;
                let idf = ratio.log2().floor().max(1.0) as f32;
                (term, scope, *query_weight, idf)
            })
            .collect::<Vec<_>>();
        for hit in hits {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
            let Some(fields) = self.fulltext_scoring_fields(meta, &hit.key)? else {
                hit.score = 0.0;
                continue;
            };
            let mut score = 0.0f32;
            for (term, scope, query_weight, idf) in &term_stats {
                let frequency = fulltext_weighted_term_frequency(
                    term,
                    scope.as_deref(),
                    &fields,
                    meta,
                    options,
                );
                if frequency == 0.0 {
                    continue;
                }
                score += query_weight * idf * frequency
                    / (frequency + 1.2 * (0.5 + 0.5 * average_document_len));
            }
            hit.score = score * fulltext_document_score(meta, &fields);
        }
        Ok(())
    }

    pub(super) fn fulltext_scoring_fields(
        &self,
        meta: &FullTextIndexMeta,
        key: &str,
    ) -> Result<Option<Vec<(String, String)>>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => {
                let fields = self.hash_get_all(key)?;
                Ok((!fields.is_empty()).then_some(fields))
            }
            FullTextSourceType::Json => {
                let Some(root) = self.fulltext_json_root(key)? else {
                    return Ok(None);
                };
                Ok(Some(self.fulltext_json_fields_from_root(&root, meta)?))
            }
        }
    }
}

pub(super) fn fulltext_collect_legacy_terms(
    ast: &FullTextQueryAst,
    scope: Option<&[String]>,
    weight: f32,
    language: &str,
    out: &mut Vec<(String, Option<Vec<String>>, f32)>,
) {
    match ast {
        FullTextQueryAst::Text(value) | FullTextQueryAst::Phrase(value) => {
            for token in fulltext_tokenize_with_language(value, language) {
                fulltext_push_scoring_term(out, token.clone(), scope, weight);
                let stem = fulltext_stem(&token, language);
                if stem != token {
                    fulltext_push_scoring_term(out, stem, scope, weight);
                }
            }
        }
        FullTextQueryAst::Field { fields, expr } => {
            fulltext_collect_legacy_terms(expr, Some(fields), weight, language, out);
        }
        FullTextQueryAst::And(children) | FullTextQueryAst::Or(children) => {
            for child in children {
                fulltext_collect_legacy_terms(child, scope, weight, language, out);
            }
        }
        FullTextQueryAst::Optional(child) => {
            fulltext_collect_legacy_terms(child, scope, weight, language, out);
        }
        FullTextQueryAst::Attributed {
            expr,
            weight: boost,
            ..
        } => {
            fulltext_collect_legacy_terms(
                expr,
                scope,
                weight * boost.unwrap_or(1.0),
                language,
                out,
            );
        }
        FullTextQueryAst::Not(_)
        | FullTextQueryAst::All
        | FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::Missing { .. }
        | FullTextQueryAst::Geo { .. }
        | FullTextQueryAst::GeoShape { .. }
        | FullTextQueryAst::VectorRange { .. }
        | FullTextQueryAst::VectorKnn { .. } => {}
    }
}

pub(super) fn fulltext_push_scoring_term(
    out: &mut Vec<(String, Option<Vec<String>>, f32)>,
    term: String,
    scope: Option<&[String]>,
    weight: f32,
) {
    let scope = scope.map(<[String]>::to_vec);
    if !out
        .iter()
        .any(|candidate| candidate.0 == term && candidate.1 == scope)
    {
        out.push((term, scope, weight));
    }
}

pub(super) fn fulltext_weighted_term_frequency(
    term: &str,
    scope: Option<&[String]>,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> f32 {
    let language = fulltext_effective_document_language(fields, meta, options);
    meta.schema
        .iter()
        .filter(|schema| {
            matches!(schema.kind, FullTextFieldKind::Text)
                && !schema.options.noindex
                && scope.is_none_or(|scope| {
                    scope
                        .iter()
                        .any(|field| field == &schema.name || field == schema.attribute_name())
                })
        })
        .map(|schema| {
            let frequency = fields
                .iter()
                .filter(|(name, _)| name == &schema.name || name == schema.attribute_name())
                .map(|(_, value)| {
                    fulltext_tokenize_with_language(value, &language)
                        .into_iter()
                        .filter(|token| {
                            token == term
                                || (!schema.options.nostem
                                    && fulltext_stem(token, &language) == term)
                        })
                        .count()
                })
                .sum::<usize>() as f32;
            frequency * schema.options.weight.unwrap_or(1.0)
        })
        .sum()
}

pub(super) fn fulltext_text_document_len(
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> usize {
    let language = fulltext_effective_document_language(fields, meta, options);
    meta.schema
        .iter()
        .filter(|schema| matches!(schema.kind, FullTextFieldKind::Text) && !schema.options.noindex)
        .flat_map(|schema| {
            fields
                .iter()
                .filter(move |(name, _)| name == &schema.name || name == schema.attribute_name())
                .map(|(_, value)| fulltext_tokenize_with_language(value, &language).len())
        })
        .sum::<usize>()
        .max(1)
}

pub(super) fn fulltext_dismax_score(
    ast: &FullTextQueryAst,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
    scope: Option<&[String]>,
) -> Result<f32, Error> {
    match ast {
        FullTextQueryAst::All => Ok(1.0),
        FullTextQueryAst::Text(term) => Ok(fulltext_tokenize_with_language(
            term,
            options
                .language
                .as_deref()
                .or(meta.index_options.language.as_deref())
                .unwrap_or("english"),
        )
        .iter()
        .map(|term| fulltext_weighted_term_frequency(term, scope, fields, meta, options))
        .sum()),
        FullTextQueryAst::Phrase(phrase) => Ok(
            if fulltext_eval_ast_against_fields(ast, fields, meta, options)? {
                fulltext_tokenize(phrase).len().max(1) as f32
            } else {
                0.0
            },
        ),
        FullTextQueryAst::And(children) => {
            let mut score = 0.0;
            for child in children {
                score += fulltext_dismax_score(child, fields, meta, options, scope)?;
            }
            Ok(score)
        }
        FullTextQueryAst::Or(children) => {
            let mut score = 0.0f32;
            for child in children {
                score = score.max(fulltext_dismax_score(child, fields, meta, options, scope)?);
            }
            Ok(score)
        }
        FullTextQueryAst::Field {
            fields: field_scope,
            expr,
        } => fulltext_dismax_score(expr, fields, meta, options, Some(field_scope)),
        FullTextQueryAst::Optional(child) => {
            fulltext_dismax_score(child, fields, meta, options, scope)
        }
        FullTextQueryAst::Attributed { expr, weight, .. } => {
            Ok(fulltext_dismax_score(expr, fields, meta, options, scope)? * weight.unwrap_or(1.0))
        }
        FullTextQueryAst::Not(_) => Ok(0.0),
        FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::Missing { .. }
        | FullTextQueryAst::Geo { .. }
        | FullTextQueryAst::GeoShape { .. } => Ok(
            if fulltext_eval_ast_against_fields(ast, fields, meta, options)? {
                1.0
            } else {
                0.0
            },
        ),
        FullTextQueryAst::VectorRange { .. } | FullTextQueryAst::VectorKnn { .. } => Ok(0.0),
    }
}
