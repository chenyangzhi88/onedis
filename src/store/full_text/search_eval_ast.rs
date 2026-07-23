fn fulltext_eval_ast_against_fields(
    ast: &FullTextQueryAst,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> Result<bool, Error> {
    match ast {
        FullTextQueryAst::All => Ok(true),
        FullTextQueryAst::Text(term) => Ok(fulltext_any_text_field_matches(term, fields, meta)),
        FullTextQueryAst::Phrase(phrase) => {
            Ok(fields
                .iter()
                .filter(|(field, _)| {
                    fulltext_schema_field(meta, field)
                        .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                })
                .any(|(_, value)| {
                    fulltext_phrase_matches(
                        value,
                        phrase,
                        &fulltext_effective_document_language(fields, meta, options),
                        options.slop.unwrap_or(0),
                        options.inorder,
                    )
                }))
        }
        FullTextQueryAst::Prefix(prefix) => {
            let prefix = prefix.to_lowercase();
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && fulltext_tokenize(value)
                        .iter()
                        .any(|token| token.starts_with(&prefix))
            }))
        }
        FullTextQueryAst::Wildcard(pattern) => {
            let regex = regex::Regex::new(&fulltext_wildcard_to_regex(pattern))
                .map_err(|_| Error::msg("ERR invalid wildcard pattern"))?;
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && regex.is_match(&value.to_lowercase())
            }))
        }
        FullTextQueryAst::Fuzzy(term) => {
            let term = term.to_lowercase();
            Ok(fields.iter().any(|(field, value)| {
                fulltext_schema_field(meta, field)
                    .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                    && fulltext_tokenize(value)
                        .iter()
                        .any(|token| fulltext_edit_distance(token, &term) <= 1)
            }))
        }
        FullTextQueryAst::Tag { field, values } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            let Some(schema) = fulltext_schema_field(meta, field) else {
                return Ok(false);
            };
            let separator = schema
                .options
                .separator
                .as_deref()
                .and_then(|separator| separator.chars().next())
                .unwrap_or(',');
            let actual = fulltext_split_indexed_tags(
                &value,
                separator,
                schema.options.case_sensitive,
            );
            Ok(values.iter().any(|expected| {
                let expected = if schema.options.case_sensitive {
                    expected.clone()
                } else {
                    expected.to_lowercase()
                };
                actual.iter().any(|actual| actual == &expected)
            }))
        }
        FullTextQueryAst::Numeric { field, min, max } => {
            let Some(value) =
                fulltext_field_value(fields, field).and_then(|value| value.parse::<f64>().ok())
            else {
                return Ok(false);
            };
            Ok(fulltext_numeric_bound_allows(value, *min, true)
                && fulltext_numeric_bound_allows(value, *max, false))
        }
        FullTextQueryAst::Geo {
            field,
            lon,
            lat,
            radius,
            unit,
        } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            fulltext_geo_value_within(&value, *lon, *lat, *radius, unit)
        }
        FullTextQueryAst::GeoShape {
            field,
            relation,
            shape,
        } => {
            let Some(value) = fulltext_field_value(fields, field) else {
                return Ok(false);
            };
            fulltext_geoshape_relation_matches(&value, relation, shape)
        }
        FullTextQueryAst::Field {
            fields: scope,
            expr,
        } => {
            let scoped = fields
                .iter()
                .filter(|(field, _)| scope.iter().any(|scope| scope == field))
                .cloned()
                .collect::<Vec<_>>();
            fulltext_eval_ast_against_fields(expr, &scoped, meta, options)
        }
        FullTextQueryAst::And(children) => {
            for child in children {
                if !fulltext_eval_ast_against_fields(child, fields, meta, options)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        FullTextQueryAst::Or(children) => {
            for child in children {
                if fulltext_eval_ast_against_fields(child, fields, meta, options)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        FullTextQueryAst::Not(child) => Ok(!fulltext_eval_ast_against_fields(
            child, fields, meta, options,
        )?),
        FullTextQueryAst::Optional(_) => Ok(true),
        FullTextQueryAst::Attributed { expr, .. } => {
            fulltext_eval_ast_against_fields(expr, fields, meta, options)
        }
        FullTextQueryAst::VectorRange { .. } | FullTextQueryAst::VectorKnn { .. } => Ok(false),
    }
}

fn fulltext_effective_document_language(
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
    options: &FullTextSearchOptions,
) -> String {
    options
        .language
        .clone()
        .or_else(|| {
            meta.index_options
                .language_field
                .as_deref()
                .and_then(|field| fulltext_field_value(fields, field))
        })
        .or_else(|| meta.index_options.language.clone())
        .unwrap_or_else(|| "english".to_string())
}

fn fulltext_phrase_matches(
    value: &str,
    phrase: &str,
    language: &str,
    slop: u32,
    inorder: bool,
) -> bool {
    let source = fulltext_tokenize_with_language(value, language);
    let query = fulltext_tokenize_with_language(phrase, language);
    if query.is_empty() {
        return true;
    }
    if query.len() == 1 {
        return source.iter().any(|token| token == &query[0]);
    }
    if inorder {
        return fulltext_ordered_phrase_matches(&source, &query, slop as usize);
    }
    fulltext_phrase_with_slop_matches(&source, &query, slop as usize)
}

fn fulltext_ordered_phrase_matches(source: &[String], query: &[String], slop: usize) -> bool {
    for (start, token) in source.iter().enumerate() {
        if token != &query[0] {
            continue;
        }
        let mut previous = start;
        let mut spent = 0usize;
        let mut matched = true;
        for expected in &query[1..] {
            let Some(next) = source
                .iter()
                .enumerate()
                .skip(previous + 1)
                .find_map(|(position, actual)| (actual == expected).then_some(position))
            else {
                matched = false;
                break;
            };
            spent = spent.saturating_add(next.saturating_sub(previous + 1));
            if spent > slop {
                matched = false;
                break;
            }
            previous = next;
        }
        if matched {
            return true;
        }
    }
    false
}

fn fulltext_phrase_with_slop_matches(
    source: &[String],
    query: &[String],
    slop: usize,
) -> bool {
    let mut positions = query
        .iter()
        .enumerate()
        .flat_map(|(query_offset, expected)| {
            source
                .iter()
                .enumerate()
                .filter_map(move |(source_offset, actual)| {
                    (actual == expected).then_some((query_offset, source_offset))
                })
        })
        .collect::<Vec<_>>();
    positions.sort_unstable();
    fulltext_phrase_position_search(&positions, query.len(), 0, None, 0, slop)
}

fn fulltext_phrase_position_search(
    positions: &[(usize, usize)],
    query_len: usize,
    query_offset: usize,
    previous: Option<usize>,
    spent: usize,
    slop: usize,
) -> bool {
    if query_offset == query_len {
        return true;
    }
    positions
        .iter()
        .filter(|(offset, _)| *offset == query_offset)
        .any(|(_, position)| {
            if previous == Some(*position) {
                return false;
            }
            let next_spent = previous.map_or(spent, |previous| {
                spent.saturating_add(previous.saturating_add(1).abs_diff(*position))
            });
            next_spent <= slop
                && fulltext_phrase_position_search(
                    positions,
                    query_len,
                    query_offset + 1,
                    Some(*position),
                    next_spent,
                    slop,
                )
        })
}

fn fulltext_any_text_field_matches(
    term: &str,
    fields: &[(String, String)],
    meta: &FullTextIndexMeta,
) -> bool {
    fields.iter().any(|(field, value)| {
        let Some(schema) = fulltext_schema_field(meta, field) else {
            return false;
        };
        if !matches!(schema.kind, FullTextFieldKind::Text) {
            return false;
        }
        let settings = FullTextTextFieldSettings {
            nostem: schema.options.nostem,
            phonetic: schema.options.phonetic.is_some(),
            with_suffix_trie: schema.options.with_suffix_trie,
            stopwords: meta
                .index_options
                .stopwords
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|word| word.to_lowercase())
                .collect(),
            language: meta
                .index_options
                .language
                .clone()
                .unwrap_or_else(|| "english".to_string()),
            weight: schema.options.weight.unwrap_or(1.0),
        };
        let materialized = fulltext_materialize_text(value, &settings);
        let variants = fulltext_query_term_variants(term, Some(&settings), &HashMap::new());
        variants.iter().any(|variant| {
            fulltext_tokenize(&materialized)
                .iter()
                .any(|token| token == variant)
        })
    })
}

fn fulltext_schema_field<'a>(
    meta: &'a FullTextIndexMeta,
    field: &str,
) -> Option<&'a FullTextFieldSchema> {
    meta.schema
        .iter()
        .find(|schema| schema.name == field || schema.attribute_name() == field)
}
