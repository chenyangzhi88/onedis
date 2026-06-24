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
            let phrase = phrase.to_lowercase();
            Ok(fields
                .iter()
                .filter(|(field, _)| {
                    fulltext_schema_field(meta, field)
                        .is_some_and(|schema| matches!(schema.kind, FullTextFieldKind::Text))
                })
                .any(|(_, value)| value.to_lowercase().contains(&phrase)))
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
            Ok(values.iter().any(|expected| {
                split_tag_values(&value)
                    .iter()
                    .any(|actual| actual.eq_ignore_ascii_case(expected))
                    || value.eq_ignore_ascii_case(expected)
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
