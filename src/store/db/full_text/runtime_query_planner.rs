use super::*;

pub(super) struct FullTextLevenshteinDfa(DFA);

impl Automaton for FullTextLevenshteinDfa {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        matches!(self.0.distance(*state), LevenshteinDistance::Exact(_))
    }

    fn can_match(&self, state: &Self::State) -> bool {
        *state != SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

impl FullTextRuntime {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn build_query(
        &self,
        query_text: &str,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let query_text = query_text.trim();
        let query = if query_text == "*" {
            Box::new(AllQuery) as Box<dyn Query>
        } else {
            let ast = FullTextQueryParser::new(query_text, options.dialect).parse()?;
            if matches!(ast, FullTextQueryAst::All) {
                Box::new(AllQuery) as Box<dyn Query>
            } else {
                self.plan_query(&ast, options.in_fields.as_deref(), options)?
            }
        };
        self.apply_search_filters(query, options)
    }

    pub(super) fn plan_query(
        &self,
        ast: &FullTextQueryAst,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        match ast {
            FullTextQueryAst::All => Ok(Box::new(AllQuery)),
            FullTextQueryAst::Text(term) => self.plan_text_query(term, field_scope, options),
            FullTextQueryAst::Phrase(phrase) => {
                self.plan_phrase_query(phrase, field_scope, options)
            }
            FullTextQueryAst::Prefix(prefix) => {
                self.plan_prefix_query(prefix, field_scope, options)
            }
            FullTextQueryAst::Wildcard(pattern) => {
                self.plan_wildcard_query(pattern, field_scope, options)
            }
            FullTextQueryAst::Fuzzy(term) => self.plan_fuzzy_query(term, field_scope, options),
            FullTextQueryAst::Tag { field, values } => self.plan_tag_query(field, values),
            FullTextQueryAst::Numeric { field, min, max } => {
                self.plan_numeric_query(field, *min, *max)
            }
            FullTextQueryAst::Geo {
                field,
                lon,
                lat,
                radius,
                unit,
            } => self.plan_geo_query(field, *lon, *lat, *radius, unit),
            FullTextQueryAst::GeoShape {
                field,
                relation,
                shape,
            } => self.plan_geoshape_query(field, relation, shape),
            FullTextQueryAst::Missing { field } => self.plan_missing_query(field),
            FullTextQueryAst::VectorKnn {
                filter,
                k,
                field,
                blob_param,
            } => {
                let _ = (filter, k, field, blob_param);
                Err(Error::msg(
                    "ERR fulltext vector query execution is not implemented",
                ))
            }
            FullTextQueryAst::VectorRange {
                field,
                radius,
                blob_param,
            } => {
                let _ = (field, radius, blob_param);
                Err(Error::msg(
                    "ERR fulltext vector query execution is not implemented",
                ))
            }
            FullTextQueryAst::Field { fields, expr } => {
                if self.no_fields {
                    return Err(Error::msg(
                        "ERR field-specific queries are disabled for this fulltext index",
                    ));
                }
                self.plan_query(expr, Some(fields), options)
            }
            FullTextQueryAst::And(children) => self.plan_and(children, field_scope, options),
            FullTextQueryAst::Or(children) => {
                self.plan_boolean(children, Occur::Should, field_scope, options)
            }
            FullTextQueryAst::Not(child) => Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                (
                    Occur::MustNot,
                    self.plan_query(child, field_scope, options)?,
                ),
            ]))),
            FullTextQueryAst::Optional(child) => Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                (Occur::Should, self.plan_query(child, field_scope, options)?),
            ]))),
            FullTextQueryAst::Attributed {
                expr,
                weight,
                slop,
                inorder,
                phonetic,
            } => {
                let mut attributed_options = options.clone();
                if let Some(slop) = slop {
                    attributed_options.slop = Some(*slop);
                }
                if let Some(inorder) = inorder {
                    attributed_options.inorder = *inorder;
                }
                if let Some(phonetic) = phonetic {
                    attributed_options.phonetic = Some(*phonetic);
                }
                let query = self.plan_query(expr, field_scope, &attributed_options)?;
                if let Some(weight) = weight {
                    Ok(Box::new(BoostQuery::new(query, *weight)))
                } else {
                    Ok(query)
                }
            }
        }
    }

    pub(super) fn plan_boolean(
        &self,
        children: &[FullTextQueryAst],
        occur: Occur,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if children.is_empty() {
            return Ok(Box::new(AllQuery));
        }
        if children.len() > self.max_expansions {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        if children.len() == 1 {
            return self.plan_query(&children[0], field_scope, options);
        }
        let queries = children
            .iter()
            .map(|child| self.plan_query(child, field_scope, options))
            .collect::<Result<Vec<_>, Error>>()?;
        if occur == Occur::Should && matches!(options.scorer, FullTextScorer::DisMax) {
            return Ok(Box::new(DisjunctionMaxQuery::new(queries)));
        }
        Ok(Box::new(BooleanQuery::new(
            queries.into_iter().map(|query| (occur, query)).collect(),
        )))
    }

    pub(super) fn plan_and(
        &self,
        children: &[FullTextQueryAst],
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if children.is_empty() {
            return Ok(Box::new(AllQuery));
        }
        if children.len() > self.max_expansions {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        let mut clauses = Vec::with_capacity(children.len() + 1);
        let mut has_required = false;
        for child in children {
            match child {
                FullTextQueryAst::Optional(optional) => clauses.push((
                    Occur::Should,
                    self.plan_query(optional, field_scope, options)?,
                )),
                FullTextQueryAst::Not(excluded) => clauses.push((
                    Occur::MustNot,
                    self.plan_query(excluded, field_scope, options)?,
                )),
                _ => {
                    has_required = true;
                    clauses.push((Occur::Must, self.plan_query(child, field_scope, options)?));
                }
            }
        }
        if !has_required {
            clauses.push((Occur::Must, Box::new(AllQuery)));
        }
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    pub(super) fn plan_text_query(
        &self,
        query_text: &str,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        if let Some(term) = fulltext_simple_query_term(query_text) {
            return self.or_field_queries(
                fields.into_iter().map(|field| {
                    let settings = self.text_field_settings.get(&field);
                    let effective = self.effective_text_settings(settings, options);
                    let variants =
                        fulltext_query_term_variants(term, Some(&effective), &HashMap::new());
                    if variants.len() > self.max_expansions {
                        return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
                    }
                    let query_fields = if options.verbatim {
                        vec![field]
                    } else if options.no_stopwords {
                        vec![self.text_variant_field(field), field]
                    } else {
                        vec![self.text_variant_field(field)]
                    };
                    let query = Box::new(BooleanQuery::new(
                        variants
                            .into_iter()
                            .flat_map(|variant| {
                                query_fields.iter().copied().map(move |query_field| {
                                    (
                                        Occur::Should,
                                        Box::new(TermQuery::new(
                                            Term::from_field_text(query_field, &variant),
                                            IndexRecordOption::Basic,
                                        ))
                                            as Box<dyn Query>,
                                    )
                                })
                            })
                            .collect(),
                    )) as Box<dyn Query>;
                    Ok(self.boost_text_field(query, field))
                }),
                options.scorer,
            );
        }
        let parser = QueryParser::for_index(&self.index, fields);
        Ok(parser.parse_query(query_text)?)
    }

    pub(super) fn plan_phrase_query(
        &self,
        phrase: &str,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if !self.has_positions {
            return Err(Error::msg(
                "ERR phrase queries require positions in this fulltext index",
            ));
        }
        let fields = self.text_fields_for_scope(field_scope)?;
        if phrase.is_empty() {
            return self.plan_empty_query(field_scope, FullTextFieldKind::Text);
        }
        self.or_field_queries(
            fields.into_iter().map(|field| {
                let settings =
                    self.effective_text_settings(self.text_field_settings.get(&field), options);
                let tokens = fulltext_phrase_tokens(phrase, &settings.language);
                if tokens.is_empty() {
                    return Ok(Box::new(AllQuery) as Box<dyn Query>);
                }
                if tokens.len() > self.max_expansions {
                    return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
                }
                if tokens.len() == 1 {
                    let query = Box::new(TermQuery::new(
                        Term::from_field_text(field, &tokens[0]),
                        IndexRecordOption::WithFreqsAndPositions,
                    )) as Box<dyn Query>;
                    return Ok(self.boost_text_field(query, field));
                }
                if options.slop.unwrap_or(0) > 0 && !options.inorder {
                    let clauses = tokens
                        .into_iter()
                        .map(|token| {
                            (
                                Occur::Must,
                                Box::new(TermQuery::new(
                                    Term::from_field_text(field, &token),
                                    IndexRecordOption::WithFreqsAndPositions,
                                )) as Box<dyn Query>,
                            )
                        })
                        .collect();
                    return Ok(self.boost_text_field(Box::new(BooleanQuery::new(clauses)), field));
                }
                let terms = tokens
                    .into_iter()
                    .enumerate()
                    .map(|(offset, token)| (offset, Term::from_field_text(field, &token)))
                    .collect();
                let query = Box::new(PhraseQuery::new_with_offset_and_slop(
                    terms,
                    options.slop.unwrap_or(0),
                )) as Box<dyn Query>;
                Ok(self.boost_text_field(query, field))
            }),
            options.scorer,
        )
    }

    pub(super) fn plan_wildcard_query(
        &self,
        pattern: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let regex = fulltext_wildcard_to_regex(pattern);
        if self.max_expansions == 0 {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        let automaton =
            FstRegex::new(&regex).map_err(|_| Error::msg("ERR invalid wildcard query"))?;
        let variant_fields = fields
            .iter()
            .map(|field| self.text_variant_field(*field))
            .collect::<Vec<_>>();
        let expanded = self.expand_automaton_terms(
            &variant_fields,
            self.max_expansions,
            &format!("wildcard:{regex}"),
            &automaton,
        )?;
        if expanded.unique_term_count <= 32 {
            self.expanded_terms_query(&fields, &expanded, _options.scorer)
        } else {
            self.regex_fields_query(&fields, &regex, _options.scorer)
        }
    }

    pub(super) fn plan_prefix_query(
        &self,
        prefix: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        if prefix.chars().count() < self.min_prefix {
            return Err(Error::msg("ERR fulltext prefix is too short"));
        }
        if self.max_prefix_expansions == 0 {
            return Err(Error::msg("ERR fulltext prefix expansion limit exceeded"));
        }
        let prefix = prefix.to_ascii_lowercase();
        let variant_fields = fields
            .iter()
            .map(|field| self.text_variant_field(*field))
            .collect::<Vec<_>>();
        let expanded = self.expand_prefix_terms(
            &variant_fields,
            self.max_prefix_expansions as usize,
            &prefix,
        )?;
        if expanded.unique_term_count <= 32 {
            self.expanded_terms_query(&fields, &expanded, _options.scorer)
        } else {
            let regex = fulltext_wildcard_to_regex(&format!("{prefix}*"));
            self.regex_fields_query(&fields, &regex, _options.scorer)
        }
    }

    pub(super) fn plan_fuzzy_query(
        &self,
        term: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        if self.max_expansions == 0 {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        let normalized = term.to_lowercase();
        let automaton = FullTextLevenshteinDfa(
            LevenshteinAutomatonBuilder::new(1, true).build_dfa(&normalized),
        );
        let variant_fields = fields
            .iter()
            .map(|field| self.text_variant_field(*field))
            .collect::<Vec<_>>();
        let expanded = self.expand_automaton_terms(
            &variant_fields,
            self.max_expansions,
            &format!("fuzzy:{normalized}"),
            &automaton,
        )?;
        self.expanded_terms_query(&fields, &expanded, _options.scorer)
    }

    pub(super) fn plan_tag_query(
        &self,
        field: &str,
        values: &[String],
    ) -> Result<Box<dyn Query>, Error> {
        let Some((tantivy_field, FullTextFieldKind::Tag)) = self.query_fields.get(field) else {
            return Err(Error::msg("ERR invalid tag field"));
        };
        let settings = self
            .tag_field_settings
            .get(tantivy_field)
            .cloned()
            .unwrap_or(FullTextTagFieldSettings {
                separator: ',',
                case_sensitive: false,
            });
        if values.len() == 1 && values[0].is_empty() {
            let fields = [field.to_string()];
            return self.plan_empty_query(Some(&fields), FullTextFieldKind::Tag);
        }
        if values.len() > self.max_expansions {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        self.or_field_queries(
            values.iter().map(|value| {
                let value = if settings.case_sensitive {
                    value.clone()
                } else {
                    value.to_lowercase()
                };
                Ok(Box::new(TermQuery::new(
                    Term::from_field_text(*tantivy_field, &value),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>)
            }),
            FullTextScorer::Bm25Std,
        )
    }

    pub(super) fn plan_missing_query(&self, field: &str) -> Result<Box<dyn Query>, Error> {
        let marker = self
            .presence_fields
            .get(field)
            .ok_or_else(|| Error::msg("ERR field does not have INDEXMISSING"))?;
        Ok(Box::new(BooleanQuery::new(vec![
            (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
            (
                Occur::MustNot,
                Box::new(TermQuery::new(
                    Term::from_field_u64(*marker, 1),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            ),
        ])))
    }

    pub(super) fn plan_empty_query(
        &self,
        field_scope: Option<&[String]>,
        expected_kind: FullTextFieldKind,
    ) -> Result<Box<dyn Query>, Error> {
        let names = match field_scope {
            Some(fields) => fields.to_vec(),
            None => self
                .query_fields
                .iter()
                .filter_map(|(name, (_, kind))| (*kind == expected_kind).then_some(name.clone()))
                .collect(),
        };
        let mut markers = names
            .into_iter()
            .filter_map(|name| self.empty_fields.get(&name).copied())
            .collect::<Vec<_>>();
        markers.sort_by_key(|field| field.field_id());
        markers.dedup();
        if markers.is_empty() {
            return Err(Error::msg("ERR field does not have INDEXEMPTY"));
        }
        self.or_field_queries(
            markers.into_iter().map(|marker| {
                Ok(Box::new(TermQuery::new(
                    Term::from_field_u64(marker, 1),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>)
            }),
            FullTextScorer::Bm25Std,
        )
    }

    pub(super) fn plan_numeric_query(
        &self,
        field: &str,
        min: FullTextNumericBound,
        max: FullTextNumericBound,
    ) -> Result<Box<dyn Query>, Error> {
        let Some((tantivy_field, FullTextFieldKind::Numeric)) = self.query_fields.get(field) else {
            return Err(Error::msg("ERR invalid numeric field"));
        };
        let lower = numeric_bound_to_tantivy(*tantivy_field, min, true);
        let upper = numeric_bound_to_tantivy(*tantivy_field, max, false);
        Ok(Box::new(RangeQuery::new(lower, upper)))
    }

    pub(super) fn plan_geo_query(
        &self,
        field: &str,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &str,
    ) -> Result<Box<dyn Query>, Error> {
        let (lon_field, lat_field) = self
            .geo_fields
            .get(field)
            .ok_or_else(|| Error::msg("ERR invalid geo field"))?;
        let radius_meters = radius * fulltext_geo_unit_meters(unit)?;
        let lat_delta = (radius_meters / 6_371_000.0).to_degrees();
        let lon_delta = if lat.abs() + lat_delta >= 90.0 {
            180.0
        } else {
            (radius_meters / (6_371_000.0 * lat.to_radians().cos().abs().max(1e-12)))
                .to_degrees()
                .min(180.0)
        };
        let lat_query = fulltext_f64_range_query(
            *lat_field,
            (lat - lat_delta).max(-90.0),
            (lat + lat_delta).min(90.0),
        );
        let lon_query: Box<dyn Query> = if lon_delta >= 180.0 {
            fulltext_f64_range_query(*lon_field, -180.0, 180.0)
        } else {
            let min = lon - lon_delta;
            let max = lon + lon_delta;
            if min < -180.0 {
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Should,
                        fulltext_f64_range_query(*lon_field, min + 360.0, 180.0),
                    ),
                    (
                        Occur::Should,
                        fulltext_f64_range_query(*lon_field, -180.0, max),
                    ),
                ]))
            } else if max > 180.0 {
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Should,
                        fulltext_f64_range_query(*lon_field, min, 180.0),
                    ),
                    (
                        Occur::Should,
                        fulltext_f64_range_query(*lon_field, -180.0, max - 360.0),
                    ),
                ]))
            } else {
                fulltext_f64_range_query(*lon_field, min, max)
            }
        };
        Ok(Box::new(BooleanQuery::new(vec![
            (Occur::Must, lon_query),
            (Occur::Must, lat_query),
        ])))
    }

    pub(super) fn plan_geoshape_query(
        &self,
        field: &str,
        relation: &str,
        shape: &str,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self
            .geoshape_fields
            .get(field)
            .ok_or_else(|| Error::msg("ERR invalid geoshape field"))?;
        let bounds = fulltext_geometry_bounds(&parse_fulltext_wkt(shape)?)
            .ok_or_else(|| Error::msg("ERR invalid WKT"))?;
        let queries = match relation.to_ascii_uppercase().as_str() {
            "WITHIN" => vec![
                fulltext_f64_lower_query(fields[0], bounds.0),
                fulltext_f64_upper_query(fields[1], bounds.1),
                fulltext_f64_lower_query(fields[2], bounds.2),
                fulltext_f64_upper_query(fields[3], bounds.3),
            ],
            "CONTAINS" => vec![
                fulltext_f64_upper_query(fields[0], bounds.0),
                fulltext_f64_lower_query(fields[1], bounds.1),
                fulltext_f64_upper_query(fields[2], bounds.2),
                fulltext_f64_lower_query(fields[3], bounds.3),
            ],
            _ => return Err(Error::msg("ERR invalid GEOSHAPE relation")),
        };
        Ok(Box::new(BooleanQuery::new(
            queries
                .into_iter()
                .map(|query| (Occur::Must, query))
                .collect(),
        )))
    }

    pub(super) fn text_fields_for_scope(
        &self,
        field_scope: Option<&[String]>,
    ) -> Result<Vec<Field>, Error> {
        match field_scope {
            Some(fields) => fields
                .iter()
                .map(|field| match self.query_fields.get(field) {
                    Some((tantivy_field, FullTextFieldKind::Text)) => Ok(*tantivy_field),
                    Some(_) => Err(Error::msg("ERR invalid text field")),
                    None => Err(Error::msg("ERR invalid text field")),
                })
                .collect(),
            None => Ok(self.text_fields.clone()),
        }
    }

    pub(super) fn expand_prefix_terms(
        &self,
        fields: &[Field],
        limit: usize,
        prefix: &str,
    ) -> Result<Arc<FullTextExpansionCacheEntry>, Error> {
        let cache_key = format!(
            "prefix:{}:{prefix}",
            fields
                .iter()
                .map(|field| field.field_id().to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        if let Some(cached) = self.cached_expansion_terms(&cache_key)? {
            global_metrics().record_fulltext_expanded_terms(cached.unique_term_count);
            return self.validate_cached_expansion(cached, limit);
        }
        let searcher = self.reader.searcher();
        let mut unique = HashSet::new();
        let mut matched = HashSet::new();
        for segment in searcher.segment_readers() {
            for field in fields {
                let inverted = segment.inverted_index(*field)?;
                let mut stream = inverted
                    .terms()
                    .range()
                    .ge(prefix.as_bytes())
                    .into_stream()?;
                while stream.advance() {
                    let Ok(term) = std::str::from_utf8(stream.key()) else {
                        continue;
                    };
                    if !term.starts_with(prefix) {
                        break;
                    }
                    unique.insert(term.to_string());
                    matched.insert((*field, term.to_string()));
                    if unique.len() > limit {
                        let entry = Arc::new(FullTextExpansionCacheEntry {
                            terms: Vec::new(),
                            unique_term_count: unique.len(),
                        });
                        self.cache_expansion_terms(cache_key, entry)?;
                        global_metrics().record_fulltext_expanded_terms(unique.len());
                        return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
                    }
                }
            }
        }
        let entry = Arc::new(FullTextExpansionCacheEntry {
            terms: matched.into_iter().collect(),
            unique_term_count: unique.len(),
        });
        self.cache_expansion_terms(cache_key, entry.clone())?;
        global_metrics().record_fulltext_expanded_terms(entry.unique_term_count);
        Ok(entry)
    }

    pub(super) fn expand_automaton_terms<A: Automaton>(
        &self,
        fields: &[Field],
        limit: usize,
        query_key: &str,
        automaton: &A,
    ) -> Result<Arc<FullTextExpansionCacheEntry>, Error>
    where
        A::State: Clone,
    {
        let cache_key = format!(
            "automaton:{}:{query_key}",
            fields
                .iter()
                .map(|field| field.field_id().to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        if let Some(cached) = self.cached_expansion_terms(&cache_key)? {
            global_metrics().record_fulltext_expanded_terms(cached.unique_term_count);
            return self.validate_cached_expansion(cached, limit);
        }
        let searcher = self.reader.searcher();
        let mut unique = HashSet::new();
        let mut matched = HashSet::new();
        for segment in searcher.segment_readers() {
            for field in fields {
                let inverted = segment.inverted_index(*field)?;
                let mut stream = inverted.terms().search(automaton).into_stream()?;
                while stream.advance() {
                    let Ok(term) = std::str::from_utf8(stream.key()) else {
                        continue;
                    };
                    unique.insert(term.to_string());
                    matched.insert((*field, term.to_string()));
                    if unique.len() > limit {
                        let entry = Arc::new(FullTextExpansionCacheEntry {
                            terms: Vec::new(),
                            unique_term_count: unique.len(),
                        });
                        self.cache_expansion_terms(cache_key, entry)?;
                        global_metrics().record_fulltext_expanded_terms(unique.len());
                        return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
                    }
                }
            }
        }
        let entry = Arc::new(FullTextExpansionCacheEntry {
            terms: matched.into_iter().collect(),
            unique_term_count: unique.len(),
        });
        self.cache_expansion_terms(cache_key, entry.clone())?;
        global_metrics().record_fulltext_expanded_terms(entry.unique_term_count);
        Ok(entry)
    }

    fn cached_expansion_terms(
        &self,
        key: &str,
    ) -> Result<Option<Arc<FullTextExpansionCacheEntry>>, Error> {
        Ok(self
            .expansion_terms
            .lock()
            .map_err(|_| Error::msg("ERR fulltext expansion cache lock poisoned"))?
            .get(key)
            .cloned())
    }

    fn cache_expansion_terms(
        &self,
        key: String,
        terms: Arc<FullTextExpansionCacheEntry>,
    ) -> Result<(), Error> {
        const MAX_EXPANSION_CACHE_ENTRIES: usize = 1_024;
        let mut cache = self
            .expansion_terms
            .lock()
            .map_err(|_| Error::msg("ERR fulltext expansion cache lock poisoned"))?;
        if cache.len() >= MAX_EXPANSION_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(key, terms);
        Ok(())
    }

    fn validate_cached_expansion(
        &self,
        cached: Arc<FullTextExpansionCacheEntry>,
        limit: usize,
    ) -> Result<Arc<FullTextExpansionCacheEntry>, Error> {
        if cached.unique_term_count > limit {
            Err(Error::msg("ERR fulltext query expansion limit exceeded"))
        } else {
            Ok(cached)
        }
    }

    fn expanded_terms_query(
        &self,
        fields: &[Field],
        expanded: &FullTextExpansionCacheEntry,
        scorer: FullTextScorer,
    ) -> Result<Box<dyn Query>, Error> {
        if expanded.terms.is_empty() {
            return Ok(Box::new(EmptyQuery));
        }
        self.or_field_queries(
            fields.iter().filter_map(|field| {
                let query_field = self.text_variant_field(*field);
                let mut queries = expanded
                    .terms
                    .iter()
                    .filter(|(expanded_field, _)| *expanded_field == query_field)
                    .map(|(_, term)| {
                        (
                            Occur::Should,
                            Box::new(TermQuery::new(
                                Term::from_field_text(query_field, term),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        )
                    })
                    .collect::<Vec<_>>();
                if queries.is_empty() {
                    return None;
                }
                let query = if queries.len() == 1 {
                    queries.pop().expect("one expansion").1
                } else {
                    Box::new(BooleanQuery::new(queries)) as Box<dyn Query>
                };
                Some(Ok(self.boost_text_field(query, *field)))
            }),
            scorer,
        )
    }

    fn regex_fields_query(
        &self,
        fields: &[Field],
        regex: &str,
        scorer: FullTextScorer,
    ) -> Result<Box<dyn Query>, Error> {
        self.or_field_queries(
            fields.iter().map(|field| {
                let query_field = self.text_variant_field(*field);
                let query =
                    Box::new(RegexQuery::from_pattern(regex, query_field)?) as Box<dyn Query>;
                Ok(self.boost_text_field(query, *field))
            }),
            scorer,
        )
    }

    pub(super) fn text_variant_field(&self, field: Field) -> Field {
        self.text_variant_fields
            .get(&field)
            .copied()
            .unwrap_or(field)
    }

    pub(super) fn or_field_queries<I>(
        &self,
        queries: I,
        scorer: FullTextScorer,
    ) -> Result<Box<dyn Query>, Error>
    where
        I: IntoIterator<Item = Result<Box<dyn Query>, Error>>,
    {
        let mut queries = queries.into_iter().collect::<Result<Vec<_>, Error>>()?;
        if queries.len() > self.max_expansions {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        if queries.is_empty() {
            return Ok(Box::new(AllQuery));
        }
        if queries.len() == 1 {
            return Ok(queries.remove(0));
        }
        if matches!(scorer, FullTextScorer::DisMax) {
            return Ok(Box::new(DisjunctionMaxQuery::new(queries)));
        }
        Ok(Box::new(BooleanQuery::new(
            queries
                .into_iter()
                .map(|query| (Occur::Should, query))
                .collect(),
        )))
    }

    pub(super) fn boost_text_field(&self, query: Box<dyn Query>, field: Field) -> Box<dyn Query> {
        let weight = self
            .text_field_settings
            .get(&field)
            .map(|settings| settings.weight)
            .unwrap_or(1.0);
        if (weight - 1.0).abs() < f32::EPSILON {
            query
        } else {
            Box::new(BoostQuery::new(query, weight))
        }
    }

    pub(super) fn effective_text_settings(
        &self,
        settings: Option<&FullTextTextFieldSettings>,
        options: &FullTextSearchOptions,
    ) -> FullTextTextFieldSettings {
        let mut settings = settings
            .cloned()
            .unwrap_or_else(|| FullTextTextFieldSettings {
                nostem: false,
                phonetic: false,
                with_suffix_trie: false,
                stopwords: HashSet::new(),
                language: self.default_language.clone(),
                weight: 1.0,
            });
        if let Some(language) = &options.language {
            settings.language.clone_from(language);
        }
        if let Some(phonetic) = options.phonetic {
            settings.phonetic = phonetic;
        }
        if options.verbatim {
            settings.nostem = true;
            settings.phonetic = false;
        }
        if options.no_stopwords {
            settings.stopwords.clear();
        }
        settings
    }

    pub(super) fn apply_search_filters(
        &self,
        query: Box<dyn Query>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let mut clauses = vec![(Occur::Must, query)];
        if options.filters.len() > self.max_expansions
            || options
                .in_keys
                .as_ref()
                .is_some_and(|keys| keys.len() > self.max_expansions)
        {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        for filter in &options.filters {
            let Some((field, FullTextFieldKind::Numeric)) = self.query_fields.get(&filter.field)
            else {
                return Err(Error::msg("ERR invalid numeric field"));
            };
            clauses.push((
                Occur::Must,
                Box::new(RangeQuery::new(
                    search_bound_to_tantivy(*field, filter.min, true),
                    search_bound_to_tantivy(*field, filter.max, false),
                )),
            ));
        }
        if let Some(keys) = &options.in_keys {
            let key_queries = keys
                .iter()
                .map(|key| {
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.key_field, key),
                        IndexRecordOption::Basic,
                    )) as Box<dyn Query>
                })
                .collect::<Vec<_>>();
            if key_queries.is_empty() {
                return Ok(Box::new(BooleanQuery::new(Vec::new())));
            }
            clauses.push((
                Occur::Must,
                Box::new(BooleanQuery::new(
                    key_queries
                        .into_iter()
                        .map(|query| (Occur::Should, query))
                        .collect(),
                )),
            ));
        }
        Ok(if clauses.len() == 1 {
            clauses.remove(0).1
        } else {
            Box::new(BooleanQuery::new(clauses))
        })
    }
}
