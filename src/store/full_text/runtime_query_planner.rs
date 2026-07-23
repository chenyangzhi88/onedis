impl FullTextRuntime {
    fn build_query(
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

    fn plan_query(
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
            FullTextQueryAst::Fuzzy(term) => {
                self.plan_fuzzy_query(term, field_scope, options)
            }
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
            } => {
                let _ = (field, lon, lat, radius, unit);
                Err(Error::msg(
                    "ERR fulltext geo query execution is not implemented",
                ))
            }
            FullTextQueryAst::GeoShape {
                field,
                relation,
                shape,
            } => {
                let _ = (field, relation, shape);
                Err(Error::msg(
                    "ERR fulltext geoshape query execution is not implemented",
                ))
            }
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
                (Occur::MustNot, self.plan_query(child, field_scope, options)?),
            ]))),
            FullTextQueryAst::Optional(child) => Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                (
                    Occur::Should,
                    self.plan_query(child, field_scope, options)?,
                ),
            ]))),
            FullTextQueryAst::Attributed { expr, weight } => {
                let query = self.plan_query(expr, field_scope, options)?;
                if let Some(weight) = weight {
                    Ok(Box::new(BoostQuery::new(query, *weight)))
                } else {
                    Ok(query)
                }
            }
        }
    }

    fn plan_boolean(
        &self,
        children: &[FullTextQueryAst],
        occur: Occur,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if children.is_empty() {
            return Ok(Box::new(AllQuery));
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

    fn plan_and(
        &self,
        children: &[FullTextQueryAst],
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if children.is_empty() {
            return Ok(Box::new(AllQuery));
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
                    clauses.push((
                        Occur::Must,
                        self.plan_query(child, field_scope, options)?,
                    ));
                }
            }
        }
        if !has_required {
            clauses.push((Occur::Must, Box::new(AllQuery)));
        }
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn plan_text_query(
        &self,
        query_text: &str,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        if !self.has_positions {
            return Err(Error::msg(
                "ERR phrase queries require offsets in this fulltext index",
            ));
        }
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        if let Some(term) = fulltext_simple_query_term(query_text) {
            return self.or_field_queries(fields.into_iter().map(|field| {
                let settings = self.text_field_settings.get(&field);
                let effective = self.effective_text_settings(settings, options);
                let variants =
                    fulltext_query_term_variants(term, Some(&effective), &self.synonyms);
                let query = Box::new(BooleanQuery::new(
                    variants
                        .into_iter()
                        .map(|variant| {
                            (
                                Occur::Should,
                                Box::new(TermQuery::new(
                                    Term::from_field_text(field, &variant),
                                    IndexRecordOption::Basic,
                                )) as Box<dyn Query>,
                            )
                        })
                        .collect(),
                )) as Box<dyn Query>;
                Ok(self.boost_text_field(query, field))
            }), options.scorer);
        }
        let parser = QueryParser::for_index(&self.index, fields);
        Ok(parser.parse_query(query_text)?)
    }

    fn plan_phrase_query(
        &self,
        phrase: &str,
        field_scope: Option<&[String]>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        self.or_field_queries(fields.into_iter().map(|field| {
            let settings = self.effective_text_settings(
                self.text_field_settings.get(&field),
                options,
            );
            let tokens = fulltext_tokenize_with_language(phrase, &settings.language);
            if tokens.is_empty() {
                return Ok(Box::new(AllQuery) as Box<dyn Query>);
            }
            if tokens.len() == 1 {
                let query = Box::new(TermQuery::new(
                    Term::from_field_text(field, &tokens[0]),
                    IndexRecordOption::WithFreqsAndPositions,
                )) as Box<dyn Query>;
                return Ok(self.boost_text_field(query, field));
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
        }), options.scorer)
    }

    fn plan_wildcard_query(
        &self,
        pattern: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let regex = fulltext_wildcard_to_regex(pattern);
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        self.or_field_queries(fields.into_iter().map(|field| {
            RegexQuery::from_pattern(&regex, field)
                .map(|query| {
                    self.boost_text_field(Box::new(query) as Box<dyn Query>, field)
                })
                .map_err(Error::from)
        }), _options.scorer)
    }

    fn plan_prefix_query(
        &self,
        prefix: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        let prefix = prefix.to_ascii_lowercase();
        self.or_field_queries(fields.into_iter().map(|field| {
            let query = Box::new(PhrasePrefixQuery::new(vec![Term::from_field_text(
                field, &prefix,
            )])) as Box<dyn Query>;
            Ok(self.boost_text_field(query, field))
        }), _options.scorer)
    }

    fn plan_fuzzy_query(
        &self,
        term: &str,
        field_scope: Option<&[String]>,
        _options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        self.or_field_queries(fields.into_iter().map(|field| {
            let query = Box::new(FuzzyTermQuery::new(
                Term::from_field_text(field, term),
                1,
                true,
            )) as Box<dyn Query>;
            Ok(self.boost_text_field(query, field))
        }), _options.scorer)
    }

    fn plan_tag_query(&self, field: &str, values: &[String]) -> Result<Box<dyn Query>, Error> {
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
        self.or_field_queries(values.iter().map(|value| {
            let value = if settings.case_sensitive {
                value.clone()
            } else {
                value.to_lowercase()
            };
            Ok(Box::new(TermQuery::new(
                Term::from_field_text(*tantivy_field, &value),
                IndexRecordOption::Basic,
            )) as Box<dyn Query>)
        }), FullTextScorer::Bm25Std)
    }

    fn plan_numeric_query(
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

    fn text_fields_for_scope(&self, field_scope: Option<&[String]>) -> Result<Vec<Field>, Error> {
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

    fn or_field_queries<I>(
        &self,
        queries: I,
        scorer: FullTextScorer,
    ) -> Result<Box<dyn Query>, Error>
    where
        I: IntoIterator<Item = Result<Box<dyn Query>, Error>>,
    {
        let mut queries = queries.into_iter().collect::<Result<Vec<_>, Error>>()?;
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

    fn boost_text_field(&self, query: Box<dyn Query>, field: Field) -> Box<dyn Query> {
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

    fn effective_text_settings(
        &self,
        settings: Option<&FullTextTextFieldSettings>,
        options: &FullTextSearchOptions,
    ) -> FullTextTextFieldSettings {
        let mut settings = settings.cloned().unwrap_or_else(|| FullTextTextFieldSettings {
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
        settings
    }

    fn apply_search_filters(
        &self,
        query: Box<dyn Query>,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let mut clauses = vec![(Occur::Must, query)];
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
