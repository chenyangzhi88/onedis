impl FullTextRuntime {
    fn build_query(
        &self,
        query_text: &str,
        options: &FullTextSearchOptions,
    ) -> Result<Box<dyn Query>, Error> {
        let query_text = query_text.trim();
        if query_text == "*" {
            return Ok(Box::new(AllQuery));
        }

        let ast = FullTextQueryParser::new(query_text, options.dialect).parse()?;
        if matches!(ast, FullTextQueryAst::All) {
            return Ok(Box::new(AllQuery));
        }
        self.plan_query(&ast, options.in_fields.as_deref())
    }

    fn plan_query(
        &self,
        ast: &FullTextQueryAst,
        field_scope: Option<&[String]>,
    ) -> Result<Box<dyn Query>, Error> {
        match ast {
            FullTextQueryAst::All => Ok(Box::new(AllQuery)),
            FullTextQueryAst::Text(term) => self.plan_text_query(term, field_scope),
            FullTextQueryAst::Phrase(phrase) => {
                self.plan_text_query(&format!("\"{phrase}\""), field_scope)
            }
            FullTextQueryAst::Prefix(prefix) => self.plan_prefix_query(prefix, field_scope),
            FullTextQueryAst::Wildcard(pattern) => self.plan_wildcard_query(pattern, field_scope),
            FullTextQueryAst::Fuzzy(term) => self.plan_fuzzy_query(term, field_scope),
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
            FullTextQueryAst::Field { fields, expr } => self.plan_query(expr, Some(fields)),
            FullTextQueryAst::And(children) => self.plan_boolean(children, Occur::Must, field_scope),
            FullTextQueryAst::Or(children) => {
                self.plan_boolean(children, Occur::Should, field_scope)
            }
            FullTextQueryAst::Not(child) => Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                (Occur::MustNot, self.plan_query(child, field_scope)?),
            ]))),
            FullTextQueryAst::Optional(child) => {
                let _ = child;
                Ok(Box::new(AllQuery))
            }
            FullTextQueryAst::Attributed { expr, weight } => {
                let query = self.plan_query(expr, field_scope)?;
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
    ) -> Result<Box<dyn Query>, Error> {
        if children.is_empty() {
            return Ok(Box::new(AllQuery));
        }
        if children.len() == 1 {
            return self.plan_query(&children[0], field_scope);
        }
        Ok(Box::new(BooleanQuery::new(
            children
                .iter()
                .map(|child| self.plan_query(child, field_scope).map(|query| (occur, query)))
                .collect::<Result<Vec<_>, Error>>()?,
        )))
    }

    fn plan_text_query(
        &self,
        query_text: &str,
        field_scope: Option<&[String]>,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        if let Some(term) = fulltext_simple_query_term(query_text) {
            return self.or_field_queries(fields.into_iter().map(|field| {
                let settings = self.text_field_settings.get(&field);
                let variants = fulltext_query_term_variants(term, settings, &self.synonyms);
                Ok(Box::new(BooleanQuery::new(
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
                )) as Box<dyn Query>)
            }));
        }
        let parser = QueryParser::for_index(&self.index, fields);
        Ok(parser.parse_query(query_text)?)
    }

    fn plan_wildcard_query(
        &self,
        pattern: &str,
        field_scope: Option<&[String]>,
    ) -> Result<Box<dyn Query>, Error> {
        let regex = fulltext_wildcard_to_regex(pattern);
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        self.or_field_queries(fields.into_iter().map(|field| {
            RegexQuery::from_pattern(&regex, field)
                .map(|query| Box::new(query) as Box<dyn Query>)
                .map_err(Error::from)
        }))
    }

    fn plan_prefix_query(
        &self,
        prefix: &str,
        field_scope: Option<&[String]>,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        let prefix = prefix.to_ascii_lowercase();
        self.or_field_queries(fields.into_iter().map(|field| {
            Ok(Box::new(PhrasePrefixQuery::new(vec![Term::from_field_text(
                field, &prefix,
            )])) as Box<dyn Query>)
        }))
    }

    fn plan_fuzzy_query(
        &self,
        term: &str,
        field_scope: Option<&[String]>,
    ) -> Result<Box<dyn Query>, Error> {
        let fields = self.text_fields_for_scope(field_scope)?;
        if fields.is_empty() {
            return Err(Error::msg("ERR invalid text field"));
        }
        self.or_field_queries(fields.into_iter().map(|field| {
            Ok(Box::new(FuzzyTermQuery::new(
                Term::from_field_text(field, term),
                1,
                true,
            )) as Box<dyn Query>)
        }))
    }

    fn plan_tag_query(&self, field: &str, values: &[String]) -> Result<Box<dyn Query>, Error> {
        let Some((tantivy_field, FullTextFieldKind::Tag)) = self.query_fields.get(field) else {
            return Err(Error::msg("ERR invalid tag field"));
        };
        self.or_field_queries(values.iter().map(|value| {
            Ok(Box::new(TermQuery::new(
                Term::from_field_text(*tantivy_field, value),
                IndexRecordOption::Basic,
            )) as Box<dyn Query>)
        }))
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

    fn or_field_queries<I>(&self, queries: I) -> Result<Box<dyn Query>, Error>
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
        Ok(Box::new(BooleanQuery::new(
            queries
                .into_iter()
                .map(|query| (Occur::Should, query))
                .collect(),
        )))
    }
}
