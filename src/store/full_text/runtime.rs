#[derive(Clone, Debug, Encode, Decode)]
struct FullTextMutationRecord {
    generation: u64,
    kind: FullTextMutationKind,
    key: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FullTextRuntimeKey {
    db_index: u16,
    index: String,
}

struct FullTextRuntime {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    key_field: Field,
    text_fields: Vec<Field>,
    text_field_settings: HashMap<Field, FullTextTextFieldSettings>,
    synonyms: HashMap<String, HashSet<String>>,
    source_fields: HashMap<String, (Field, FullTextFieldKind)>,
    query_fields: HashMap<String, (Field, FullTextFieldKind)>,
    last_refresh_at: Instant,
}

#[derive(Clone, Debug)]
struct FullTextTextFieldSettings {
    nostem: bool,
    phonetic: bool,
    with_suffix_trie: bool,
    stopwords: HashSet<String>,
}

impl FullTextRuntimeRegistry {
    fn key(db_index: u16, index: &str) -> FullTextRuntimeKey {
        FullTextRuntimeKey {
            db_index,
            index: index.to_string(),
        }
    }

    fn insert(&self, db_index: u16, index: &str, runtime: FullTextRuntime) {
        self.indexes
            .insert(Self::key(db_index, index), Arc::new(RwLock::new(runtime)));
    }

    fn get(&self, db_index: u16, index: &str) -> Option<Arc<RwLock<FullTextRuntime>>> {
        self.indexes
            .get(&Self::key(db_index, index))
            .map(|entry| entry.value().clone())
    }

    fn remove(&self, db_index: u16, index: &str) {
        self.indexes.remove(&Self::key(db_index, index));
    }

    pub(crate) fn remove_db(&self, db_index: u16) {
        self.indexes.retain(|key, _| key.db_index != db_index);
    }
}

impl FullTextRuntime {
    fn new(
        store: crate::store::kv_store::KvStore,
        db_index: u16,
        index_name: &str,
        meta: &FullTextIndexMeta,
    ) -> Result<Self, Error> {
        let mut builder = Schema::builder();
        let key_field = builder.add_text_field(FULLTEXT_KEY_FIELD, STRING | STORED);
        let mut text_fields = Vec::new();
        let mut text_field_settings = HashMap::new();
        let mut source_fields = HashMap::new();
        let mut query_fields = HashMap::new();
        let index_stopwords = meta
            .index_options
            .stopwords
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|word| word.to_lowercase())
            .collect::<HashSet<_>>();
        for field in &meta.schema {
            if field.options.noindex {
                continue;
            }
            let attribute = field.attribute_name();
            let tantivy_field = match field.kind {
                FullTextFieldKind::Text => {
                    let field_id = builder.add_text_field(attribute, TEXT);
                    text_fields.push(field_id);
                    text_field_settings.insert(
                        field_id,
                        FullTextTextFieldSettings {
                            nostem: field.options.nostem,
                            phonetic: field.options.phonetic.is_some(),
                            with_suffix_trie: field.options.with_suffix_trie,
                            stopwords: index_stopwords.clone(),
                        },
                    );
                    field_id
                }
                FullTextFieldKind::Tag => builder.add_text_field(attribute, STRING),
                FullTextFieldKind::Numeric => builder.add_f64_field(attribute, INDEXED),
                FullTextFieldKind::Geo
                | FullTextFieldKind::GeoShape
                | FullTextFieldKind::Vector => {
                    continue;
                }
            };
            source_fields.insert(field.name.clone(), (tantivy_field, field.kind));
            if field.attribute_name() != field.name {
                source_fields.insert(attribute.to_string(), (tantivy_field, field.kind));
            }
            query_fields.insert(attribute.to_string(), (tantivy_field, field.kind));
        }
        let schema = builder.build();
        let synonyms = load_fulltext_synonyms_from_store(&store, db_index, index_name)?;
        let directory = KvTantivyDirectory::new(store, db_index, index_name);
        let index = Index::open_or_create(directory, schema)?;
        let reader = index.reader()?;
        let writer = index.writer(FULLTEXT_WRITER_HEAP_BYTES)?;
        Ok(Self {
            index,
            reader,
            writer,
            key_field,
            text_fields,
            text_field_settings,
            synonyms,
            source_fields,
            query_fields,
            last_refresh_at: Instant::now(),
        })
    }

    fn upsert_hash(&mut self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
        self.upsert_fields(key, fields)
    }

    fn upsert_fields(&mut self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
        let mut doc = TantivyDocument::default();
        doc.add_text(self.key_field, key);
        let mut indexed_bytes = key.len();
        for (field_name, value) in fields {
            let Some((field, kind)) = self.source_fields.get(field_name) else {
                continue;
            };
            indexed_bytes += field_name.len() + value.len();
            match kind {
                FullTextFieldKind::Text => {
                    let value = self
                        .text_field_settings
                        .get(field)
                        .map(|settings| fulltext_materialize_text(value, settings))
                        .unwrap_or_else(|| value.clone());
                    doc.add_text(*field, &value);
                }
                FullTextFieldKind::Tag => doc.add_text(*field, value),
                FullTextFieldKind::Numeric => {
                    if let Ok(number) = value.parse::<f64>()
                        && number.is_finite()
                    {
                        doc.add_f64(*field, number);
                    }
                }
                FullTextFieldKind::Geo
                | FullTextFieldKind::GeoShape
                | FullTextFieldKind::Vector => {}
            }
        }
        self.writer.add_document(doc)?;
        Ok(indexed_bytes)
    }

    fn delete_hash(&mut self, key: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
    }

    fn publish(&mut self) -> Result<(), Error> {
        self.writer.commit()?;
        self.reader.reload()?;
        self.last_refresh_at = Instant::now();
        Ok(())
    }

    fn refresh_due(&self, policy: &FullTextRefreshPolicy) -> bool {
        self.last_refresh_at.elapsed() >= Duration::from_millis(policy.refresh_interval_ms)
    }

    fn search(
        &self,
        query_text: &str,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextSearchHit>, Error> {
        let searcher = self.reader.searcher();
        let query_text = substitute_fulltext_params(query_text, &options.params)?;
        let query = self.build_query(&query_text, options)?;
        self.search_query(query, &searcher)
    }

    fn search_ast(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextSearchHit>, Error> {
        let searcher = self.reader.searcher();
        let query = self.plan_query(ast, options.in_fields.as_deref())?;
        self.search_query(query, &searcher)
    }

    fn search_query(
        &self,
        query: Box<dyn Query>,
        searcher: &tantivy::Searcher,
    ) -> Result<Vec<FullTextSearchHit>, Error> {
        let raw_total = searcher.search(query.as_ref(), &Count)?;
        if raw_total == 0 {
            return Ok(Vec::new());
        }
        let top_docs = searcher.search(
            query.as_ref(),
            &TopDocs::with_limit(raw_total).order_by_score(),
        )?;
        let mut hits = Vec::new();
        for (score, address) in top_docs {
            let doc: TantivyDocument = searcher.doc(address)?;
            let Some(key) = doc
                .get_first(self.key_field)
                .and_then(|value| value.as_str())
            else {
                continue;
            };
            hits.push(FullTextSearchHit {
                key: key.to_string(),
                score,
            });
        }
        Ok(hits)
    }

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
            FullTextQueryAst::And(children) => {
                self.plan_boolean(children, Occur::Must, field_scope)
            }
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
                .map(|child| {
                    self.plan_query(child, field_scope)
                        .map(|query| (occur, query))
                })
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

