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
}
