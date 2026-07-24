use super::*;
impl FullTextRuntime {
    pub(super) fn new(
        store: crate::store::kv_store::KvStore,
        db_index: u16,
        index_name: &str,
        storage_name: &str,
        meta: &FullTextIndexMeta,
        config: &FullTextRuntimeConfig,
    ) -> Result<Self, Error> {
        let mut builder = Schema::builder();
        let key_field = builder.add_text_field(FULLTEXT_KEY_FIELD, STRING | STORED);
        let mut text_fields = Vec::new();
        let mut text_variant_fields = HashMap::new();
        let mut text_field_settings = HashMap::new();
        let mut tag_field_settings = HashMap::new();
        let mut source_fields = HashMap::new();
        let mut query_fields = HashMap::new();
        let default_language = normalize_fulltext_language(
            meta.index_options.language.as_deref().unwrap_or("english"),
        )?;
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
                    let index_option = if meta.index_options.no_freqs {
                        IndexRecordOption::Basic
                    } else if meta.index_options.no_offsets {
                        IndexRecordOption::WithFreqs
                    } else {
                        IndexRecordOption::WithFreqsAndPositions
                    };
                    let text_options = TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default().set_index_option(index_option),
                    );
                    let field_id = builder.add_text_field(attribute, text_options);
                    let variant_field = builder.add_text_field(
                        &format!("__onedis_ft_variant_{}", text_fields.len()),
                        TextOptions::default().set_indexing_options(
                            TextFieldIndexing::default().set_index_option(IndexRecordOption::Basic),
                        ),
                    );
                    text_fields.push(field_id);
                    text_variant_fields.insert(field_id, variant_field);
                    text_field_settings.insert(
                        field_id,
                        FullTextTextFieldSettings {
                            nostem: field.options.nostem,
                            phonetic: field.options.phonetic.is_some(),
                            with_suffix_trie: field.options.with_suffix_trie,
                            stopwords: index_stopwords.clone(),
                            language: default_language.clone(),
                            weight: field.options.weight.unwrap_or(1.0),
                        },
                    );
                    field_id
                }
                FullTextFieldKind::Tag => {
                    let field_id = builder.add_text_field(attribute, STRING);
                    tag_field_settings.insert(
                        field_id,
                        FullTextTagFieldSettings {
                            separator: field
                                .options
                                .separator
                                .as_deref()
                                .and_then(|separator| separator.chars().next())
                                .unwrap_or(','),
                            case_sensitive: field.options.case_sensitive,
                        },
                    );
                    field_id
                }
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
        let directory = KvTantivyDirectory::new(store, db_index, storage_name);
        let index = Index::open_or_create(directory, schema)?;
        let reader = index.reader()?;
        let writer = index.writer(config.writer_heap_bytes)?;
        Ok(Self {
            index,
            reader,
            writer,
            key_field,
            text_fields,
            text_variant_fields,
            text_field_settings,
            tag_field_settings,
            synonyms,
            source_fields,
            query_fields,
            default_language,
            language_field: meta.index_options.language_field.clone(),
            no_fields: meta.index_options.no_fields,
            has_positions: !(meta.index_options.no_freqs || meta.index_options.no_offsets),
            min_prefix: config.min_prefix,
            max_expansions: config.max_expansions,
            max_prefix_expansions: config.max_prefix_expansions,
            last_refresh_at: Instant::now(),
        })
    }
}
