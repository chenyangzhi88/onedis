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
        // The key is also a FAST field so bounded SORTBY collection can use it as the
        // deterministic secondary key across segments.
        let key_field = builder.add_text_field(FULLTEXT_KEY_FIELD, STRING | STORED | FAST);
        let expires_at_field = builder.add_u64_field(FULLTEXT_EXPIRES_AT_FIELD, INDEXED);
        let mut text_fields = Vec::new();
        let mut text_variant_fields = HashMap::new();
        let mut text_field_settings = HashMap::new();
        let mut tag_field_settings = HashMap::new();
        let mut source_fields = HashMap::new();
        let mut query_fields = HashMap::new();
        let mut presence_fields = HashMap::new();
        let mut empty_fields = HashMap::new();
        let mut geo_fields = HashMap::new();
        let mut geoshape_fields = HashMap::new();
        let mut sortable_fields = HashMap::new();
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
        for (schema_offset, field) in meta.schema.iter().enumerate() {
            if field.options.index_missing {
                let marker = builder.add_u64_field(
                    &format!("{FULLTEXT_PRESENCE_FIELD_PREFIX}{schema_offset}"),
                    INDEXED,
                );
                presence_fields.insert(field.name.clone(), marker);
                presence_fields.insert(field.attribute_name().to_string(), marker);
            }
            if field.options.index_empty {
                let marker = builder.add_u64_field(
                    &format!("{FULLTEXT_EMPTY_FIELD_PREFIX}{schema_offset}"),
                    INDEXED,
                );
                empty_fields.insert(field.name.clone(), marker);
                empty_fields.insert(field.attribute_name().to_string(), marker);
            }
            if matches!(field.kind, FullTextFieldKind::Geo) && !field.options.noindex {
                let lon = builder.add_f64_field(
                    &format!("{FULLTEXT_GEO_FIELD_PREFIX}{schema_offset}_lon"),
                    INDEXED | FAST,
                );
                let lat = builder.add_f64_field(
                    &format!("{FULLTEXT_GEO_FIELD_PREFIX}{schema_offset}_lat"),
                    INDEXED | FAST,
                );
                geo_fields.insert(field.name.clone(), (lon, lat));
                geo_fields.insert(field.attribute_name().to_string(), (lon, lat));
            }
            if matches!(field.kind, FullTextFieldKind::GeoShape) && !field.options.noindex {
                let bounds = [
                    builder.add_f64_field(
                        &format!("{FULLTEXT_GEOSHAPE_FIELD_PREFIX}{schema_offset}_min_x"),
                        INDEXED,
                    ),
                    builder.add_f64_field(
                        &format!("{FULLTEXT_GEOSHAPE_FIELD_PREFIX}{schema_offset}_max_x"),
                        INDEXED,
                    ),
                    builder.add_f64_field(
                        &format!("{FULLTEXT_GEOSHAPE_FIELD_PREFIX}{schema_offset}_min_y"),
                        INDEXED,
                    ),
                    builder.add_f64_field(
                        &format!("{FULLTEXT_GEOSHAPE_FIELD_PREFIX}{schema_offset}_max_y"),
                        INDEXED,
                    ),
                ];
                let cells = builder.add_text_field(
                    &format!("{FULLTEXT_GEOSHAPE_FIELD_PREFIX}{schema_offset}_cells"),
                    STRING,
                );
                let fields = FullTextGeoShapeFields { bounds, cells };
                geoshape_fields.insert(field.name.clone(), fields);
                geoshape_fields.insert(field.attribute_name().to_string(), fields);
            }
            if field.options.sortable && !field.options.noindex {
                let sort_field = match field.kind {
                    FullTextFieldKind::Numeric => {
                        builder.add_f64_field(&format!("__sort_{schema_offset}"), FAST)
                    }
                    FullTextFieldKind::Text | FullTextFieldKind::Tag => builder.add_text_field(
                        &format!("__sort_{schema_offset}"),
                        TextOptions::default().set_fast(None),
                    ),
                    _ => {
                        return Err(Error::msg("ERR unsupported SORTABLE fulltext field type"));
                    }
                };
                sortable_fields.insert(field.name.clone(), (sort_field, field.kind));
                sortable_fields
                    .insert(field.attribute_name().to_string(), (sort_field, field.kind));
            }
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
        let directory = KvTantivyDirectory::new_tiered(
            store,
            db_index,
            storage_name,
            config.directory_cache_bytes,
        );
        let index = Index::open_or_create(directory.clone(), schema)?;
        let reader = index.reader()?;
        let has_expiring_documents = reader.searcher().search(
            &RangeQuery::new(
                Bound::Excluded(Term::from_field_u64(expires_at_field, 0)),
                Bound::Unbounded,
            ),
            &Count,
        )? > 0;
        let writer = index.writer(config.writer_heap_bytes)?;
        let mut merge_policy = LogMergePolicy::default();
        merge_policy.set_min_num_segments(config.merge_min_segments);
        merge_policy.set_max_docs_before_merge(config.merge_max_docs);
        merge_policy.set_min_layer_size(
            config
                .merge_min_layer_docs
                .try_into()
                .map_err(|_| Error::msg("ERR invalid fulltext merge layer size"))?,
        );
        merge_policy.set_del_docs_ratio_before_merge(config.merge_delete_ratio);
        writer.set_merge_policy(Box::new(merge_policy));
        let search = Arc::new(FullTextSearchGeneration {
            incarnation: meta.incarnation,
            search_meta: meta.clone(),
            index,
            reader,
            key_field,
            expires_at_field,
            text_fields,
            text_variant_fields,
            text_field_settings,
            tag_field_settings,
            source_fields,
            query_fields,
            presence_fields,
            empty_fields,
            geo_fields,
            geoshape_fields,
            sortable_fields,
            default_language,
            language_field: meta.index_options.language_field.clone(),
            no_fields: meta.index_options.no_fields,
            has_positions: !(meta.index_options.no_freqs || meta.index_options.no_offsets),
            min_prefix: config.min_prefix,
            max_expansions: config.max_expansions,
            max_prefix_expansions: config.max_prefix_expansions,
            has_expiring_documents: AtomicBool::new(has_expiring_documents),
            retired: AtomicBool::new(false),
            expansion_terms: Mutex::new(HashMap::new()),
        });
        Ok(Self {
            search,
            writer,
            directory,
            published_outbox_seq: meta.last_indexed_outbox_seq,
            durable_outbox_seq: meta.last_indexed_outbox_seq,
            published_backfill_cursor: meta.backfill_cursor.clone(),
            backfill_complete: !matches!(
                meta.state,
                FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
            ),
            writer_synonyms: synonyms,
            last_refresh_at: Instant::now(),
            last_checkpoint_at: Instant::now(),
        })
    }
}
