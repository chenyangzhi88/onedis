use super::*;
impl FullTextRuntime {
    pub(super) fn upsert_hash(
        &mut self,
        key: &str,
        fields: &[(String, String)],
        expires_at_ms: u64,
    ) -> Result<usize, Error> {
        self.upsert_fields(key, fields, expires_at_ms)
    }

    pub(super) fn upsert_fields(
        &mut self,
        key: &str,
        fields: &[(String, String)],
        expires_at_ms: u64,
    ) -> Result<usize, Error> {
        let prepared = self.prepare_fields_document(key, fields, expires_at_ms)?;
        let indexed_bytes = prepared.indexed_bytes;
        self.apply_prepared_document(prepared)?;
        Ok(indexed_bytes)
    }

    pub(super) fn prepare_fields_document(
        &self,
        key: &str,
        fields: &[(String, String)],
        expires_at_ms: u64,
    ) -> Result<FullTextPreparedDocument, Error> {
        let document_language = self
            .language_field
            .as_ref()
            .and_then(|language_field| {
                fields
                    .iter()
                    .find(|(name, _)| name == language_field)
                    .map(|(_, value)| value.as_str())
            })
            .map(normalize_fulltext_language)
            .transpose()?
            .unwrap_or_else(|| self.default_language.clone());
        let mut doc = TantivyDocument::default();
        doc.add_text(self.key_field, key);
        doc.add_u64(self.expires_at_field, expires_at_ms);
        let present_names = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<HashSet<_>>();
        let mut marked_presence = HashSet::new();
        for name in &present_names {
            if let Some(marker) = self.presence_fields.get(*name)
                && marked_presence.insert(*marker)
            {
                doc.add_u64(*marker, 1);
            }
        }
        let mut marked_empty = HashSet::new();
        let mut marked_sort_fields = HashSet::new();
        for (name, value) in fields {
            if value.is_empty()
                && let Some(marker) = self.empty_fields.get(name)
                && marked_empty.insert(*marker)
            {
                doc.add_u64(*marker, 1);
            }
        }
        for (name, value) in fields {
            if let Some((lon_field, lat_field)) = self.geo_fields.get(name)
                && let Ok((lon, lat)) = parse_fulltext_geo_value(value)
            {
                doc.add_f64(*lon_field, lon);
                doc.add_f64(*lat_field, lat);
            }
            if let Some(geoshape_fields) = self.geoshape_fields.get(name)
                && let Ok(geometry) = parse_fulltext_wkt(value)
                && let Some((min_x, max_x, min_y, max_y)) = fulltext_geometry_bounds(&geometry)
            {
                let bounds_fields = geoshape_fields.bounds;
                doc.add_f64(bounds_fields[0], min_x);
                doc.add_f64(bounds_fields[1], max_x);
                doc.add_f64(bounds_fields[2], min_y);
                doc.add_f64(bounds_fields[3], max_y);
                match fulltext_geoshape_cells((min_x, max_x, min_y, max_y)) {
                    Some(cells) => {
                        for cell in cells {
                            doc.add_text(geoshape_fields.cells, cell);
                        }
                    }
                    None => doc.add_text(geoshape_fields.cells, FULLTEXT_GEOSHAPE_OVERSIZE_CELL),
                }
            }
            if let Some((sort_field, kind)) = self.sortable_fields.get(name)
                && marked_sort_fields.insert(*sort_field)
            {
                // JSON paths can produce multiple values. RediSearch exposes the first
                // value as the sort key, so the FAST field must not collect the rest.
                match kind {
                    FullTextFieldKind::Numeric => {
                        if let Ok(value) = value.parse::<f64>()
                            && value.is_finite()
                        {
                            doc.add_f64(*sort_field, value);
                        }
                    }
                    FullTextFieldKind::Text | FullTextFieldKind::Tag => {
                        doc.add_text(*sort_field, value);
                    }
                    _ => {}
                }
            }
        }
        let mut indexed_bytes = key.len();
        for (field_name, value) in fields {
            if value.is_empty() {
                continue;
            }
            let Some((field, kind)) = self.source_fields.get(field_name) else {
                continue;
            };
            indexed_bytes += field_name.len() + value.len();
            match kind {
                FullTextFieldKind::Text => {
                    let (source_value, variant_value) = self
                        .text_field_settings
                        .get(field)
                        .map(|settings| {
                            let mut settings = settings.clone();
                            settings.language.clone_from(&document_language);
                            fulltext_materialize_text_with_synonyms(
                                value,
                                &settings,
                                &self.writer_synonyms,
                            )
                        })
                        .unwrap_or_else(|| (value.clone(), value.clone()));
                    doc.add_text(*field, &source_value);
                    if let Some(variant_field) = self.text_variant_fields.get(field) {
                        doc.add_text(*variant_field, &variant_value);
                    }
                }
                FullTextFieldKind::Tag => {
                    let settings = self.tag_field_settings.get(field).cloned().unwrap_or(
                        FullTextTagFieldSettings {
                            separator: ',',
                            case_sensitive: false,
                        },
                    );
                    for tag in fulltext_split_indexed_tags(
                        value,
                        settings.separator,
                        settings.case_sensitive,
                    ) {
                        doc.add_text(*field, tag);
                    }
                }
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
        Ok(FullTextPreparedDocument {
            key: key.to_string(),
            document: doc,
            indexed_bytes,
            expires_at_ms,
        })
    }

    pub(super) fn apply_prepared_document(
        &mut self,
        prepared: FullTextPreparedDocument,
    ) -> Result<(), Error> {
        if prepared.expires_at_ms > 0 {
            self.has_expiring_documents
                .store(true, AtomicOrdering::Release);
        }
        self.writer
            .delete_term(Term::from_field_text(self.key_field, &prepared.key));
        self.writer.add_document(prepared.document)?;
        Ok(())
    }

    pub(super) fn delete_hash(&mut self, key: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
    }

    pub(super) fn publish(&mut self) -> Result<(), Error> {
        self.writer.commit()?;
        self.reader.reload()?;
        self.has_expiring_documents.store(
            self.reader.searcher().search(
                &RangeQuery::new(
                    Bound::Excluded(Term::from_field_u64(self.expires_at_field, 0)),
                    Bound::Unbounded,
                ),
                &Count,
            )? > 0,
            AtomicOrdering::Release,
        );
        self.expansion_terms
            .lock()
            .map_err(|_| Error::msg("ERR fulltext expansion cache lock poisoned"))?
            .clear();
        self.last_refresh_at = Instant::now();
        Ok(())
    }

    pub(super) fn publish_through(&mut self, outbox_seq: u64) -> Result<(), Error> {
        self.publish()?;
        self.published_outbox_seq = self.published_outbox_seq.max(outbox_seq);
        Ok(())
    }

    pub(super) fn published_outbox_seq(&self) -> u64 {
        self.published_outbox_seq
    }

    pub(super) fn durable_outbox_seq(&self) -> u64 {
        self.durable_outbox_seq
    }

    pub(super) fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub(super) fn visit_indexed_keys(
        &self,
        mut visitor: impl FnMut(&str) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let searcher = self.reader.searcher();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(self.key_field)?;
            let mut stream = inverted.terms().stream()?;
            while stream.advance() {
                let Ok(key) = std::str::from_utf8(stream.key()) else {
                    continue;
                };
                let term = Term::from_field_text(self.key_field, key);
                let query = TermQuery::new(term, IndexRecordOption::Basic);
                if searcher.search(&query, &Count)? > 0 {
                    visitor(key)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn refresh_due(&self, policy: &FullTextRefreshPolicy) -> bool {
        self.last_refresh_at.elapsed() >= Duration::from_millis(policy.refresh_interval_ms)
    }
}
