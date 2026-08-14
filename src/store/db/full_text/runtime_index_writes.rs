use super::*;
impl FullTextRuntime {
    pub(super) fn upsert_hash(
        &mut self,
        key: &str,
        fields: &[(String, String)],
    ) -> Result<usize, Error> {
        self.upsert_fields(key, fields)
    }

    pub(super) fn upsert_fields(
        &mut self,
        key: &str,
        fields: &[(String, String)],
    ) -> Result<usize, Error> {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
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
        let mut indexed_bytes = key.len();
        for (field_name, value) in fields {
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
                                &self.synonyms,
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
        self.writer.add_document(doc)?;
        Ok(indexed_bytes)
    }

    pub(super) fn delete_hash(&mut self, key: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.key_field, key));
    }

    pub(super) fn publish(&mut self) -> Result<(), Error> {
        self.writer.commit()?;
        self.reader.reload()?;
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
