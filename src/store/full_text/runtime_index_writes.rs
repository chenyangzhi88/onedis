impl FullTextRuntime {
    fn upsert_hash(&mut self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
        self.upsert_fields(key, fields)
    }

    fn upsert_fields(&mut self, key: &str, fields: &[(String, String)]) -> Result<usize, Error> {
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
                    let value = self
                        .text_field_settings
                        .get(field)
                        .map(|settings| {
                            let mut settings = settings.clone();
                            settings.language.clone_from(&document_language);
                            fulltext_materialize_text(value, &settings)
                        })
                        .unwrap_or_else(|| value.clone());
                    doc.add_text(*field, &value);
                }
                FullTextFieldKind::Tag => {
                    let settings = self
                        .tag_field_settings
                        .get(field)
                        .cloned()
                        .unwrap_or(FullTextTagFieldSettings {
                            separator: ',',
                            case_sensitive: false,
                        });
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
}
