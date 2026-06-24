impl FullTextRuntime {
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
}
