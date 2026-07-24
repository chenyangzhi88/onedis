impl Db {
    pub fn fulltext_explain(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
        cli: bool,
    ) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        let options = self.fulltext_effective_search_options(options)?;
        let ast_query = substitute_fulltext_params(query, &options.params)?;
        let ast = FullTextQueryParser::new(&ast_query, options.dialect).parse()?;
        if contains_fulltext_geo_query(&ast) {
            fulltext_validate_geo_query_ast(&meta, &ast)?;
        }
        let lines = fulltext_explain_ast_lines(&ast);
        if cli {
            Ok(Frame::Array(
                lines.into_iter().map(Frame::bulk_string).collect(),
            ))
        } else {
            Ok(Frame::bulk_string(lines.join("\n")))
        }
    }

    pub async fn fulltext_explain_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
        cli: bool,
    ) -> Result<Frame, Error> {
        let index = index.to_string();
        let query = query.to_string();
        self.run_blocking_store_task(move |db| {
            db.fulltext_explain(&index, &query, options, cli)
        })
        .await
    }

    pub fn fulltext_tagvals(&self, index: &str, field: &str) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        let Some(schema) = fulltext_schema_field(&meta, field) else {
            return Err(Error::msg("ERR invalid tag field"));
        };
        if !matches!(schema.kind, FullTextFieldKind::Tag) {
            return Err(Error::msg("ERR invalid tag field"));
        }
        let attribute = schema.attribute_name().to_string();
        let mut values = BTreeSet::new();
        for key in self.fulltext_source_keys(&meta)? {
            let fields = match meta.source_type {
                FullTextSourceType::Hash => self.hash_get_all(&key)?,
                FullTextSourceType::Json => self.fulltext_json_fields(&key, &meta)?.unwrap_or_default(),
            };
            for (_, value) in fields.iter().filter(|(name, _)| name == &attribute) {
                let separator = schema
                    .options
                    .separator
                    .as_deref()
                    .and_then(|separator| separator.chars().next())
                    .unwrap_or(',');
                for tag in fulltext_split_indexed_tags(
                    value,
                    separator,
                    schema.options.case_sensitive,
                ) {
                    values.insert(tag);
                }
            }
        }
        Ok(Frame::Array(
            values.into_iter().map(Frame::bulk_string).collect(),
        ))
    }

    pub async fn fulltext_tagvals_async(&self, index: &str, field: &str) -> Result<Frame, Error> {
        let index = index.to_string();
        let field = field.to_string();
        self.run_blocking_store_task(move |db| db.fulltext_tagvals(&index, &field))
            .await
    }
}
