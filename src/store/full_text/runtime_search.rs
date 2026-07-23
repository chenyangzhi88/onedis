impl FullTextRuntime {
    fn search(
        &self,
        query_text: &str,
        options: &FullTextSearchOptions,
        fetch_limit: Option<usize>,
    ) -> Result<FullTextSearchHits, Error> {
        let searcher = self.reader.searcher();
        let query_text = substitute_fulltext_params(query_text, &options.params)?;
        let query = self.build_query(&query_text, options)?;
        self.search_query(query, &searcher, fetch_limit)
    }

    fn search_ast(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextSearchHit>, Error> {
        let searcher = self.reader.searcher();
        let query = self.plan_query(ast, options.in_fields.as_deref(), options)?;
        Ok(self.search_query(query, &searcher, None)?.hits)
    }

    fn search_query(
        &self,
        query: Box<dyn Query>,
        searcher: &tantivy::Searcher,
        fetch_limit: Option<usize>,
    ) -> Result<FullTextSearchHits, Error> {
        let raw_total = searcher.search(query.as_ref(), &Count)?;
        if raw_total == 0 {
            return Ok(FullTextSearchHits {
                total: 0,
                hits: Vec::new(),
            });
        }
        let fetch_limit = fetch_limit.unwrap_or(raw_total).min(raw_total);
        if fetch_limit == 0 {
            return Ok(FullTextSearchHits {
                total: raw_total,
                hits: Vec::new(),
            });
        }
        let top_docs = searcher.search(
            query.as_ref(),
            &TopDocs::with_limit(fetch_limit).order_by_score(),
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
        Ok(FullTextSearchHits {
            total: raw_total,
            hits,
        })
    }
}
