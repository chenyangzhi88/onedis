impl Db {
    fn fulltext_collect_live_hits(
        &self,
        index: &str,
        query: &str,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        fulltext_validate_search_geo_filters(&meta, &options.geo_filters)?;
        self.ensure_fulltext_runtime(&index)?;
        self.fulltext_refresh_index(&index, true)?;
        let runtime = self
            .fulltext_runtimes
            .get(self.db_index, &index)
            .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
        let ast_query = if fulltext_query_has_vector_syntax(query) {
            query.to_string()
        } else {
            substitute_fulltext_params(query, &options.params)?
        };
        let ast = FullTextQueryParser::new(&ast_query, options.dialect).parse()?;
        if contains_fulltext_vector_query(&ast) {
            return self.fulltext_vector_hits(&index, &meta, &runtime, &ast, options);
        }
        if contains_fulltext_geo_query(&ast) {
            fulltext_validate_geo_query_ast(&meta, &ast)?;
            return self.fulltext_exact_filter_hits(&meta, &ast, options);
        }
        let mut candidate_hits = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
            .search(query, options)?;
        let candidate_keys = candidate_hits
            .iter()
            .map(|hit| hit.key.clone())
            .collect::<Vec<_>>();
        if matches!(meta.source_type, FullTextSourceType::Hash)
            && self.fulltext_revalidate_hash_candidates(&runtime, &candidate_keys)?
        {
            candidate_hits = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .search(query, options)?;
        }
        let mut live = Vec::new();
        for hit in candidate_hits {
            if options
                .in_keys
                .as_ref()
                .is_some_and(|keys| !keys.contains(&hit.key))
            {
                continue;
            }
            if let Some(hit) =
                self.fulltext_live_hit_from_source(&meta, &options, hit.key, hit.score)?
            {
                live.push(hit);
            }
        }
        Ok(live)
    }
}
