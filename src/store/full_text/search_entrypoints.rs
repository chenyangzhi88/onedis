impl Db {
    pub fn fulltext_search(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        let started = Instant::now();
        let result = self.fulltext_search_inner(index, query, options);
        global_metrics().record_fulltext_search(elapsed_us(started));
        result
    }

    fn fulltext_search_inner(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.SEARCH")?;
        let options = self.fulltext_effective_search_options(options)?;
        let live =
            self.fulltext_collect_live_hits(index, query, &options, FullTextCollectMode::Page)?;
        self.fulltext_search_frame(live, &options, &fulltext_display_terms(query))
    }

    pub async fn fulltext_search_async(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_search(index, query, options)
    }
}
