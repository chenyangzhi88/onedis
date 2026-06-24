impl Db {
    pub fn fulltext_search(
        &self,
        index: &str,
        query: &str,
        options: FullTextSearchOptions,
    ) -> Result<Frame, Error> {
        self.fulltext_reject_cluster_multi_shard("FT.SEARCH")?;
        let options = self.fulltext_effective_search_options(options)?;
        let live = self.fulltext_collect_live_hits(index, query, &options)?;
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
