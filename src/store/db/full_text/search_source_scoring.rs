use super::*;

impl Db {
    pub(super) fn fulltext_apply_selected_scorer(
        &self,
        _meta: &FullTextIndexMeta,
        _ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        hits: &mut [FullTextLiveHit],
        deadline: Instant,
        fail_on_timeout: bool,
    ) -> Result<(), Error> {
        if !matches!(options.scorer, FullTextScorer::Bm25) {
            // BM25STD is Tantivy's native BM25 score. DISMAX is planned as a
            // DisjunctionMaxQuery and DOCSCORE is materialized from the indexed
            // source projection, so neither needs a second scoring pass.
            return Ok(());
        }
        for hit in hits {
            if fulltext_search_timeout_reached(deadline, fail_on_timeout)? {
                break;
            }
            hit.score = fulltext_legacy_bm25_score(hit.score);
        }
        Ok(())
    }
}

/// RedisSearch retains BM25 as a legacy scorer distinct from BM25STD. Keep its
/// score scale distinct with a monotonic saturation while reusing Tantivy's
/// postings, corpus statistics and field norms. Monotonicity preserves TopK,
/// avoiding the former O(N) source scan and re-tokenization on every query.
pub(super) fn fulltext_legacy_bm25_score(score: f32) -> f32 {
    if score <= 0.0 || !score.is_finite() {
        return 0.0;
    }
    score / (score + 1.2)
}
