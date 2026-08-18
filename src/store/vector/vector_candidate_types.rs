#[derive(Clone, Debug)]
struct VectorCandidate {
    id: String,
    doc_version: u64,
    distance: f32,
    /// Position in the immutable source aligned with a persisted HNSW node.
    /// Consumed by runtime FP32 reranking before candidates leave a segment.
    source_position: Option<usize>,
}

struct VectorSearchContext<'a> {
    index: &'a str,
    version: u64,
    meta: &'a VectorIndexMeta,
    query: &'a [f32],
    query_norm_squared: f64,
    options: &'a VectorSearchOptions,
    filters: &'a [FilterPredicate],
    allow_doc_ids: Option<&'a HashSet<String>>,
}
