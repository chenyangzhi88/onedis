#[derive(Clone, Debug)]
struct VectorCandidate {
    id: String,
    doc_version: u64,
    distance: f32,
}

struct VectorSearchContext<'a> {
    index: &'a str,
    version: u64,
    meta: &'a VectorIndexMeta,
    query: &'a [f32],
    options: &'a VectorSearchOptions,
    filters: &'a [FilterPredicate],
    allow_doc_ids: Option<&'a HashSet<String>>,
}
