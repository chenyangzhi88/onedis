#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum FullTextIndexState {
    Creating,
    Backfilling,
    Ready,
    Dirty,
    Rebuilding,
    Dropping,
}

#[derive(Clone, Debug, Encode, Decode)]
struct FullTextAliasMeta {
    index: String,
}

#[derive(Clone, Debug, Encode, Decode)]
struct FullTextSuggestRecord {
    score: f64,
    payload: Option<String>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct FullTextSynonymGroup {
    terms: Vec<String>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct FullTextRefreshPolicy {
    max_docs: usize,
    max_bytes: usize,
    refresh_interval_ms: u64,
}

impl Default for FullTextRefreshPolicy {
    fn default() -> Self {
        Self {
            max_docs: DEFAULT_REFRESH_MAX_DOCS,
            max_bytes: DEFAULT_REFRESH_MAX_BYTES,
            refresh_interval_ms: DEFAULT_REFRESH_INTERVAL_MS,
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct FullTextIndexMeta {
    source_type: FullTextSourceType,
    prefixes: Vec<String>,
    schema: Vec<FullTextFieldSchema>,
    aliases: Vec<String>,
    index_options: FullTextIndexOptions,
    state: FullTextIndexState,
    generation: u64,
    backfill_cursor: Option<String>,
    last_indexed_outbox_seq: u64,
    refresh_policy: FullTextRefreshPolicy,
}
