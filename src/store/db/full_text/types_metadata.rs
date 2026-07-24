use super::*;
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
pub(super) struct FullTextAliasMeta {
    pub(super) index: String,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextSuggestRecord {
    pub(super) score: f64,
    pub(super) payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextSynonymGroup {
    pub(super) terms: Vec<String>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextRefreshPolicy {
    pub(super) max_docs: usize,
    pub(super) max_bytes: usize,
    pub(super) refresh_interval_ms: u64,
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
pub(super) struct FullTextIndexMeta {
    pub(super) source_type: FullTextSourceType,
    pub(super) prefixes: Vec<String>,
    pub(super) schema: Vec<FullTextFieldSchema>,
    pub(super) aliases: Vec<String>,
    pub(super) index_options: FullTextIndexOptions,
    pub(super) state: FullTextIndexState,
    pub(super) incarnation: u64,
    pub(super) generation: u64,
    pub(super) revision: u64,
    pub(super) active_storage: String,
    pub(super) backfill_cursor: Option<String>,
    pub(super) last_indexed_outbox_seq: u64,
    pub(super) indexed_docs: u64,
    pub(super) indexed_bytes: u64,
    pub(super) refresh_policy: FullTextRefreshPolicy,
}
