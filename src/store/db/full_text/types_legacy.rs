use super::*;

/// Fulltext metadata written before lifecycle incarnations and generation storage names were
/// persisted. Keep the exact field order because bincode records are positional.
#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct LegacyFullTextIndexMetaV1 {
    pub(super) source_type: FullTextSourceType,
    pub(super) prefixes: Vec<String>,
    pub(super) schema: Vec<FullTextFieldSchema>,
    pub(super) aliases: Vec<String>,
    pub(super) index_options: FullTextIndexOptions,
    pub(super) state: FullTextIndexState,
    pub(super) generation: u64,
    pub(super) backfill_cursor: Option<String>,
    pub(super) last_indexed_outbox_seq: u64,
    pub(super) refresh_policy: FullTextRefreshPolicy,
}

impl From<LegacyFullTextIndexMetaV1> for FullTextIndexMeta {
    fn from(value: LegacyFullTextIndexMetaV1) -> Self {
        Self {
            source_type: value.source_type,
            prefixes: value.prefixes,
            schema: value.schema,
            aliases: value.aliases,
            index_options: value.index_options,
            state: value.state,
            incarnation: value.generation,
            generation: value.generation,
            revision: 0,
            // The reader fills this with the index name. Pre-generation records stored Tantivy
            // files under that name directly.
            active_storage: String::new(),
            backfill_cursor: value.backfill_cursor,
            last_indexed_outbox_seq: value.last_indexed_outbox_seq,
            indexed_docs: 0,
            indexed_bytes: 0,
            refresh_policy: value.refresh_policy,
        }
    }
}
