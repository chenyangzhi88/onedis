use super::*;
#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextMutationRecord {
    pub(super) incarnation: u64,
    pub(super) kind: FullTextMutationKind,
    pub(super) key: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct FullTextRuntimeKey {
    pub(super) db_index: u16,
    pub(super) index: String,
}

pub(super) struct FullTextRuntime {
    pub(super) index: Index,
    pub(super) reader: IndexReader,
    pub(super) writer: IndexWriter,
    pub(super) directory: KvTantivyDirectory,
    pub(super) published_outbox_seq: u64,
    pub(super) durable_outbox_seq: u64,
    pub(super) published_backfill_cursor: Option<String>,
    pub(super) backfill_complete: bool,
    pub(super) key_field: Field,
    pub(super) expires_at_field: Field,
    pub(super) text_fields: Vec<Field>,
    pub(super) text_variant_fields: HashMap<Field, Field>,
    pub(super) text_field_settings: HashMap<Field, FullTextTextFieldSettings>,
    pub(super) tag_field_settings: HashMap<Field, FullTextTagFieldSettings>,
    pub(super) synonyms: HashMap<String, HashSet<String>>,
    pub(super) source_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) query_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) presence_fields: HashMap<String, Field>,
    pub(super) empty_fields: HashMap<String, Field>,
    pub(super) geo_fields: HashMap<String, (Field, Field)>,
    pub(super) geoshape_fields: HashMap<String, [Field; 4]>,
    pub(super) sortable_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) default_language: String,
    pub(super) language_field: Option<String>,
    pub(super) no_fields: bool,
    pub(super) has_positions: bool,
    pub(super) min_prefix: usize,
    pub(super) max_expansions: usize,
    pub(super) max_prefix_expansions: u32,
    pub(super) last_refresh_at: Instant,
}

pub(super) struct FullTextRuntimeConfig {
    pub(super) writer_heap_bytes: usize,
    pub(super) directory_cache_bytes: usize,
    pub(super) merge_min_segments: usize,
    pub(super) merge_max_docs: usize,
    pub(super) merge_min_layer_docs: usize,
    pub(super) merge_delete_ratio: f32,
    pub(super) min_prefix: usize,
    pub(super) max_expansions: usize,
    pub(super) max_prefix_expansions: u32,
}

#[derive(Clone, Debug)]
pub(super) struct FullTextTextFieldSettings {
    pub(super) nostem: bool,
    pub(super) phonetic: bool,
    pub(super) with_suffix_trie: bool,
    pub(super) stopwords: HashSet<String>,
    pub(super) language: String,
    pub(super) weight: f32,
}

#[derive(Clone, Debug)]
pub(super) struct FullTextTagFieldSettings {
    pub(super) separator: char,
    pub(super) case_sensitive: bool,
}
