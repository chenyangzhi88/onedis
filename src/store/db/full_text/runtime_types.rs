use super::*;
#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextMutationRecord {
    pub(super) incarnation: u64,
    pub(super) kind: FullTextMutationKind,
    pub(super) key: String,
    pub(super) projection: Option<FullTextIndexedProjection>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextMutationRecordV1 {
    pub(super) incarnation: u64,
    pub(super) kind: FullTextMutationKind,
    pub(super) key: String,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextMutationBatchRecord {
    pub(super) incarnation: u64,
    pub(super) kind: FullTextMutationKind,
    pub(super) keys: Vec<String>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextIndexedProjection {
    pub(super) fields: Vec<(String, String)>,
    pub(super) expires_at_ms: u64,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextProjectedMutation {
    pub(super) key: String,
    pub(super) projection: FullTextIndexedProjection,
}

#[derive(Clone, Debug, Encode, Decode)]
pub(super) struct FullTextProjectedMutationBatchRecord {
    pub(super) incarnation: u64,
    pub(super) kind: FullTextMutationKind,
    pub(super) mutations: Vec<FullTextProjectedMutation>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct FullTextRuntimeKey {
    pub(super) db_index: u16,
    pub(super) index: String,
}

pub(super) struct FullTextRuntime {
    pub(super) search: Arc<FullTextSearchGeneration>,
    pub(super) writer: IndexWriter,
    pub(super) directory: KvTantivyDirectory,
    pub(super) published_outbox_seq: u64,
    pub(super) durable_outbox_seq: u64,
    pub(super) published_backfill_cursor: Option<String>,
    pub(super) backfill_complete: bool,
    pub(super) writer_synonyms: HashMap<String, HashSet<String>>,
    pub(super) last_refresh_at: Instant,
    pub(super) last_checkpoint_at: Instant,
}

/// Immutable query-facing state for one index incarnation.
///
/// `IndexReader` reloads internally, while schema-derived fields and query
/// configuration remain immutable for the lifetime of the incarnation.
pub(super) struct FullTextSearchGeneration {
    pub(super) incarnation: u64,
    pub(super) search_meta: FullTextIndexMeta,
    pub(super) index: Index,
    pub(super) reader: IndexReader,
    pub(super) key_field: Field,
    pub(super) expires_at_field: Field,
    pub(super) text_fields: Vec<Field>,
    pub(super) text_variant_fields: HashMap<Field, Field>,
    pub(super) text_field_settings: HashMap<Field, FullTextTextFieldSettings>,
    pub(super) tag_field_settings: HashMap<Field, FullTextTagFieldSettings>,
    pub(super) source_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) query_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) presence_fields: HashMap<String, Field>,
    pub(super) empty_fields: HashMap<String, Field>,
    pub(super) geo_fields: HashMap<String, (Field, Field)>,
    pub(super) geoshape_fields: HashMap<String, FullTextGeoShapeFields>,
    pub(super) sortable_fields: HashMap<String, (Field, FullTextFieldKind)>,
    pub(super) default_language: String,
    pub(super) language_field: Option<String>,
    pub(super) no_fields: bool,
    pub(super) has_positions: bool,
    pub(super) min_prefix: usize,
    pub(super) max_expansions: usize,
    pub(super) max_prefix_expansions: u32,
    pub(super) has_expiring_documents: AtomicBool,
    pub(super) retired: AtomicBool,
    pub(super) expansion_terms: Mutex<HashMap<String, Arc<FullTextExpansionCacheEntry>>>,
}

impl std::ops::Deref for FullTextRuntime {
    type Target = FullTextSearchGeneration;

    fn deref(&self) -> &Self::Target {
        &self.search
    }
}

impl FullTextRuntime {
    pub(super) fn search_generation(&self) -> Arc<FullTextSearchGeneration> {
        Arc::clone(&self.search)
    }
}

impl FullTextSearchGeneration {
    pub(super) fn ensure_active(&self) -> Result<(), Error> {
        if self.retired.load(AtomicOrdering::Acquire) {
            Err(Error::msg("ERR fulltext index generation was retired"))
        } else {
            Ok(())
        }
    }

    pub(super) fn retire(&self) {
        self.retired.store(true, AtomicOrdering::Release);
        if let Ok(mut cache) = self.expansion_terms.lock() {
            cache.clear();
        }
    }
}

pub(super) struct FullTextPreparedDocument {
    pub(super) key: String,
    pub(super) document: TantivyDocument,
    pub(super) indexed_bytes: usize,
    pub(super) expires_at_ms: u64,
}

pub(super) struct FullTextExpansionCacheEntry {
    pub(super) terms: Vec<(Field, String)>,
    pub(super) unique_term_count: usize,
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

#[derive(Clone, Copy)]
pub(super) struct FullTextGeoShapeFields {
    pub(super) bounds: [Field; 4],
    pub(super) cells: Field,
}
