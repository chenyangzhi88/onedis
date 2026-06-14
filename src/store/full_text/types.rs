#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum FullTextFieldKind {
    Text,
    Tag,
    Numeric,
    Geo,
    GeoShape,
    Vector,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum FullTextSourceType {
    Hash,
    Json,
}

#[derive(Clone, Debug, Default, Encode, Decode, PartialEq)]
pub struct FullTextFieldOptions {
    pub alias: Option<String>,
    pub sortable: bool,
    pub sortable_unf: bool,
    pub noindex: bool,
    pub weight: Option<f32>,
    pub nostem: bool,
    pub phonetic: Option<String>,
    pub separator: Option<String>,
    pub case_sensitive: bool,
    pub with_suffix_trie: bool,
    pub index_empty: bool,
    pub index_missing: bool,
    pub geoshape_coordinate_system: Option<FullTextGeoShapeCoordinateSystem>,
    pub vector: Option<FullTextVectorOptions>,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum FullTextGeoShapeCoordinateSystem {
    Flat,
    Spherical,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum FullTextVectorAlgorithm {
    Flat,
    Hnsw,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct FullTextVectorOptions {
    pub algorithm: FullTextVectorAlgorithm,
    pub attributes: Vec<(String, String)>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct FullTextFieldSchema {
    pub name: String,
    pub kind: FullTextFieldKind,
    pub options: FullTextFieldOptions,
}

impl FullTextFieldSchema {
    pub fn attribute_name(&self) -> &str {
        self.options.alias.as_deref().unwrap_or(&self.name)
    }
}

#[derive(Clone, Debug)]
pub struct FullTextCreateOptions {
    pub source_type: FullTextSourceType,
    pub prefixes: Vec<String>,
    pub schema: Vec<FullTextFieldSchema>,
    pub index_options: FullTextIndexOptions,
}

#[derive(Clone, Debug, Default, Encode, Decode)]
pub struct FullTextIndexOptions {
    pub skip_initial_scan: bool,
    pub filter: Option<String>,
    pub language: Option<String>,
    pub language_field: Option<String>,
    pub score: Option<f64>,
    pub score_field: Option<String>,
    pub payload_field: Option<String>,
    pub max_text_fields: bool,
    pub temporary_seconds: Option<u64>,
    pub no_offsets: bool,
    pub no_hl: bool,
    pub no_fields: bool,
    pub no_freqs: bool,
    pub stopwords: Option<Vec<String>>,
    pub index_all: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct FullTextSearchOptions {
    pub offset: usize,
    pub limit: usize,
    pub return_fields: Option<Vec<FullTextReturnField>>,
    pub no_content: bool,
    pub with_scores: bool,
    pub with_payloads: bool,
    pub with_sort_keys: bool,
    pub filters: Vec<FullTextSearchNumericFilter>,
    pub geo_filters: Vec<FullTextSearchGeoFilter>,
    pub in_keys: Option<HashSet<String>>,
    pub in_fields: Option<Vec<String>>,
    pub sort_by: Option<FullTextSortBy>,
    pub timeout_ms: Option<u64>,
    pub slop: Option<u32>,
    pub inorder: bool,
    pub language: Option<String>,
    pub payload: Option<String>,
    pub summarize: bool,
    pub highlight: bool,
    pub explain_score: bool,
    pub params: HashMap<String, Vec<u8>>,
    pub dialect: u8,
    pub dialect_explicit: bool,
}

#[derive(Clone, Debug)]
pub struct FullTextReturnField {
    pub identifier: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FullTextSearchNumericFilter {
    pub field: String,
    pub min: FullTextSearchBound,
    pub max: FullTextSearchBound,
}

#[derive(Clone, Debug)]
pub struct FullTextSearchGeoFilter {
    pub field: String,
    pub lon: f64,
    pub lat: f64,
    pub radius: f64,
    pub unit: String,
}

#[derive(Clone, Copy, Debug)]
pub enum FullTextSearchBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

#[derive(Clone, Debug)]
pub struct FullTextSortBy {
    pub field: String,
    pub asc: bool,
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateOptions {
    pub search_options: FullTextSearchOptions,
    pub load: Option<Vec<FullTextAggregateLoadField>>,
    pub steps: Vec<FullTextAggregateStep>,
    pub sort_by: Vec<FullTextAggregateSortBy>,
    pub offset: usize,
    pub limit: usize,
    pub cursor_count: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateLoadField {
    pub identifier: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FullTextAggregateStep {
    Apply {
        expression: String,
        alias: String,
    },
    Filter {
        expression: String,
    },
    GroupBy {
        fields: Vec<String>,
        reducers: Vec<FullTextAggregateReducer>,
    },
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateReducer {
    pub kind: FullTextAggregateReducerKind,
    pub args: Vec<String>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FullTextAggregateReducerKind {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    FirstValue,
    ToList,
}

#[derive(Clone, Debug)]
pub struct FullTextAggregateSortBy {
    pub field: String,
    pub asc: bool,
}

#[derive(Clone, Debug)]
struct FullTextSearchHit {
    key: String,
    score: f32,
}

#[derive(Clone, Debug)]
struct FullTextLiveHit {
    key: String,
    score: f32,
    fields: Vec<(String, String)>,
    sort_key: Option<String>,
}

#[derive(Clone, Debug)]
struct FullTextAggregateRow {
    values: HashMap<String, FullTextAggregateValue>,
    output: Vec<(String, FullTextAggregateValue)>,
}

#[derive(Clone, Debug)]
enum FullTextAggregateValue {
    Null,
    String(String),
    Number(f64),
    List(Vec<FullTextAggregateValue>),
}

#[derive(Default)]
pub struct FullTextRuntimeRegistry {
    indexes: DashMap<FullTextRuntimeKey, Arc<RwLock<FullTextRuntime>>>,
}

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

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyFullTextIndexMeta {
    prefixes: Vec<String>,
    schema: Vec<LegacyFullTextFieldSchema>,
    state: FullTextIndexState,
    generation: u64,
    backfill_cursor: Option<String>,
    last_indexed_outbox_seq: u64,
    refresh_policy: FullTextRefreshPolicy,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyPhase2FullTextIndexMeta {
    source_type: FullTextSourceType,
    prefixes: Vec<String>,
    schema: Vec<LegacyPhase2FullTextFieldSchema>,
    aliases: Vec<String>,
    index_options: LegacyPhase2FullTextIndexOptions,
    state: FullTextIndexState,
    generation: u64,
    backfill_cursor: Option<String>,
    last_indexed_outbox_seq: u64,
    refresh_policy: FullTextRefreshPolicy,
}

#[derive(Clone, Debug, Default, Encode, Decode)]
struct LegacyPhase2FullTextIndexOptions {
    skip_initial_scan: bool,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyPhase2FullTextFieldSchema {
    name: String,
    kind: FullTextFieldKind,
    options: LegacyPhase2FullTextFieldOptions,
}

#[derive(Clone, Debug, Default, Encode, Decode)]
struct LegacyPhase2FullTextFieldOptions {
    alias: Option<String>,
    sortable: bool,
    noindex: bool,
    weight: Option<f32>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyFullTextFieldSchema {
    name: String,
    kind: FullTextFieldKind,
}

impl From<LegacyPhase2FullTextIndexMeta> for FullTextIndexMeta {
    fn from(value: LegacyPhase2FullTextIndexMeta) -> Self {
        Self {
            source_type: value.source_type,
            prefixes: value.prefixes,
            schema: value
                .schema
                .into_iter()
                .map(FullTextFieldSchema::from)
                .collect(),
            aliases: value.aliases,
            index_options: FullTextIndexOptions {
                skip_initial_scan: value.index_options.skip_initial_scan,
                ..FullTextIndexOptions::default()
            },
            state: value.state,
            generation: value.generation,
            backfill_cursor: value.backfill_cursor,
            last_indexed_outbox_seq: value.last_indexed_outbox_seq,
            refresh_policy: value.refresh_policy,
        }
    }
}

impl From<LegacyPhase2FullTextFieldSchema> for FullTextFieldSchema {
    fn from(value: LegacyPhase2FullTextFieldSchema) -> Self {
        Self {
            name: value.name,
            kind: value.kind,
            options: FullTextFieldOptions {
                alias: value.options.alias,
                sortable: value.options.sortable,
                noindex: value.options.noindex,
                weight: value.options.weight,
                ..FullTextFieldOptions::default()
            },
        }
    }
}

impl From<LegacyFullTextIndexMeta> for FullTextIndexMeta {
    fn from(value: LegacyFullTextIndexMeta) -> Self {
        Self {
            source_type: FullTextSourceType::Hash,
            prefixes: value.prefixes,
            schema: value
                .schema
                .into_iter()
                .map(|field| FullTextFieldSchema {
                    name: field.name,
                    kind: field.kind,
                    options: FullTextFieldOptions::default(),
                })
                .collect(),
            aliases: Vec::new(),
            index_options: FullTextIndexOptions::default(),
            state: value.state,
            generation: value.generation,
            backfill_cursor: value.backfill_cursor,
            last_indexed_outbox_seq: value.last_indexed_outbox_seq,
            refresh_policy: value.refresh_policy,
        }
    }
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
enum FullTextMutationKind {
    UpsertKey,
    DeleteKey,
    UpsertJson,
}

struct FullTextVectorPlan<'a> {
    kind: FullTextVectorPlanKind,
    filter: Option<&'a FullTextQueryAst>,
    field: String,
    blob_param: String,
}

#[derive(Clone, Copy)]
enum FullTextVectorPlanKind {
    Knn { k: usize },
    Range { radius: f32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FullTextJsonPathToken {
    Field(String),
    Index(usize),
    Wildcard,
}

