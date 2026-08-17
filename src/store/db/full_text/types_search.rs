use super::*;
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
    pub phonetic: Option<bool>,
    pub verbatim: bool,
    pub no_stopwords: bool,
    pub language: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub scorer: FullTextScorer,
    pub summarize: Option<FullTextSummarizeOptions>,
    pub highlight: Option<FullTextHighlightOptions>,
    pub explain_score: bool,
    pub params: HashMap<String, Vec<u8>>,
    pub dialect: u8,
    pub dialect_explicit: bool,
    pub vector_ef_runtime: Option<usize>,
    pub vector_filter_ef: Option<usize>,
    pub vector_epsilon: f32,
}

#[derive(Clone, Debug)]
pub struct FullTextSummarizeOptions {
    pub fields: Option<HashSet<String>>,
    pub frags: usize,
    pub len: usize,
    pub separator: String,
}

impl Default for FullTextSummarizeOptions {
    fn default() -> Self {
        Self {
            fields: None,
            frags: 1,
            len: 20,
            separator: "... ".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FullTextHighlightOptions {
    pub fields: Option<HashSet<String>>,
    pub open_tag: String,
    pub close_tag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullTextSpellcheckDictionary {
    pub name: String,
    pub terms: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum FullTextHybridCombine {
    Rrf {
        window: usize,
        constant: f32,
    },
    Linear {
        window: usize,
        alpha: f32,
        beta: f32,
    },
}

#[derive(Clone, Debug)]
pub struct FullTextHybridOptions {
    pub search: FullTextSearchOptions,
    pub combine: FullTextHybridCombine,
    pub search_score_alias: Option<String>,
    pub vector_score_alias: Option<String>,
    pub combined_score_alias: Option<String>,
    pub post_filter: Option<String>,
    pub no_sort: bool,
}

impl Default for FullTextHighlightOptions {
    fn default() -> Self {
        Self {
            fields: None,
            open_tag: "<b>".to_string(),
            close_tag: "</b>".to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FullTextScorer {
    Bm25,
    #[default]
    Bm25Std,
    DisMax,
    DocScore,
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
pub(super) struct FullTextSearchHit {
    pub(super) key: String,
    pub(super) score: f32,
}

pub(super) struct FullTextSearchHits {
    pub(super) total: usize,
    pub(super) hits: Vec<FullTextSearchHit>,
    pub(super) timed_out: bool,
}

pub(super) struct FullTextCollectedHits {
    pub(super) total: usize,
    pub(super) hits: Vec<FullTextLiveHit>,
}

#[derive(Clone, Copy)]
pub(super) struct FullTextSearchLimits {
    pub(super) timeout: FullTextSearchDeadline,
    pub(super) result_cap: usize,
    pub(super) reader_budget: usize,
}

#[derive(Clone, Copy)]
pub(super) struct FullTextSearchDeadline {
    pub(super) at: Instant,
    pub(super) fail_on_timeout: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FullTextCollectMode {
    Page,
    All,
    Window(usize),
}

#[derive(Clone, Debug)]
pub(super) struct FullTextLiveHit {
    pub(super) key: String,
    pub(super) score: f32,
    pub(super) fields: Vec<(String, String)>,
    pub(super) sort_key: Option<String>,
    pub(super) payload: Option<String>,
}
