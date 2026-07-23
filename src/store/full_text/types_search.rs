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
    pub scorer: FullTextScorer,
    pub summarize: bool,
    pub highlight: bool,
    pub explain_score: bool,
    pub params: HashMap<String, Vec<u8>>,
    pub dialect: u8,
    pub dialect_explicit: bool,
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
struct FullTextSearchHit {
    key: String,
    score: f32,
}

struct FullTextSearchHits {
    total: usize,
    hits: Vec<FullTextSearchHit>,
}

struct FullTextCollectedHits {
    total: usize,
    hits: Vec<FullTextLiveHit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FullTextCollectMode {
    Page,
    All,
}

#[derive(Clone, Debug)]
struct FullTextLiveHit {
    key: String,
    score: f32,
    fields: Vec<(String, String)>,
    sort_key: Option<String>,
    payload: Option<String>,
}
