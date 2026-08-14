#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum VectorFieldKind {
    Tag,
    Numeric,
    Text,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct VectorFieldSchema {
    pub name: String,
    pub kind: VectorFieldKind,
    pub indexed: bool,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum VectorDistance {
    Cosine,
    L2,
    Ip,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum VectorQuantization {
    /// Keep the HNSW input in full precision.
    F32,
    /// Symmetric per-vector signed 8-bit quantization.  Search results are
    /// always reranked with the original persisted FP32 vector.
    Q8,
    /// One-bit sign quantization for candidate generation, followed by exact
    /// FP32 reranking.
    Binary,
}

#[derive(Clone, Debug)]
pub struct VectorCreateOptions {
    pub dim: usize,
    /// Original input dimension when vectors are reduced into `dim` through
    /// the index's persisted random projection.
    pub source_dim: Option<usize>,
    pub distance: String,
    pub schema: Vec<VectorFieldSchema>,
    pub segment_max_docs: Option<u64>,
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_runtime: Option<usize>,
    pub initial_cap: Option<usize>,
    pub quantization: VectorQuantization,
}

#[derive(Clone, Debug)]
pub struct VectorSearchOptions {
    pub k: usize,
    pub filter: Option<String>,
    pub with_scores: bool,
    pub with_attrs: Vec<String>,
    pub with_attrs_json: bool,
    pub ef: Option<usize>,
    pub filter_ef: Option<usize>,
    pub exact: bool,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub attrs: Vec<(String, String)>,
    pub attrs_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorElement {
    pub vector: Vec<f32>,
    pub attrs_json: String,
}
