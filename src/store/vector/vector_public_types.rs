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

#[derive(Clone, Debug)]
pub struct VectorCreateOptions {
    pub dim: usize,
    pub distance: String,
    pub schema: Vec<VectorFieldSchema>,
    pub segment_max_docs: Option<u64>,
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_runtime: Option<usize>,
    pub initial_cap: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct VectorSearchOptions {
    pub k: usize,
    pub filter: Option<String>,
    pub with_scores: bool,
    pub with_attrs: Vec<String>,
    pub ef: Option<usize>,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub attrs: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorElement {
    pub vector: Vec<f32>,
    pub attrs_json: String,
}
