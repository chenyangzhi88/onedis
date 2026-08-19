pub struct VAdd {
    pub key: String,
    pub element: String,
    pub vector: Vec<f32>,
    pub reduce_dim: Option<usize>,
    pub attrs_json: Option<String>,
    pub m: Option<usize>,
    pub ef: Option<usize>,
    pub cas: bool,
    pub quantization: Option<VectorQuantization>,
}

pub struct VSim {
    pub key: String,
    pub query: VSimQuery,
    pub with_scores: bool,
    pub with_attrs: bool,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter_ef: Option<usize>,
    pub rerank: Option<usize>,
    pub filter: Option<String>,
    pub epsilon: Option<f32>,
    pub truth: bool,
    pub no_thread: bool,
}

pub enum VSimQuery {
    Element(String),
    Vector(Vec<f32>),
}

pub struct VRem {
    pub key: String,
    pub element: String,
}

pub struct VCard {
    pub key: String,
}

pub struct VDim {
    pub key: String,
}

pub struct VEmb {
    pub key: String,
    pub element: String,
    pub raw: bool,
}

pub struct VGetAttr {
    pub key: String,
    pub element: String,
}

pub struct VSetAttr {
    pub key: String,
    pub element: String,
    pub attrs_json: Option<String>,
}

pub struct VInfo {
    pub key: String,
}

pub struct VRandMember {
    pub key: String,
    pub count: Option<i64>,
}

pub struct VLinks {
    pub key: String,
    pub element: String,
    pub with_scores: bool,
}

pub struct VIsMember {
    pub key: String,
    pub element: String,
}

pub struct VRange {
    pub key: String,
    pub start: String,
    pub end: String,
    pub count: usize,
}
