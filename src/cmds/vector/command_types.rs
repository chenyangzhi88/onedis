pub struct VAdd {
    pub key: String,
    pub element: String,
    pub vector: Vec<f32>,
    pub attrs_json: Option<String>,
    pub m: Option<usize>,
    pub ef: Option<usize>,
}

pub struct VSim {
    pub key: String,
    pub query: VSimQuery,
    pub with_scores: bool,
    pub with_attrs: bool,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub epsilon: Option<f32>,
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
