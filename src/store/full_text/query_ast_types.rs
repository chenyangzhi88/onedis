#[derive(Clone, Debug)]
enum FullTextQueryAst {
    All,
    Text(String),
    Phrase(String),
    Prefix(String),
    Wildcard(String),
    Fuzzy(String),
    Tag {
        field: String,
        values: Vec<String>,
    },
    Numeric {
        field: String,
        min: FullTextNumericBound,
        max: FullTextNumericBound,
    },
    Geo {
        field: String,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: String,
    },
    GeoShape {
        field: String,
        relation: String,
        shape: String,
    },
    VectorRange {
        field: String,
        radius: f64,
        blob_param: String,
    },
    VectorKnn {
        filter: Box<FullTextQueryAst>,
        k: usize,
        field: String,
        blob_param: String,
    },
    Field {
        fields: Vec<String>,
        expr: Box<FullTextQueryAst>,
    },
    And(Vec<FullTextQueryAst>),
    Or(Vec<FullTextQueryAst>),
    Not(Box<FullTextQueryAst>),
    Optional(Box<FullTextQueryAst>),
    Attributed {
        expr: Box<FullTextQueryAst>,
        weight: Option<f32>,
    },
}

#[derive(Clone, Copy, Debug)]
enum FullTextNumericBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}
