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
