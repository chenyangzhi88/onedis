#[derive(Clone, Debug, Encode, Decode)]
struct FullTextMutationRecord {
    generation: u64,
    kind: FullTextMutationKind,
    key: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FullTextRuntimeKey {
    db_index: u16,
    index: String,
}

struct FullTextRuntime {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    key_field: Field,
    text_fields: Vec<Field>,
    text_field_settings: HashMap<Field, FullTextTextFieldSettings>,
    synonyms: HashMap<String, HashSet<String>>,
    source_fields: HashMap<String, (Field, FullTextFieldKind)>,
    query_fields: HashMap<String, (Field, FullTextFieldKind)>,
    last_refresh_at: Instant,
}

#[derive(Clone, Debug)]
struct FullTextTextFieldSettings {
    nostem: bool,
    phonetic: bool,
    with_suffix_trie: bool,
    stopwords: HashSet<String>,
}
