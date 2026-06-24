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
