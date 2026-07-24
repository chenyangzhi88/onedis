use super::*;
#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub(super) enum FullTextMutationKind {
    UpsertKey,
    DeleteKey,
    UpsertJson,
}

pub(super) struct FullTextVectorPlan {
    pub(super) kind: FullTextVectorPlanKind,
    pub(super) filter: Option<FullTextQueryAst>,
    pub(super) field: String,
    pub(super) blob_param: String,
}

#[derive(Clone, Copy)]
pub(super) enum FullTextVectorPlanKind {
    Knn { k: usize },
    Range { radius: f32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FullTextJsonPathToken {
    Field(String),
    Index(usize),
    Wildcard,
}
