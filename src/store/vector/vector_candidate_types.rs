#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct VectorCandidateKey {
    id: String,
    doc_version: u64,
}

#[derive(Clone, Debug)]
struct VectorCandidate {
    id: String,
    doc_version: u64,
    distance: f32,
}
