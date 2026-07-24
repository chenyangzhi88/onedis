#[derive(Clone, Debug, Encode, Decode)]
struct VectorIndexMeta {
    dim: u32,
    distance: VectorDistance,
    schema: Vec<VectorFieldSchema>,
    m: u32,
    ef_construction: u32,
    ef_runtime: u32,
    initial_cap: u64,
    next_doc_version: u64,
    doc_count: u64,
    next_segment_id: u64,
    snapshot_doc_version: u64,
    segment_max_docs: u64,
}

#[derive(Clone, Copy)]
struct VectorRuntimeConfig {
    dim: usize,
    distance: VectorDistance,
    m: usize,
    ef_construction: usize,
    initial_cap: usize,
}

impl From<&VectorIndexMeta> for VectorRuntimeConfig {
    fn from(meta: &VectorIndexMeta) -> Self {
        Self {
            dim: meta.dim as usize,
            distance: meta.distance,
            m: meta.m as usize,
            ef_construction: meta.ef_construction as usize,
            initial_cap: meta.initial_cap as usize,
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorDocRecord {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
    attrs_json: String,
    deleted: bool,
}

struct VectorRuntimeEntry {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
}

impl From<&VectorDocRecord> for VectorRuntimeEntry {
    fn from(doc: &VectorDocRecord) -> Self {
        Self {
            id: doc.id.clone(),
            doc_version: doc.doc_version,
            vector: doc.vector.clone(),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorSegmentMeta {
    segment_id: u64,
    graph_key: Vec<u8>,
    doc_count: u64,
    max_doc_version: u64,
}

#[derive(Clone, Debug, Encode, Decode)]
struct HnswGraphSnapshot {
    dim: u32,
    distance: VectorDistance,
    m: u32,
    ef_construction: u32,
    nodes: Vec<HnswSnapshotNode>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct HnswSnapshotNode {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
    deleted: bool,
}
