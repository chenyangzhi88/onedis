#[derive(Clone, Debug, Encode, Decode)]
struct VectorIndexMeta {
    dim: u32,
    projection: Option<VectorProjection>,
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
    /// Number of distinct mutations collected before an L0 source segment is
    /// published.  This is the vector LSM memtable flush threshold.
    segment_max_docs: u64,
    /// Largest compacted source segment.  Four same-level segments are merged
    /// until the next merge would cross this bound.
    max_segment_docs: u64,
    quantization: VectorQuantization,
    internal: bool,
}

#[derive(Clone, Copy, Debug, Encode, Decode)]
struct VectorProjection {
    input_dim: u32,
    seed: u64,
}

#[derive(Clone, Copy)]
struct VectorRuntimeConfig {
    dim: usize,
    distance: VectorDistance,
    m: usize,
    ef_construction: usize,
    initial_cap: usize,
    quantization: VectorQuantization,
}

impl From<&VectorIndexMeta> for VectorRuntimeConfig {
    fn from(meta: &VectorIndexMeta) -> Self {
        Self {
            dim: meta.dim as usize,
            distance: meta.distance,
            m: meta.m as usize,
            ef_construction: meta.ef_construction as usize,
            initial_cap: meta.initial_cap as usize,
            quantization: meta.quantization,
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
    attrs_json: String,
}

impl From<&VectorDocRecord> for VectorRuntimeEntry {
    fn from(doc: &VectorDocRecord) -> Self {
        Self {
            id: doc.id.clone(),
            doc_version: doc.doc_version,
            vector: doc.vector.clone(),
            attrs_json: doc.attrs_json.clone(),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorSegmentMeta {
    segment_id: u64,
    level: u32,
    source_key: Vec<u8>,
    /// Empty while the source segment is searched by brute force.  Once the
    /// background builder publishes this key, it points at a complete,
    /// directly searchable HNSW topology blob.
    index_key: Vec<u8>,
    doc_count: u64,
    min_doc_version: u64,
    max_doc_version: u64,
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorSegmentBlob {
    entries: Vec<VectorSegmentEntry>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorSegmentEntry {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
}

impl From<&VectorDocRecord> for VectorSegmentEntry {
    fn from(doc: &VectorDocRecord) -> Self {
        Self {
            id: doc.id.clone(),
            doc_version: doc.doc_version,
            vector: doc.vector.clone(),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorHnswIndexBlob {
    dim: u32,
    distance: VectorDistance,
    m: u32,
    ef_construction: u32,
    quantization: VectorQuantization,
    entry_point: u32,
    max_layer: u32,
    nodes: Vec<VectorHnswIndexNode>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorHnswIndexNode {
    id: String,
    doc_version: u64,
    /// The exact payload used while building the HNSW graph.  F32, Q8 and BIN
    /// indexes therefore reload without rebuilding or re-quantizing.
    vector: HnswSnapshotVector,
    /// Layer zero first.  Values are indexes into `VectorHnswIndexBlob.nodes`.
    layers: Vec<Vec<u32>>,
}

#[derive(Clone, Debug, Encode, Decode)]
enum HnswSnapshotVector {
    F32(Vec<f32>),
    Q8 { scale: f32, values: Vec<u8> },
    Binary { dimensions: u32, bits: Vec<u8> },
}
