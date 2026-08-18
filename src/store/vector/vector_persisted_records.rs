#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
enum VectorIndexAlgorithm {
    Hnsw,
    Flat,
}

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
    /// Largest compacted source segment. Same-level segments are merged
    /// until the next merge would cross this bound.
    max_segment_docs: u64,
    quantization: VectorQuantization,
    internal: bool,
    algorithm: VectorIndexAlgorithm,
}

/// Frequently changing collection state.  Keeping this separate prevents a
/// VADD/VREM from serializing and rewriting the immutable schema/HNSW config.
/// The legacy counter copies in `VectorIndexMeta` remain as a recovery fallback
/// for indexes created before this record existed.
#[derive(Clone, Copy, Debug, Encode, Decode)]
struct VectorMutableState {
    next_doc_version: u64,
    doc_count: u64,
}

impl VectorMutableState {
    fn from_meta(meta: &VectorIndexMeta) -> Self {
        Self {
            next_doc_version: meta.next_doc_version,
            doc_count: meta.doc_count,
        }
    }

    fn apply_to(self, meta: &mut VectorIndexMeta) {
        meta.next_doc_version = self.next_doc_version;
        meta.doc_count = self.doc_count;
    }
}

/// Vector metadata written before the index algorithm became part of the
/// durable configuration.  Those indexes all used HNSW.
#[derive(Clone, Debug, Encode, Decode)]
struct LegacyVectorIndexMetaV1 {
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
    segment_max_docs: u64,
    max_segment_docs: u64,
    quantization: VectorQuantization,
    internal: bool,
    algorithm: VectorIndexAlgorithm,
}

impl From<LegacyVectorIndexMetaV1> for VectorIndexMeta {
    fn from(legacy: LegacyVectorIndexMetaV1) -> Self {
        Self {
            dim: legacy.dim,
            projection: legacy.projection,
            distance: legacy.distance,
            schema: legacy.schema,
            m: legacy.m,
            ef_construction: legacy.ef_construction,
            ef_runtime: legacy.ef_runtime,
            initial_cap: legacy.initial_cap,
            next_doc_version: legacy.next_doc_version,
            doc_count: legacy.doc_count,
            next_segment_id: legacy.next_segment_id,
            snapshot_doc_version: legacy.snapshot_doc_version,
            segment_max_docs: legacy.segment_max_docs,
            max_segment_docs: legacy.max_segment_docs,
            quantization: legacy.quantization,
            internal: legacy.internal,
            algorithm: VectorIndexAlgorithm::Hnsw,
        }
    }
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
struct VectorProjection {
    input_dim: u32,
    seed: u64,
}

#[derive(Clone, PartialEq, Eq)]
struct VectorRuntimeConfig {
    dim: usize,
    projection: Option<VectorProjection>,
    distance: VectorDistance,
    schema: Arc<[VectorFieldSchema]>,
    m: usize,
    ef_construction: usize,
    initial_cap: usize,
    quantization: VectorQuantization,
    internal: bool,
    algorithm: VectorIndexAlgorithm,
}

impl From<&VectorIndexMeta> for VectorRuntimeConfig {
    fn from(meta: &VectorIndexMeta) -> Self {
        Self {
            dim: meta.dim as usize,
            projection: meta.projection,
            distance: meta.distance,
            schema: Arc::from(meta.schema.clone()),
            m: meta.m as usize,
            ef_construction: meta.ef_construction as usize,
            initial_cap: meta.initial_cap as usize,
            quantization: meta.quantization,
            internal: meta.internal,
            algorithm: meta.algorithm,
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

#[derive(Clone, Debug, Encode, Decode)]
struct VectorVersionMutation {
    id: String,
    doc_version: u64,
    deleted: bool,
}

#[derive(Clone, Debug, Encode, Decode)]
struct VectorVersionCheckpoint {
    through_doc_version: u64,
    current_versions: Vec<(String, u64)>,
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
    ids: Vec<String>,
    doc_versions: Vec<u64>,
    vectors: Vec<HnswSnapshotVector>,
    /// Squared integer norm for every Q8 payload. Other quantizations store
    /// zero. Keeping this beside the packed payload avoids rescanning a node
    /// for every HNSW edge evaluation.
    q8_norms: Vec<u32>,
    /// Node -> layer range offsets. Length is node_count + 1.
    node_layer_offsets: Vec<u32>,
    /// Layer -> neighbor range offsets. Length is layer_count + 1.
    layer_neighbor_offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

/// Packed graph format used before cached Q8 norms were added.
#[derive(Clone, Debug, Encode, Decode)]
struct LegacyVectorHnswIndexBlobV2 {
    dim: u32,
    distance: VectorDistance,
    m: u32,
    ef_construction: u32,
    quantization: VectorQuantization,
    entry_point: u32,
    max_layer: u32,
    ids: Vec<String>,
    doc_versions: Vec<u64>,
    vectors: Vec<HnswSnapshotVector>,
    node_layer_offsets: Vec<u32>,
    layer_neighbor_offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyVectorHnswIndexBlobV1 {
    dim: u32,
    distance: VectorDistance,
    m: u32,
    ef_construction: u32,
    quantization: VectorQuantization,
    entry_point: u32,
    max_layer: u32,
    nodes: Vec<LegacyVectorHnswIndexNodeV1>,
}

#[derive(Clone, Debug, Encode, Decode)]
struct LegacyVectorHnswIndexNodeV1 {
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
