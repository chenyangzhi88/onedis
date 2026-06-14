#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum VectorFieldKind {
    Tag,
    Numeric,
    Text,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct VectorFieldSchema {
    pub name: String,
    pub kind: VectorFieldKind,
    pub indexed: bool,
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum VectorDistance {
    Cosine,
    L2,
    Ip,
}

#[derive(Clone, Debug)]
pub struct VectorCreateOptions {
    pub dim: usize,
    pub distance: String,
    pub schema: Vec<VectorFieldSchema>,
    pub segment_max_docs: Option<u64>,
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_runtime: Option<usize>,
    pub initial_cap: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct VectorSearchOptions {
    pub k: usize,
    pub filter: Option<String>,
    pub with_scores: bool,
    pub with_attrs: Vec<String>,
    pub ef: Option<usize>,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub attrs: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorElement {
    pub vector: Vec<f32>,
    pub attrs_json: String,
}

#[derive(Default)]
pub struct VectorRuntimeRegistry {
    indexes: DashMap<VectorRuntimeKey, Arc<RwLock<VectorRuntime>>>,
    write_locks: DashMap<VectorWriteLockKey, Arc<Mutex<()>>>,
}

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

#[derive(Clone, Debug, Encode, Decode)]
struct VectorDocRecord {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
    attrs_json: String,
    deleted: bool,
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

#[derive(Clone, Debug)]
enum FilterPredicate {
    TagEq(String, String),
    TagIn(String, Vec<String>),
    NumericCmp(String, NumericOp, f64),
}

#[derive(Clone, Copy, Debug)]
enum NumericOp {
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct VectorRuntimeKey {
    db_index: u16,
    index: String,
    version: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct VectorWriteLockKey {
    db_index: u16,
    index: String,
}

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

struct VectorSegmentRuntime {
    meta: VectorSegmentMeta,
    graph: HnswGraph,
}

struct VectorRuntime {
    active: HnswGraph,
    segments: Vec<VectorSegmentRuntime>,
    next_segment_id: u64,
}

#[derive(Clone)]
struct HnswNode {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
    deleted: bool,
}

struct HnswGraph {
    dim: usize,
    distance: VectorDistance,
    m: usize,
    ef_construction: usize,
    nodes: Vec<HnswNode>,
    id_to_pos: HashMap<String, usize>,
    backend: HnswBackend,
}

enum HnswBackend {
    L2(Hnsw<'static, f32, DistL2>),
    Cosine(Hnsw<'static, f32, DistCosine>),
    Ip(Hnsw<'static, f32, DistInnerProduct>),
}

#[derive(Clone, Copy, Default)]
struct DistInnerProduct;

impl Distance<f32> for DistInnerProduct {
    fn eval(&self, left: &[f32], right: &[f32]) -> f32 {
        let dot = left.iter().zip(right).map(|(a, b)| a * b).sum::<f32>();
        1.0 / (1.0 + dot.exp())
    }
}

impl HnswGraph {
    fn new(
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
    ) -> Self {
        let m = m.clamp(1, 256);
        let ef_construction = ef_construction.max(m);
        let initial_cap = initial_cap.max(1);
        Self {
            dim,
            distance,
            m,
            ef_construction,
            nodes: Vec::new(),
            id_to_pos: HashMap::new(),
            backend: HnswBackend::new(distance, m, initial_cap, ef_construction),
        }
    }

    fn upsert(&mut self, id: String, doc_version: u64, vector: Vec<f32>) -> Result<(), Error> {
        validate_vector(&vector, self.dim)?;
        if let Some(pos) = self.id_to_pos.get(&id).copied() {
            self.nodes[pos].deleted = true;
        }
        validate_vector_for_distance(&vector, self.distance)?;
        let pos = self.nodes.len();
        self.nodes.push(HnswNode {
            id: id.clone(),
            doc_version,
            vector: vector.clone(),
            deleted: false,
        });
        self.id_to_pos.insert(id, pos);
        self.backend.insert(&vector, pos);
        Ok(())
    }

    fn mark_deleted(&mut self, id: &str) {
        if let Some(pos) = self.id_to_pos.get(id).copied() {
            self.nodes[pos].deleted = true;
        }
    }

    fn search(
        &self,
        query: &[f32],
        limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        validate_vector(query, self.dim)?;
        validate_vector_for_distance(query, self.distance)?;
        if self.len() == 0 || limit == 0 {
            return Ok(Vec::new());
        }
        let filter = |origin_id: &usize| {
            self.nodes.get(*origin_id).is_some_and(|node| {
                !node.deleted
                    && allow_doc_ids.is_none_or(|allow_doc_ids| allow_doc_ids.contains(&node.id))
            })
        };
        let neighbours = self.backend.search(
            query,
            limit,
            ef.max(limit),
            allow_doc_ids.map(|_| &filter as &dyn hnsw_rs::filter::FilterT),
        );
        Ok(neighbours
            .into_iter()
            .filter_map(|neighbour| {
                let pos = neighbour.d_id;
                let node = self.nodes.get(pos)?;
                if node.deleted {
                    return None;
                }
                Some(VectorCandidate {
                    id: node.id.clone(),
                    doc_version: node.doc_version,
                    distance: neighbour.distance,
                })
            })
            .take(limit)
            .collect())
    }

    fn len(&self) -> usize {
        self.nodes.iter().filter(|node| !node.deleted).count()
    }

    fn max_doc_version(&self) -> u64 {
        self.nodes
            .iter()
            .filter(|node| !node.deleted)
            .map(|node| node.doc_version)
            .max()
            .unwrap_or(0)
    }

    fn to_snapshot(&self) -> HnswGraphSnapshot {
        HnswGraphSnapshot {
            dim: self.dim as u32,
            distance: self.distance,
            m: self.m as u32,
            ef_construction: self.ef_construction as u32,
            nodes: self
                .nodes
                .iter()
                .map(|node| HnswSnapshotNode {
                    id: node.id.clone(),
                    doc_version: node.doc_version,
                    vector: node.vector.clone(),
                    deleted: node.deleted,
                })
                .collect(),
        }
    }

    fn from_snapshot(snapshot: HnswGraphSnapshot) -> Result<Self, Error> {
        let mut graph = HnswGraph::new(
            snapshot.dim as usize,
            snapshot.distance,
            snapshot.m as usize,
            snapshot.ef_construction as usize,
            snapshot.nodes.len().max(1),
        );
        for node in snapshot.nodes {
            if node.deleted {
                graph.nodes.push(HnswNode {
                    id: node.id,
                    doc_version: node.doc_version,
                    vector: node.vector,
                    deleted: true,
                });
                continue;
            }
            graph.upsert(node.id, node.doc_version, node.vector)?;
        }
        Ok(graph)
    }
}

impl HnswBackend {
    fn new(distance: VectorDistance, m: usize, initial_cap: usize, ef_construction: usize) -> Self {
        match distance {
            VectorDistance::L2 => HnswBackend::L2(Hnsw::<f32, DistL2>::new(
                m,
                initial_cap,
                DEFAULT_HNSW_MAX_LAYER,
                ef_construction,
                DistL2 {},
            )),
            VectorDistance::Cosine => HnswBackend::Cosine(Hnsw::<f32, DistCosine>::new(
                m,
                initial_cap,
                DEFAULT_HNSW_MAX_LAYER,
                ef_construction,
                DistCosine {},
            )),
            VectorDistance::Ip => HnswBackend::Ip(Hnsw::<f32, DistInnerProduct>::new(
                m,
                initial_cap,
                DEFAULT_HNSW_MAX_LAYER,
                ef_construction,
                DistInnerProduct,
            )),
        }
    }

    fn insert(&self, vector: &[f32], origin_id: usize) {
        match self {
            HnswBackend::L2(index) => index.insert((vector, origin_id)),
            HnswBackend::Cosine(index) => index.insert((vector, origin_id)),
            HnswBackend::Ip(index) => index.insert((vector, origin_id)),
        }
    }

    fn search(
        &self,
        query: &[f32],
        limit: usize,
        ef: usize,
        filter: Option<&dyn hnsw_rs::filter::FilterT>,
    ) -> Vec<hnsw_rs::prelude::Neighbour> {
        match self {
            HnswBackend::L2(index) => index.search_filter(query, limit, ef, filter),
            HnswBackend::Cosine(index) => index.search_filter(query, limit, ef, filter),
            HnswBackend::Ip(index) => index.search_filter(query, limit, ef, filter),
        }
    }
}

impl VectorRuntime {
    fn new(
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
        next_segment_id: u64,
    ) -> Self {
        Self {
            active: HnswGraph::new(dim, distance, m, ef_construction, initial_cap),
            segments: Vec::new(),
            next_segment_id,
        }
    }

    fn with_segments(
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
        next_segment_id: u64,
        segments: Vec<VectorSegmentRuntime>,
    ) -> Self {
        Self {
            active: HnswGraph::new(dim, distance, m, ef_construction, initial_cap),
            segments,
            next_segment_id,
        }
    }

    fn upsert(&mut self, id: String, doc_version: u64, vector: Vec<f32>) -> Result<(), Error> {
        self.active.upsert(id, doc_version, vector)
    }

    fn mark_deleted(&mut self, id: &str) {
        self.active.mark_deleted(id);
        for segment in &mut self.segments {
            segment.graph.mark_deleted(id);
        }
    }

    fn len(&self) -> usize {
        self.active.len()
            + self
                .segments
                .iter()
                .map(|segment| segment.graph.len())
                .sum::<usize>()
    }

    fn search(
        &self,
        query: &[f32],
        candidate_limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        let mut candidates = Vec::new();
        for segment in &self.segments {
            if segment.graph.len() == 0 {
                continue;
            }
            let limit = candidate_limit.min(segment.graph.len());
            candidates.extend(
                segment
                    .graph
                    .search(query, limit, ef.max(limit), allow_doc_ids)?,
            );
        }
        if self.active.len() > 0 {
            let limit = candidate_limit.min(self.active.len());
            candidates.extend(
                self.active
                    .search(query, limit, ef.max(limit), allow_doc_ids)?,
            );
        }
        reduce_vector_candidates(candidates, candidate_limit)
    }

    fn freeze_active(&mut self) -> Option<(VectorSegmentMeta, HnswGraphSnapshot)> {
        if self.active.len() == 0 {
            return None;
        }
        let segment_id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        let dim = self.active.dim;
        let distance = self.active.distance;
        let m = self.active.m;
        let ef_construction = self.active.ef_construction;
        let frozen = std::mem::replace(
            &mut self.active,
            HnswGraph::new(dim, distance, m, ef_construction, 1),
        );
        let max_doc_version = frozen.max_doc_version();
        let snapshot = frozen.to_snapshot();
        let segment = VectorSegmentMeta {
            segment_id,
            graph_key: Vec::new(),
            doc_count: snapshot.nodes.iter().filter(|node| !node.deleted).count() as u64,
            max_doc_version,
        };
        self.segments.push(VectorSegmentRuntime {
            meta: segment.clone(),
            graph: frozen,
        });
        Some((segment, snapshot))
    }

    fn set_segment_graph_key(&mut self, segment_id: u64, graph_key: Vec<u8>) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.meta.segment_id == segment_id)
        {
            segment.meta.graph_key = graph_key;
        }
    }

    fn remove_segments(&mut self, segment_ids: &HashSet<u64>) {
        self.segments
            .retain(|segment| !segment_ids.contains(&segment.meta.segment_id));
    }
}

impl VectorRuntimeRegistry {
    fn key(db_index: u16, index: &str, version: u64) -> VectorRuntimeKey {
        VectorRuntimeKey {
            db_index,
            index: index.to_string(),
            version,
        }
    }

    fn reset(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
    ) {
        self.indexes.insert(
            Self::key(db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::new(
                dim,
                distance,
                m,
                ef_construction,
                initial_cap,
                1,
            ))),
        );
    }

    fn write_lock(&self, db_index: u16, index: &str) -> Arc<Mutex<()>> {
        self.write_locks
            .entry(VectorWriteLockKey {
                db_index,
                index: index.to_string(),
            })
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone()
    }

    fn get(&self, db_index: u16, index: &str, version: u64) -> Option<Arc<RwLock<VectorRuntime>>> {
        self.indexes
            .get(&Self::key(db_index, index, version))
            .map(|entry| entry.value().clone())
    }

    fn upsert(
        &self,
        db_index: u16,
        index: &str,
        version: u64,
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
        id: String,
        doc_version: u64,
        vector: Vec<f32>,
    ) -> Result<(), Error> {
        let runtime = self
            .indexes
            .entry(Self::key(db_index, index, version))
            .or_insert_with(|| {
                Arc::new(RwLock::new(VectorRuntime::new(
                    dim,
                    distance,
                    m,
                    ef_construction,
                    initial_cap,
                    1,
                )))
            })
            .value()
            .clone();
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .upsert(id, doc_version, vector)
    }

    fn mark_deleted(&self, db_index: u16, index: &str, version: u64, id: &str) {
        if let Some(runtime) = self.get(db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.mark_deleted(id);
        }
    }

    fn remove(&self, db_index: u16, index: &str, version: u64) {
        self.indexes.remove(&Self::key(db_index, index, version));
    }
}

