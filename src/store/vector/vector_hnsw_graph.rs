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
