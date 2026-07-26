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
        let dot = left
            .iter()
            .zip(right)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>();
        (-dot) as f32
    }
}

fn hnsw_vector(distance: VectorDistance, vector: &[f32]) -> Vec<f32> {
    if distance != VectorDistance::Cosine {
        return vector.to_vec();
    }
    let norm = vector
        .iter()
        .map(|value| {
            let value = f64::from(*value);
            value * value
        })
        .sum::<f64>()
        .sqrt();
    vector
        .iter()
        .map(|value| (f64::from(*value) / norm) as f32)
        .collect()
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
        let replaces_existing = if let Some(pos) = self.id_to_pos.get(&id).copied() {
            self.nodes[pos].deleted = true;
            true
        } else {
            false
        };
        validate_vector_for_distance(&vector, self.distance)?;
        let pos = self.nodes.len();
        self.nodes.push(HnswNode {
            id: id.clone(),
            doc_version,
            vector: vector.clone(),
            deleted: false,
        });
        self.id_to_pos.insert(id, pos);
        if replaces_existing {
            // hnsw_rs has no physical replacement operation. Rebuilding the
            // bounded active graph prevents a fresh version from connecting
            // only to its tombstoned predecessor, which otherwise makes
            // VLINKS and small-graph searches nondeterministic.
            self.rebuild_backend();
        } else {
            self.backend.insert(&hnsw_vector(self.distance, &vector), pos);
        }
        Ok(())
    }

    fn rebuild_backend(&mut self) {
        self.backend = HnswBackend::new(
            self.distance,
            self.m,
            self.nodes.len().max(1),
            self.ef_construction,
        );
        self.id_to_pos.clear();
        for (pos, node) in self.nodes.iter().enumerate() {
            if node.deleted {
                continue;
            }
            self.id_to_pos.insert(node.id.clone(), pos);
            self.backend
                .insert(&hnsw_vector(self.distance, &node.vector), pos);
        }
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
        let backend_query = hnsw_vector(self.distance, query);
        let neighbours = self.backend.search(
            &backend_query,
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

    fn links(&self, id: &str) -> Option<Vec<Vec<(String, f32)>>> {
        let origin_id = self.id_to_pos.get(id).copied()?;
        if self.nodes.get(origin_id).is_none_or(|node| node.deleted) {
            return None;
        }
        let mut layers = self
            .backend
            .neighborhood(origin_id)
            .into_iter()
            .map(|layer| {
                layer
                    .into_iter()
                    .filter_map(|neighbor| {
                        let node = self.nodes.get(neighbor.d_id)?;
                        (!node.deleted && node.id != id)
                            .then(|| (node.id.clone(), neighbor.distance))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        // hnsw_rs may retain an outgoing edge to a tombstoned version of an
        // updated document while live nodes still have real incoming edges to
        // the replacement. VLINKS is a graph-neighborhood view, so include
        // those incoming edges as well instead of intermittently reporting no
        // live links for the replacement node.
        for (candidate_id, candidate) in self.nodes.iter().enumerate() {
            if candidate_id == origin_id || candidate.deleted {
                continue;
            }
            for (layer_index, layer) in self
                .backend
                .neighborhood(candidate_id)
                .into_iter()
                .enumerate()
            {
                let Some(edge) = layer.into_iter().find(|edge| edge.d_id == origin_id) else {
                    continue;
                };
                if layers.len() <= layer_index {
                    layers.resize_with(layer_index + 1, Vec::new);
                }
                if !layers[layer_index]
                    .iter()
                    .any(|(neighbor_id, _)| neighbor_id == &candidate.id)
                {
                    layers[layer_index].push((candidate.id.clone(), edge.distance));
                }
            }
        }
        while layers.last().is_some_and(Vec::is_empty) {
            layers.pop();
        }
        Some(layers)
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
        if snapshot.dim == 0
            || snapshot.dim as usize > MAX_VECTOR_DIMENSIONS
            || snapshot.m == 0
            || snapshot.m > 256
            || snapshot.ef_construction < snapshot.m
            || snapshot.ef_construction as usize > MAX_VECTOR_HNSW_EF
            || snapshot.nodes.len() > MAX_VECTOR_INITIAL_CAP
        {
            return Err(Error::msg("ERR invalid persisted vector graph"));
        }
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

    fn neighborhood(&self, origin_id: usize) -> Vec<Vec<hnsw_rs::prelude::Neighbour>> {
        macro_rules! neighborhood {
            ($index:expr) => {
                $index
                    .get_point_indexation()
                    .into_iter()
                    .find(|point| point.get_origin_id() == origin_id)
                    .map(|point| point.get_neighborhood_id())
                    .unwrap_or_default()
            };
        }
        match self {
            HnswBackend::L2(index) => neighborhood!(index),
            HnswBackend::Cosine(index) => neighborhood!(index),
            HnswBackend::Ip(index) => neighborhood!(index),
        }
    }
}
