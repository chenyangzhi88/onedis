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
    live_count: usize,
    backend: HnswBackend,
    quantization: VectorQuantization,
    max_doc_version: u64,
    // Maximum squared norm used by the MIPS -> L2 transform.  Keeping the
    // radius fixed for every point in one graph preserves inner-product
    // ordering.  A larger vector rebuilds the bounded graph with a new radius.
    ip_radius_squared: f64,
}

#[derive(Clone, Copy, Debug)]
struct HnswSearchQueueItem {
    node: u32,
    distance: f32,
}

impl PartialEq for HnswSearchQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for HnswSearchQueueItem {}

impl PartialOrd for HnswSearchQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HnswSearchQueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.node.cmp(&other.node))
    }
}

enum HnswBackend {
    F32L2(Hnsw<'static, f32, DistL2>),
    F32Cosine(Hnsw<'static, f32, DistCosine>),
    // Maximum inner-product search is embedded into one extra L2 dimension:
    // data(x) = [x, sqrt(R^2 - ||x||^2)], query(q) = [q, 0].
    F32Ip(Hnsw<'static, f32, DistL2>),
    Q8(Hnsw<'static, u8, DistQ8>),
    Binary(Hnsw<'static, u8, DistPackedBinary>),
}

#[derive(Clone, Copy)]
struct DistQ8 {
    cosine: bool,
}

impl Distance<u8> for DistQ8 {
    fn eval(&self, lhs: &[u8], rhs: &[u8]) -> f32 {
        if lhs.len() < 4 || rhs.len() < 4 || lhs.len() != rhs.len() {
            return f32::MAX;
        }
        let lhs_scale = f32::from_le_bytes(lhs[..4].try_into().unwrap_or([0; 4]));
        let rhs_scale = f32::from_le_bytes(rhs[..4].try_into().unwrap_or([0; 4]));
        if self.cosine {
            let mut dot = 0.0f64;
            let mut lhs_norm = 0.0f64;
            let mut rhs_norm = 0.0f64;
            for (&lhs, &rhs) in lhs[4..].iter().zip(&rhs[4..]) {
                let lhs = f64::from((lhs as i8) as f32 * lhs_scale);
                let rhs = f64::from((rhs as i8) as f32 * rhs_scale);
                dot += lhs * rhs;
                lhs_norm += lhs * lhs;
                rhs_norm += rhs * rhs;
            }
            if lhs_norm == 0.0 || rhs_norm == 0.0 {
                return 1.0;
            }
            return (1.0 - (dot / (lhs_norm * rhs_norm).sqrt()).clamp(-1.0, 1.0)) as f32;
        }
        lhs[4..]
            .iter()
            .zip(&rhs[4..])
            .map(|(&lhs, &rhs)| {
                let lhs = (lhs as i8) as f32 * lhs_scale;
                let rhs = (rhs as i8) as f32 * rhs_scale;
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt()
    }
}

#[derive(Clone, Copy)]
struct DistPackedBinary;

impl Distance<u8> for DistPackedBinary {
    fn eval(&self, lhs: &[u8], rhs: &[u8]) -> f32 {
        if lhs.len() != rhs.len() || lhs.is_empty() {
            return f32::MAX;
        }
        let differing = lhs
            .iter()
            .zip(rhs)
            .map(|(lhs, rhs)| (lhs ^ rhs).count_ones())
            .sum::<u32>();
        differing as f32 / (lhs.len() * 8) as f32
    }
}

fn encode_q8_vector(vector: &[f32]) -> Vec<u8> {
    let max_abs = vector.iter().copied().map(f32::abs).fold(0.0f32, f32::max);
    let scale = if max_abs == 0.0 { 1.0 } else { max_abs / 127.0 };
    let mut encoded = Vec::with_capacity(vector.len() + 4);
    encoded.extend_from_slice(&scale.to_le_bytes());
    encoded.extend(
        vector
            .iter()
            .map(|value| (value / scale).round().clamp(-127.0, 127.0) as i8 as u8),
    );
    encoded
}

fn encode_binary_vector(vector: &[f32]) -> Vec<u8> {
    let mut encoded = vec![0u8; vector.len().div_ceil(8)];
    for (index, value) in vector.iter().enumerate() {
        if *value >= 0.0 {
            encoded[index / 8] |= 1 << (index % 8);
        }
    }
    encoded
}

fn snapshot_vector(vector: &[f32], quantization: VectorQuantization) -> HnswSnapshotVector {
    match quantization {
        VectorQuantization::F32 => HnswSnapshotVector::F32(vector.to_vec()),
        VectorQuantization::Q8 => {
            let encoded = encode_q8_vector(vector);
            HnswSnapshotVector::Q8 {
                scale: f32::from_le_bytes(encoded[..4].try_into().unwrap_or([0; 4])),
                values: encoded[4..].to_vec(),
            }
        }
        VectorQuantization::Binary => HnswSnapshotVector::Binary {
            dimensions: vector.len() as u32,
            bits: encode_binary_vector(vector),
        },
    }
}

fn hnsw_index_payload(
    distance: VectorDistance,
    vector: &[f32],
    ip_radius_squared: f64,
    quantization: VectorQuantization,
) -> HnswSnapshotVector {
    let indexed = hnsw_data_vector(distance, vector, ip_radius_squared);
    snapshot_vector(&indexed, quantization)
}

fn hnsw_query_payload(
    distance: VectorDistance,
    query: &[f32],
    quantization: VectorQuantization,
) -> HnswSnapshotVector {
    let indexed = hnsw_query_vector(distance, query);
    snapshot_vector(&indexed, quantization)
}

fn q8_snapshot_bytes(scale: f32, values: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(values.len() + 4);
    encoded.extend_from_slice(&scale.to_le_bytes());
    encoded.extend_from_slice(values);
    encoded
}

fn hnsw_payload_distance(
    distance: VectorDistance,
    left: &HnswSnapshotVector,
    right: &HnswSnapshotVector,
) -> Result<f32, Error> {
    let value = match (left, right) {
        (HnswSnapshotVector::F32(left), HnswSnapshotVector::F32(right)) => {
            let payload_distance = if distance == VectorDistance::Cosine {
                VectorDistance::Cosine
            } else {
                // IP vectors have already been embedded into L2 space.
                VectorDistance::L2
            };
            distance_score(payload_distance, left, right)?
        }
        (
            HnswSnapshotVector::Q8 {
                scale: left_scale,
                values: left_values,
            },
            HnswSnapshotVector::Q8 {
                scale: right_scale,
                values: right_values,
            },
        ) => DistQ8 {
            cosine: distance == VectorDistance::Cosine,
        }
        .eval(
            &q8_snapshot_bytes(*left_scale, left_values),
            &q8_snapshot_bytes(*right_scale, right_values),
        ),
        (
            HnswSnapshotVector::Binary {
                dimensions: left_dimensions,
                bits: left_bits,
            },
            HnswSnapshotVector::Binary {
                dimensions: right_dimensions,
                bits: right_bits,
            },
        ) if left_dimensions == right_dimensions => DistPackedBinary.eval(left_bits, right_bits),
        _ => return Err(Error::msg("ERR invalid persisted HNSW vector payload")),
    };
    if !value.is_finite() {
        return Err(Error::msg("ERR invalid persisted HNSW distance"));
    }
    Ok(value)
}

fn vector_norm_squared(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|value| {
            let value = f64::from(*value);
            value * value
        })
        .sum()
}

fn hnsw_data_vector(distance: VectorDistance, vector: &[f32], ip_radius_squared: f64) -> Vec<f32> {
    if distance == VectorDistance::Ip {
        let mut embedded = Vec::with_capacity(vector.len() + 1);
        embedded.extend_from_slice(vector);
        embedded.push(
            (ip_radius_squared - vector_norm_squared(vector))
                .max(0.0)
                .sqrt() as f32,
        );
        return embedded;
    }
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

fn hnsw_query_vector(distance: VectorDistance, vector: &[f32]) -> Vec<f32> {
    if distance == VectorDistance::Ip {
        let mut embedded = Vec::with_capacity(vector.len() + 1);
        embedded.extend_from_slice(vector);
        embedded.push(0.0);
        return embedded;
    }
    hnsw_data_vector(distance, vector, 0.0)
}

impl HnswGraph {
    fn new(
        dim: usize,
        distance: VectorDistance,
        m: usize,
        ef_construction: usize,
        initial_cap: usize,
        quantization: VectorQuantization,
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
            live_count: 0,
            backend: HnswBackend::new(distance, quantization, m, initial_cap, ef_construction),
            quantization,
            max_doc_version: 0,
            ip_radius_squared: 0.0,
        }
    }

    fn upsert(&mut self, id: String, doc_version: u64, vector: Vec<f32>) -> Result<(), Error> {
        validate_vector(&vector, self.dim)?;
        validate_vector_for_distance(&vector, self.distance)?;
        let replaces_live = if let Some(pos) = self.id_to_pos.get(&id).copied() {
            let was_live = !self.nodes[pos].deleted;
            self.nodes[pos].deleted = true;
            was_live
        } else {
            false
        };
        let radius_grew = if self.distance == VectorDistance::Ip {
            let norm_squared = vector_norm_squared(&vector);
            if norm_squared > self.ip_radius_squared {
                // Reserve growth headroom so monotonically increasing norms
                // do not rebuild the active graph on every insert.
                self.ip_radius_squared = (norm_squared * 4.0).max(norm_squared);
                true
            } else {
                false
            }
        } else {
            false
        };
        let pos = self.nodes.len();
        self.nodes.push(HnswNode {
            id: id.clone(),
            doc_version,
            vector: vector.clone(),
            deleted: false,
        });
        self.max_doc_version = self.max_doc_version.max(doc_version);
        self.id_to_pos.insert(id, pos);
        if !replaces_live {
            self.live_count = self.live_count.saturating_add(1);
        }
        if replaces_live || radius_grew {
            // hnsw_rs has no physical replacement operation. Rebuilding the
            // bounded active graph prevents a fresh version from connecting
            // only to its tombstoned predecessor, which otherwise makes
            // VLINKS and small-graph searches nondeterministic.
            self.rebuild_backend();
        } else {
            self.backend.insert(
                &hnsw_data_vector(self.distance, &vector, self.ip_radius_squared),
                pos,
            );
        }
        Ok(())
    }

    fn rebuild_backend(&mut self) {
        self.backend = HnswBackend::new(
            self.distance,
            self.quantization,
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
            self.backend.insert(
                &hnsw_data_vector(self.distance, &node.vector, self.ip_radius_squared),
                pos,
            );
        }
    }

    fn mark_deleted(&mut self, id: &str) {
        if let Some(pos) = self.id_to_pos.get(id).copied()
            && !self.nodes[pos].deleted
        {
            self.nodes[pos].deleted = true;
            self.live_count = self.live_count.saturating_sub(1);
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
        let backend_query = hnsw_query_vector(self.distance, query);
        let neighbours = self.backend.search(
            &backend_query,
            limit,
            ef.max(limit),
            // Always filter tombstoned nodes in the HNSW traversal.  Filtering
            // only when an external allow-list was present let deleted
            // versions consume the backend's `limit`, and the post-filter
            // below could then underfill an otherwise complete result set.
            Some(&filter as &dyn hnsw_rs::filter::FilterT),
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
                    // Recompute against the original vector.  Besides giving
                    // the public IP score, this makes candidates from segments
                    // with different MIPS radii directly comparable.
                    distance: distance_score(self.distance, query, &node.vector).ok()?,
                })
            })
            .take(limit)
            .collect())
    }

    fn len(&self) -> usize {
        self.live_count
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
                        (!node.deleted && node.id != id).then(|| {
                            (
                                node.id.clone(),
                                distance_score(
                                    self.distance,
                                    &self.nodes[origin_id].vector,
                                    &node.vector,
                                )
                                .unwrap_or(neighbor.distance),
                            )
                        })
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
                    layers[layer_index].push((
                        candidate.id.clone(),
                        distance_score(
                            self.distance,
                            &self.nodes[origin_id].vector,
                            &candidate.vector,
                        )
                        .unwrap_or(edge.distance),
                    ));
                }
            }
        }
        while layers.last().is_some_and(Vec::is_empty) {
            layers.pop();
        }
        Some(layers)
    }

    fn max_doc_version(&self) -> u64 {
        self.max_doc_version
    }

    fn to_persisted_index(&self) -> Result<VectorHnswIndexBlob, Error> {
        let live_positions = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(position, node)| (!node.deleted).then_some(position))
            .collect::<Vec<_>>();
        if live_positions.is_empty() {
            return Err(Error::msg("ERR cannot build an empty HNSW index"));
        }
        let mut remap = vec![None; self.nodes.len()];
        for (persisted, position) in live_positions.iter().copied().enumerate() {
            remap[position] = Some(persisted as u32);
        }

        let mut nodes = Vec::with_capacity(live_positions.len());
        for position in live_positions.iter().copied() {
            let node = &self.nodes[position];
            let mut layers = self
                .backend
                .neighborhood(position)
                .into_iter()
                .map(|neighbors| {
                    neighbors
                        .into_iter()
                        .filter_map(|neighbor| remap.get(neighbor.d_id).copied().flatten())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            if layers.is_empty() {
                layers.push(Vec::new());
            }
            nodes.push(VectorHnswIndexNode {
                id: node.id.clone(),
                doc_version: node.doc_version,
                vector: hnsw_index_payload(
                    self.distance,
                    &node.vector,
                    self.ip_radius_squared,
                    self.quantization,
                ),
                layers,
            });
        }

        let entry_point = self
            .backend
            .entry_point_origin_id()
            .and_then(|position| remap.get(position).copied().flatten())
            .unwrap_or_else(|| {
                nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, node)| node.layers.len())
                    .map(|(position, _)| position as u32)
                    .unwrap_or(0)
            });
        let max_layer = nodes
            .get(entry_point as usize)
            .map(|node| node.layers.len().saturating_sub(1) as u32)
            .unwrap_or(0);
        Ok(VectorHnswIndexBlob {
            dim: self.dim as u32,
            distance: self.distance,
            m: self.m as u32,
            ef_construction: self.ef_construction as u32,
            quantization: self.quantization,
            entry_point,
            max_layer,
            nodes,
        })
    }

}

impl VectorHnswIndexBlob {
    fn validate(&self) -> Result<(), Error> {
        let payload_dim = self.dim as usize + usize::from(self.distance == VectorDistance::Ip);
        if self.dim == 0
            || self.dim as usize > MAX_VECTOR_DIMENSIONS
            || self.m == 0
            || self.m > 256
            || self.ef_construction < self.m
            || self.ef_construction as usize > MAX_VECTOR_HNSW_EF
            || self.nodes.is_empty()
            || self.nodes.len() > MAX_VECTOR_INITIAL_CAP
            || self.entry_point as usize >= self.nodes.len()
            || self.max_layer as usize >= DEFAULT_HNSW_MAX_LAYER
            || self.max_layer as usize
                >= self.nodes[self.entry_point as usize].layers.len()
        {
            return Err(Error::msg("ERR invalid persisted HNSW index"));
        }
        let mut ids = HashSet::with_capacity(self.nodes.len());
        let mut observed_max_layer = 0usize;
        for (node_id, node) in self.nodes.iter().enumerate() {
            if node.layers.is_empty()
                || node.layers.len() > DEFAULT_HNSW_MAX_LAYER
                || node.id.is_empty()
                || node.doc_version == 0
                || !ids.insert(node.id.as_str())
            {
                return Err(Error::msg("ERR invalid persisted HNSW topology"));
            }
            observed_max_layer = observed_max_layer.max(node.layers.len() - 1);
            for layer in &node.layers {
                let mut neighbors = HashSet::with_capacity(layer.len());
                if layer.iter().any(|neighbor| {
                    *neighbor as usize >= self.nodes.len()
                        || *neighbor as usize == node_id
                        || !neighbors.insert(*neighbor)
                }) {
                    return Err(Error::msg("ERR invalid persisted HNSW topology"));
                }
            }
            let valid_payload = match (&node.vector, self.quantization) {
                (HnswSnapshotVector::F32(values), VectorQuantization::F32) => {
                    values.len() == payload_dim && values.iter().all(|value| value.is_finite())
                }
                (
                    HnswSnapshotVector::Q8 { scale, values },
                    VectorQuantization::Q8,
                ) => scale.is_finite() && *scale > 0.0 && values.len() == payload_dim,
                (
                    HnswSnapshotVector::Binary { dimensions, bits },
                    VectorQuantization::Binary,
                ) => {
                    *dimensions as usize == payload_dim && bits.len() == payload_dim.div_ceil(8)
                }
                _ => false,
            };
            if !valid_payload {
                return Err(Error::msg("ERR invalid persisted HNSW vector payload"));
            }
        }
        if observed_max_layer != self.max_layer as usize {
            return Err(Error::msg("ERR invalid persisted HNSW max layer"));
        }
        Ok(())
    }

    fn node_distance(
        &self,
        node: u32,
        query: &HnswSnapshotVector,
    ) -> Result<f32, Error> {
        let node = self
            .nodes
            .get(node as usize)
            .ok_or_else(|| Error::msg("ERR invalid persisted HNSW node"))?;
        hnsw_payload_distance(self.distance, query, &node.vector)
    }

    fn links(
        &self,
        id: &str,
        current_versions: &HashMap<String, u64>,
    ) -> Option<Vec<Vec<(String, f32)>>> {
        let origin = self.nodes.iter().position(|node| {
            node.id == id
                && current_versions.get(id).copied() == Some(node.doc_version)
        })?;
        let origin_vector = &self.nodes[origin].vector;
        let mut layers = self.nodes[origin]
            .layers
            .iter()
            .map(|neighbors| {
                neighbors
                    .iter()
                    .filter_map(|neighbor| {
                        let neighbor = self.nodes.get(*neighbor as usize)?;
                        if current_versions.get(&neighbor.id).copied()
                            != Some(neighbor.doc_version)
                        {
                            return None;
                        }
                        Some((
                            neighbor.id.clone(),
                            hnsw_payload_distance(self.distance, origin_vector, &neighbor.vector)
                                .ok()?,
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        while layers.last().is_some_and(Vec::is_empty) {
            layers.pop();
        }
        Some(layers)
    }

    fn search(
        &self,
        query: &[f32],
        limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
        current_versions: &HashMap<String, u64>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        self.validate()?;
        validate_vector(query, self.dim as usize)?;
        validate_vector_for_distance(query, self.distance)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let query_payload = hnsw_query_payload(self.distance, query, self.quantization);
        let mut current = self.entry_point;
        let mut current_distance = self.node_distance(current, &query_payload)?;

        for layer in (1..=self.max_layer as usize).rev() {
            loop {
                let mut improved = false;
                let neighbors = self.nodes[current as usize]
                    .layers
                    .get(layer)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for &neighbor in neighbors {
                    let distance = self.node_distance(neighbor, &query_payload)?;
                    if distance < current_distance {
                        current = neighbor;
                        current_distance = distance;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }

        let ef = ef.max(limit).min(self.nodes.len()).max(1);
        let start = HnswSearchQueueItem {
            node: current,
            distance: current_distance,
        };
        let mut candidates = BinaryHeap::new();
        candidates.push(std::cmp::Reverse(start));
        let mut nearest = BinaryHeap::new();
        nearest.push(start);
        let mut visited = HashSet::with_capacity(ef.saturating_mul(2).min(self.nodes.len()));
        visited.insert(current);

        while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
            if nearest.len() >= ef
                && nearest
                    .peek()
                    .is_some_and(|worst| candidate.distance > worst.distance)
            {
                break;
            }
            let neighbors = self.nodes[candidate.node as usize]
                .layers
                .first()
                .map(Vec::as_slice)
                .unwrap_or_default();
            for &neighbor in neighbors {
                if !visited.insert(neighbor) {
                    continue;
                }
                let distance = self.node_distance(neighbor, &query_payload)?;
                let item = HnswSearchQueueItem {
                    node: neighbor,
                    distance,
                };
                if nearest.len() < ef
                    || nearest
                        .peek()
                        .is_some_and(|worst| item.distance < worst.distance)
                {
                    candidates.push(std::cmp::Reverse(item));
                    nearest.push(item);
                    if nearest.len() > ef {
                        nearest.pop();
                    }
                }
            }
        }

        let mut nearest = nearest.into_vec();
        nearest.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.node.cmp(&right.node))
        });
        let mut output = Vec::with_capacity(limit.min(nearest.len()));
        for item in nearest {
            let node = &self.nodes[item.node as usize];
            if allow_doc_ids.is_some_and(|allowed| !allowed.contains(&node.id)) {
                continue;
            }
            if current_versions.get(&node.id).copied() != Some(node.doc_version) {
                continue;
            }
            output.push(VectorCandidate {
                id: node.id.clone(),
                doc_version: node.doc_version,
                distance: item.distance,
            });
            if output.len() >= limit {
                break;
            }
        }
        Ok(output)
    }
}

impl HnswBackend {
    fn new(
        distance: VectorDistance,
        quantization: VectorQuantization,
        m: usize,
        initial_cap: usize,
        ef_construction: usize,
    ) -> Self {
        match (quantization, distance) {
            (VectorQuantization::F32, VectorDistance::L2) => {
                HnswBackend::F32L2(Hnsw::<f32, DistL2>::new(
                    m,
                    initial_cap,
                    DEFAULT_HNSW_MAX_LAYER,
                    ef_construction,
                    DistL2 {},
                ))
            }
            (VectorQuantization::F32, VectorDistance::Cosine) => {
                HnswBackend::F32Cosine(Hnsw::<f32, DistCosine>::new(
                    m,
                    initial_cap,
                    DEFAULT_HNSW_MAX_LAYER,
                    ef_construction,
                    DistCosine {},
                ))
            }
            (VectorQuantization::F32, VectorDistance::Ip) => {
                HnswBackend::F32Ip(Hnsw::<f32, DistL2>::new(
                    m,
                    initial_cap,
                    DEFAULT_HNSW_MAX_LAYER,
                    ef_construction,
                    DistL2 {},
                ))
            }
            (VectorQuantization::Q8, _) => HnswBackend::Q8(Hnsw::<u8, DistQ8>::new(
                m,
                initial_cap,
                DEFAULT_HNSW_MAX_LAYER,
                ef_construction,
                DistQ8 {
                    cosine: distance == VectorDistance::Cosine,
                },
            )),
            (VectorQuantization::Binary, _) => {
                HnswBackend::Binary(Hnsw::<u8, DistPackedBinary>::new(
                    m,
                    initial_cap,
                    DEFAULT_HNSW_MAX_LAYER,
                    ef_construction,
                    DistPackedBinary,
                ))
            }
        }
    }

    fn insert(&self, vector: &[f32], origin_id: usize) {
        match self {
            HnswBackend::F32L2(index) => index.insert((vector, origin_id)),
            HnswBackend::F32Cosine(index) => index.insert((vector, origin_id)),
            HnswBackend::F32Ip(index) => index.insert((vector, origin_id)),
            HnswBackend::Q8(index) => index.insert((&encode_q8_vector(vector), origin_id)),
            HnswBackend::Binary(index) => {
                index.insert((&encode_binary_vector(vector), origin_id));
            }
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
            HnswBackend::F32L2(index) => index.search_filter(query, limit, ef, filter),
            HnswBackend::F32Cosine(index) => index.search_filter(query, limit, ef, filter),
            HnswBackend::F32Ip(index) => index.search_filter(query, limit, ef, filter),
            HnswBackend::Q8(index) => {
                index.search_filter(&encode_q8_vector(query), limit, ef, filter)
            }
            HnswBackend::Binary(index) => {
                index.search_filter(&encode_binary_vector(query), limit, ef, filter)
            }
        }
    }

    fn neighborhood(&self, origin_id: usize) -> Vec<Vec<hnsw_rs::prelude::Neighbour>> {
        macro_rules! neighborhood {
            ($index:expr) => {
                $index
                    .get_point_indexation()
                    .into_iter()
                    .find(|point| point.get_origin_id() == origin_id)
                    .map(|point| {
                        let mut layers = point.get_neighborhood_id();
                        // Empty upper layers are still part of the node's
                        // persisted HNSW level and must survive reload.
                        layers.resize_with(point.get_point_id().0 as usize + 1, Vec::new);
                        layers
                    })
                    .unwrap_or_default()
            };
        }
        match self {
            HnswBackend::F32L2(index) => neighborhood!(index),
            HnswBackend::F32Cosine(index) => neighborhood!(index),
            HnswBackend::F32Ip(index) => neighborhood!(index),
            HnswBackend::Q8(index) => neighborhood!(index),
            HnswBackend::Binary(index) => neighborhood!(index),
        }
    }

    fn entry_point_origin_id(&self) -> Option<usize> {
        macro_rules! entry_point {
            ($index:expr) => {
                $index
                    .get_point_indexation()
                    .into_iter()
                    .max_by_key(|point| point.get_point_id().0)
                    .map(|point| point.get_origin_id())
            };
        }
        match self {
            HnswBackend::F32L2(index) => entry_point!(index),
            HnswBackend::F32Cosine(index) => entry_point!(index),
            HnswBackend::F32Ip(index) => entry_point!(index),
            HnswBackend::Q8(index) => entry_point!(index),
            HnswBackend::Binary(index) => entry_point!(index),
        }
    }
}
