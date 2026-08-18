#[cfg(test)]
#[derive(Clone)]
struct HnswNode {
    id: String,
    doc_version: u64,
    vector: Vec<f32>,
    deleted: bool,
}

#[cfg(test)]
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
        let lhs_values = &lhs[4..];
        let rhs_values = &rhs[4..];
        q8_distance(
            lhs_scale,
            lhs_values,
            q8_norm_squared(lhs_values),
            rhs_scale,
            rhs_values,
            q8_norm_squared(rhs_values),
            self.cosine,
        )
    }
}

fn q8_norm_squared(values: &[u8]) -> u32 {
    q8_dot(values, values).max(0) as u32
}

fn q8_dot(lhs: &[u8], rhs: &[u8]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: feature detection above guarantees AVX2 support and the
        // helper performs only unaligned loads inside the supplied slices.
        return unsafe { q8_dot_avx2(lhs, rhs) };
    }
    q8_dot_scalar(lhs, rhs)
}

fn q8_dot_scalar(lhs: &[u8], rhs: &[u8]) -> i64 {
    lhs.iter().zip(rhs).fold(0i64, |dot, (lhs, rhs)| {
        dot + i64::from(*lhs as i8) * i64::from(*rhs as i8)
    })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn q8_dot_avx2(lhs: &[u8], rhs: &[u8]) -> i64 {
    use std::arch::x86_64::*;

    let common = lhs.len().min(rhs.len());
    let vectorized = common / 32 * 32;
    let mut accumulator = _mm256_setzero_si256();
    let mut offset = 0usize;
    while offset < vectorized {
        // SAFETY: offset is bounded by vectorized <= common and unaligned
        // loads accept byte-aligned pointers.
        let left = unsafe { _mm256_loadu_si256(lhs.as_ptr().add(offset).cast()) };
        let right = unsafe { _mm256_loadu_si256(rhs.as_ptr().add(offset).cast()) };
        let left_low = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(left));
        let right_low = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(right));
        let left_high = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(left, 1));
        let right_high = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(right, 1));
        accumulator = _mm256_add_epi32(
            accumulator,
            _mm256_madd_epi16(left_low, right_low),
        );
        accumulator = _mm256_add_epi32(
            accumulator,
            _mm256_madd_epi16(left_high, right_high),
        );
        offset += 32;
    }
    let mut lanes = [0i32; 8];
    // SAFETY: lanes has exactly the 32 bytes required by the store.
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast(), accumulator) };
    let mut dot = lanes.into_iter().map(i64::from).sum::<i64>();
    dot += q8_dot_scalar(&lhs[vectorized..common], &rhs[vectorized..common]);
    dot
}

fn q8_distance(
    lhs_scale: f32,
    lhs: &[u8],
    lhs_norm_squared: u32,
    rhs_scale: f32,
    rhs: &[u8],
    rhs_norm_squared: u32,
    cosine: bool,
) -> f32 {
    if lhs.len() != rhs.len() || lhs.is_empty() {
        return f32::MAX;
    }
    let dot = q8_dot(lhs, rhs) as f64;
    if cosine {
        if lhs_norm_squared == 0 || rhs_norm_squared == 0 {
            return 1.0;
        }
        // Both scales are positive and cancel from cosine similarity. Keep the
        // accumulation integral until the final division.
        let denominator =
            (f64::from(lhs_norm_squared) * f64::from(rhs_norm_squared)).sqrt();
        return (1.0 - (dot / denominator).clamp(-1.0, 1.0)) as f32;
    }
    // Sum((a*sa-b*sb)^2) can be recovered from the two cached norms and the
    // integer dot product, avoiding a floating-point loop over every edge.
    let lhs_scale = f64::from(lhs_scale);
    let rhs_scale = f64::from(rhs_scale);
    let squared = lhs_scale * lhs_scale * f64::from(lhs_norm_squared)
        + rhs_scale * rhs_scale * f64::from(rhs_norm_squared)
        - 2.0 * lhs_scale * rhs_scale * dot;
    squared.max(0.0).sqrt() as f32
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
            if distance == VectorDistance::Cosine {
                // COSINE payloads are normalized before they enter the graph.
                // Avoid recomputing two norms and square roots for every edge.
                let dot = left
                    .iter()
                    .zip(right)
                    .map(|(left, right)| f64::from(*left) * f64::from(*right))
                    .sum::<f64>();
                (1.0 - dot.clamp(-1.0, 1.0)) as f32
            } else {
                // IP vectors have already been embedded into L2 space.
                distance_score(VectorDistance::L2, left, right)?
            }
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
        ) => q8_distance(
            *left_scale,
            left_values,
            q8_norm_squared(left_values),
            *right_scale,
            right_values,
            q8_norm_squared(right_values),
            distance == VectorDistance::Cosine,
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

fn hnsw_candidate_distance(
    distance: VectorDistance,
    query: &[f32],
    payload: &HnswSnapshotVector,
    graph_distance: f32,
) -> Result<f32, Error> {
    if distance != VectorDistance::Ip {
        return Ok(graph_distance);
    }
    let score = match payload {
        HnswSnapshotVector::F32(values) => -query
            .iter()
            .zip(values.iter().take(query.len()))
            .map(|(query, value)| f64::from(*query) * f64::from(*value))
            .sum::<f64>(),
        HnswSnapshotVector::Q8 { scale, values } => -query
            .iter()
            .zip(values.iter().take(query.len()))
            .map(|(query, value)| {
                f64::from(*query) * f64::from((*value as i8) as f32 * *scale)
            })
            .sum::<f64>(),
        // One-bit payloads intentionally retain only angular/sign information;
        // their graph distance is already comparable across segments.
        HnswSnapshotVector::Binary { .. } => return Ok(graph_distance),
    };
    if !score.is_finite() || score < -f64::from(f32::MAX) || score > f64::from(f32::MAX) {
        return Err(Error::msg("ERR vector distance overflow"));
    }
    Ok(score as f32)
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

#[cfg(test)]
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

    #[cfg(test)]
    fn mark_deleted(&mut self, id: &str) {
        if let Some(pos) = self.id_to_pos.get(id).copied()
            && !self.nodes[pos].deleted
        {
            self.nodes[pos].deleted = true;
            self.live_count = self.live_count.saturating_sub(1);
        }
    }

    #[cfg(test)]
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
                    source_position: None,
                })
            })
            .take(limit)
            .collect())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.live_count
    }

    #[cfg(test)]
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
            nodes.push((
                node.id.clone(),
                node.doc_version,
                hnsw_index_payload(
                    self.distance,
                    &node.vector,
                    self.ip_radius_squared,
                    self.quantization,
                ),
                layers,
            ));
        }

        let entry_point = self
            .backend
            .entry_point_origin_id()
            .and_then(|position| remap.get(position).copied().flatten())
            .unwrap_or_else(|| {
                nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, node)| node.3.len())
                    .map(|(position, _)| position as u32)
                    .unwrap_or(0)
            });
        let max_layer = nodes[entry_point as usize].3.len().saturating_sub(1) as u32;
        Ok(VectorHnswIndexBlob::from_node_parts(
            self.dim as u32,
            self.distance,
            self.m as u32,
            self.ef_construction as u32,
            self.quantization,
            entry_point,
            max_layer,
            nodes,
        ))
    }

}

impl VectorHnswIndexBlob {
    fn build(source: &VectorSegmentBlob, meta: &VectorIndexMeta) -> Result<Self, Error> {
        if source.entries.is_empty() {
            return Err(Error::msg("ERR cannot index an empty vector segment"));
        }
        let ip_radius_squared = if meta.distance == VectorDistance::Ip {
            source
                .entries
                .iter()
                .map(|entry| vector_norm_squared(&entry.vector))
                .fold(0.0f64, f64::max)
                * 4.0
        } else {
            0.0
        };
        let payloads = source
            .entries
            .iter()
            .map(|entry| {
                hnsw_index_payload(
                    meta.distance,
                    &entry.vector,
                    ip_radius_squared,
                    meta.quantization,
                )
            })
            .collect::<Vec<_>>();
        let backend = HnswBackend::new(
            meta.distance,
            meta.quantization,
            meta.m as usize,
            source.entries.len(),
            meta.ef_construction as usize,
        );
        backend.insert_payloads(&payloads);

        let mut nodes = Vec::with_capacity(source.entries.len());
        for (position, (entry, payload)) in source.entries.iter().zip(payloads).enumerate() {
            let mut layers = backend
                .neighborhood(position)
                .into_iter()
                .map(|neighbors| {
                    neighbors
                        .into_iter()
                        .filter_map(|neighbor| {
                            (neighbor.d_id < source.entries.len()).then_some(neighbor.d_id as u32)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            if layers.is_empty() {
                layers.push(Vec::new());
            }
            nodes.push((entry.id.clone(), entry.doc_version, payload, layers));
        }
        let entry_point = backend
            .entry_point_origin_id()
            .filter(|position| *position < nodes.len())
            .unwrap_or_else(|| {
                nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, node)| node.3.len())
                    .map(|(position, _)| position)
                    .unwrap_or(0)
            }) as u32;
        let max_layer = nodes[entry_point as usize].3.len().saturating_sub(1) as u32;
        let index = Self::from_node_parts(
            meta.dim,
            meta.distance,
            meta.m,
            meta.ef_construction,
            meta.quantization,
            entry_point,
            max_layer,
            nodes,
        );
        index.validate()?;
        Ok(index)
    }

    fn from_legacy(legacy: LegacyVectorHnswIndexBlobV1) -> Self {
        Self::from_node_parts(
            legacy.dim,
            legacy.distance,
            legacy.m,
            legacy.ef_construction,
            legacy.quantization,
            legacy.entry_point,
            legacy.max_layer,
            legacy
                .nodes
                .into_iter()
                .map(|node| (node.id, node.doc_version, node.vector, node.layers))
                .collect(),
        )
    }

    fn from_legacy_v2(legacy: LegacyVectorHnswIndexBlobV2) -> Self {
        let q8_norms = legacy
            .vectors
            .iter()
            .map(|vector| match vector {
                HnswSnapshotVector::Q8 { values, .. } => q8_norm_squared(values),
                _ => 0,
            })
            .collect();
        Self {
            dim: legacy.dim,
            distance: legacy.distance,
            m: legacy.m,
            ef_construction: legacy.ef_construction,
            quantization: legacy.quantization,
            entry_point: legacy.entry_point,
            max_layer: legacy.max_layer,
            ids: legacy.ids,
            doc_versions: legacy.doc_versions,
            vectors: legacy.vectors,
            q8_norms,
            node_layer_offsets: legacy.node_layer_offsets,
            layer_neighbor_offsets: legacy.layer_neighbor_offsets,
            neighbors: legacy.neighbors,
        }
    }

    fn from_node_parts(
        dim: u32,
        distance: VectorDistance,
        m: u32,
        ef_construction: u32,
        quantization: VectorQuantization,
        entry_point: u32,
        max_layer: u32,
        nodes: Vec<(String, u64, HnswSnapshotVector, Vec<Vec<u32>>)>,
    ) -> Self {
        let mut ids = Vec::with_capacity(nodes.len());
        let mut doc_versions = Vec::with_capacity(nodes.len());
        let mut vectors = Vec::with_capacity(nodes.len());
        let mut node_layer_offsets = Vec::with_capacity(nodes.len() + 1);
        let mut layer_neighbor_offsets = Vec::new();
        let mut neighbors = Vec::new();
        node_layer_offsets.push(0);
        layer_neighbor_offsets.push(0);
        for (id, doc_version, vector, layers) in nodes {
            ids.push(id);
            doc_versions.push(doc_version);
            vectors.push(vector);
            for layer in layers {
                neighbors.extend(layer);
                layer_neighbor_offsets.push(neighbors.len() as u32);
            }
            node_layer_offsets.push((layer_neighbor_offsets.len() - 1) as u32);
        }
        let q8_norms = vectors
            .iter()
            .map(|vector| match vector {
                HnswSnapshotVector::Q8 { values, .. } => q8_norm_squared(values),
                _ => 0,
            })
            .collect();
        Self {
            dim,
            distance,
            m,
            ef_construction,
            quantization,
            entry_point,
            max_layer,
            ids,
            doc_versions,
            vectors,
            q8_norms,
            node_layer_offsets,
            layer_neighbor_offsets,
            neighbors,
        }
    }

    fn node_count(&self) -> usize {
        self.ids.len()
    }

    fn node_layer_count(&self, node: usize) -> usize {
        self.node_layer_offsets
            .get(node..=node.saturating_add(1))
            .filter(|offsets| offsets.len() == 2)
            .map_or(0, |offsets| offsets[1].saturating_sub(offsets[0]) as usize)
    }

    fn node_layer(&self, node: usize, layer: usize) -> &[u32] {
        let Some(first_layer) = self.node_layer_offsets.get(node).copied() else {
            return &[];
        };
        if layer >= self.node_layer_count(node) {
            return &[];
        }
        let layer = first_layer as usize + layer;
        let Some(offsets) = self.layer_neighbor_offsets.get(layer..=layer + 1) else {
            return &[];
        };
        self.neighbors
            .get(offsets[0] as usize..offsets[1] as usize)
            .unwrap_or_default()
    }

    fn validate(&self) -> Result<(), Error> {
        let payload_dim = self.dim as usize + usize::from(self.distance == VectorDistance::Ip);
        if self.dim == 0
            || self.dim as usize > MAX_VECTOR_DIMENSIONS
            || self.m == 0
            || self.m > 256
            || self.ef_construction < self.m
            || self.ef_construction as usize > MAX_VECTOR_HNSW_EF
            || self.ids.is_empty()
            || self.ids.len() > MAX_VECTOR_INITIAL_CAP
            || self.ids.len() != self.doc_versions.len()
            || self.ids.len() != self.vectors.len()
            || self.ids.len() != self.q8_norms.len()
            || self.node_layer_offsets.len() != self.ids.len().saturating_add(1)
            || self.node_layer_offsets.first().copied() != Some(0)
            || self.layer_neighbor_offsets.first().copied() != Some(0)
            || self.node_layer_offsets.last().copied().map(|offset| offset as usize)
                != Some(self.layer_neighbor_offsets.len().saturating_sub(1))
            || self.layer_neighbor_offsets.last().copied().map(|offset| offset as usize)
                != Some(self.neighbors.len())
            || !self.node_layer_offsets.windows(2).all(|offsets| offsets[0] <= offsets[1])
            || !self
                .layer_neighbor_offsets
                .windows(2)
                .all(|offsets| offsets[0] <= offsets[1])
            || self.entry_point as usize >= self.ids.len()
            || self.max_layer as usize >= DEFAULT_HNSW_MAX_LAYER
            || self.max_layer as usize >= self.node_layer_count(self.entry_point as usize)
        {
            return Err(Error::msg("ERR invalid persisted HNSW index"));
        }
        let mut ids = HashSet::with_capacity(self.ids.len());
        let mut observed_max_layer = 0usize;
        for node_id in 0..self.ids.len() {
            let layer_count = self.node_layer_count(node_id);
            if layer_count == 0
                || layer_count > DEFAULT_HNSW_MAX_LAYER
                || self.ids[node_id].is_empty()
                || self.doc_versions[node_id] == 0
                || !ids.insert(self.ids[node_id].as_str())
            {
                return Err(Error::msg("ERR invalid persisted HNSW topology"));
            }
            observed_max_layer = observed_max_layer.max(layer_count - 1);
            for layer_index in 0..layer_count {
                let layer = self.node_layer(node_id, layer_index);
                let mut neighbors = HashSet::with_capacity(layer.len());
                if layer.iter().any(|neighbor| {
                    *neighbor as usize >= self.ids.len()
                        || *neighbor as usize == node_id
                        || !neighbors.insert(*neighbor)
                }) {
                    return Err(Error::msg("ERR invalid persisted HNSW topology"));
                }
            }
            let valid_payload = match (&self.vectors[node_id], self.quantization) {
                (HnswSnapshotVector::F32(values), VectorQuantization::F32) => {
                    values.len() == payload_dim && values.iter().all(|value| value.is_finite())
                }
                (
                    HnswSnapshotVector::Q8 { scale, values },
                    VectorQuantization::Q8,
                ) => {
                    scale.is_finite()
                        && *scale > 0.0
                        && values.len() == payload_dim
                        && self.q8_norms[node_id] == q8_norm_squared(values)
                }
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
        query_q8_norm: u32,
    ) -> Result<f32, Error> {
        let vector = self
            .vectors
            .get(node as usize)
            .ok_or_else(|| Error::msg("ERR invalid persisted HNSW node"))?;
        match (query, vector) {
            (
                HnswSnapshotVector::Q8 {
                    scale: query_scale,
                    values: query_values,
                },
                HnswSnapshotVector::Q8 {
                    scale: vector_scale,
                    values: vector_values,
                },
            ) => Ok(q8_distance(
                *query_scale,
                query_values,
                query_q8_norm,
                *vector_scale,
                vector_values,
                self.q8_norms[node as usize],
                self.distance == VectorDistance::Cosine,
            )),
            _ => hnsw_payload_distance(self.distance, query, vector),
        }
    }

    fn links(
        &self,
        id: &str,
        current_versions: &DashMap<String, u64>,
    ) -> Option<Vec<Vec<(String, f32)>>> {
        let origin = self.ids.iter().enumerate().position(|(node, node_id)| {
            node_id == id
                && current_versions
                    .get(id)
                    .is_some_and(|version| *version == self.doc_versions[node])
        })?;
        let origin_vector = &self.vectors[origin];
        let mut layers = (0..self.node_layer_count(origin))
            .map(|layer| {
                let neighbors = self.node_layer(origin, layer);
                neighbors
                    .iter()
                    .filter_map(|neighbor| {
                        let neighbor = *neighbor as usize;
                        let neighbor_id = self.ids.get(neighbor)?;
                        let doc_version = *self.doc_versions.get(neighbor)?;
                        if current_versions
                            .get(neighbor_id)
                            .is_none_or(|version| *version != doc_version)
                        {
                            return None;
                        }
                        Some((
                            neighbor_id.clone(),
                            hnsw_payload_distance(
                                self.distance,
                                origin_vector,
                                self.vectors.get(neighbor)?,
                            )
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

    #[cfg(test)]
    fn search(
        &self,
        query: &[f32],
        limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
        current_versions: &DashMap<String, u64>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        validate_vector(query, self.dim as usize)?;
        validate_vector_for_distance(query, self.distance)?;
        let query_payload = hnsw_query_payload(self.distance, query, self.quantization);
        self.search_prepared(
            query,
            &query_payload,
            limit,
            ef,
            allow_doc_ids,
            current_versions,
        )
    }

    fn search_prepared(
        &self,
        query: &[f32],
        query_payload: &HnswSnapshotVector,
        limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
        current_versions: &DashMap<String, u64>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let query_q8_norm = match query_payload {
            HnswSnapshotVector::Q8 { values, .. } => q8_norm_squared(values),
            _ => 0,
        };
        let mut current = self.entry_point;
        let mut current_distance =
            self.node_distance(current, query_payload, query_q8_norm)?;
        let mut distance_calculations = 1usize;

        for layer in (1..=self.max_layer as usize).rev() {
            loop {
                let mut improved = false;
                let neighbors = self.node_layer(current as usize, layer);
                for &neighbor in neighbors {
                    distance_calculations = distance_calculations.saturating_add(1);
                    let distance =
                        self.node_distance(neighbor, query_payload, query_q8_norm)?;
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

        let ef = ef.max(limit).min(self.node_count()).max(1);
        let start = HnswSearchQueueItem {
            node: current,
            distance: current_distance,
        };
        let mut nearest = VECTOR_HNSW_VISITED.with(|scratch| -> Result<Vec<_>, Error> {
            let mut scratch = scratch.borrow_mut();
            let generation = scratch.begin(self.node_count());
            scratch.marks[current as usize] = generation;
            let mut candidates = BinaryHeap::new();
            candidates.push(std::cmp::Reverse(start));
            let mut nearest = BinaryHeap::new();
            nearest.push(start);

            while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
                if nearest.len() >= ef
                    && nearest
                        .peek()
                        .is_some_and(|worst| candidate.distance > worst.distance)
                {
                    break;
                }
                let neighbors = self.node_layer(candidate.node as usize, 0);
                for &neighbor in neighbors {
                    if scratch.marks[neighbor as usize] == generation {
                        continue;
                    }
                    scratch.marks[neighbor as usize] = generation;
                    distance_calculations = distance_calculations.saturating_add(1);
                    let distance = self.node_distance(neighbor, query_payload, query_q8_norm)?;
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
            Ok(nearest.into_vec())
        })?;
        nearest.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.node.cmp(&right.node))
        });
        let mut output = Vec::with_capacity(limit.min(nearest.len()));
        for item in nearest {
            let node = item.node as usize;
            let id = &self.ids[node];
            let doc_version = self.doc_versions[node];
            if allow_doc_ids.is_some_and(|allowed| !allowed.contains(id)) {
                continue;
            }
            if current_versions
                .get(id)
                .is_none_or(|version| *version != doc_version)
            {
                continue;
            }
            output.push(VectorCandidate {
                id: id.clone(),
                doc_version,
                distance: hnsw_candidate_distance(
                    self.distance,
                    query,
                    &self.vectors[node],
                    item.distance,
                )?,
                source_position: Some(node),
            });
            if output.len() >= limit {
                break;
            }
        }
        global_metrics().record_vector_distance_calculations(distance_calculations);
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

    #[cfg(test)]
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

    fn insert_payloads(&self, payloads: &[HnswSnapshotVector]) {
        const PARALLEL_BUILD_THRESHOLD: usize = 256;
        macro_rules! insert_f32 {
            ($index:expr) => {{
                let vectors = payloads
                    .iter()
                    .filter_map(|payload| match payload {
                        HnswSnapshotVector::F32(values) => Some(values.as_slice()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if vectors.len() >= PARALLEL_BUILD_THRESHOLD {
                    let refs = vectors
                        .iter()
                        .enumerate()
                        .map(|(origin_id, vector)| (*vector, origin_id))
                        .collect::<Vec<_>>();
                    $index.parallel_insert_slice(&refs);
                } else {
                    for (origin_id, vector) in vectors.into_iter().enumerate() {
                        $index.insert((vector, origin_id));
                    }
                }
            }};
        }
        match self {
            HnswBackend::F32L2(index) => insert_f32!(index),
            HnswBackend::F32Cosine(index) => insert_f32!(index),
            HnswBackend::F32Ip(index) => insert_f32!(index),
            HnswBackend::Q8(index) => {
                let encoded = payloads
                    .iter()
                    .filter_map(|payload| match payload {
                        HnswSnapshotVector::Q8 { scale, values } => {
                            Some(q8_snapshot_bytes(*scale, values))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if encoded.len() >= PARALLEL_BUILD_THRESHOLD {
                    let refs = encoded
                        .iter()
                        .enumerate()
                        .map(|(origin_id, vector)| (vector.as_slice(), origin_id))
                        .collect::<Vec<_>>();
                    index.parallel_insert_slice(&refs);
                } else {
                    for (origin_id, vector) in encoded.iter().enumerate() {
                        index.insert((vector.as_slice(), origin_id));
                    }
                }
            }
            HnswBackend::Binary(index) => {
                let vectors = payloads
                    .iter()
                    .filter_map(|payload| match payload {
                        HnswSnapshotVector::Binary { bits, .. } => Some(bits.as_slice()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if vectors.len() >= PARALLEL_BUILD_THRESHOLD {
                    let refs = vectors
                        .iter()
                        .enumerate()
                        .map(|(origin_id, vector)| (*vector, origin_id))
                        .collect::<Vec<_>>();
                    index.parallel_insert_slice(&refs);
                } else {
                    for (origin_id, vector) in vectors.into_iter().enumerate() {
                        index.insert((vector, origin_id));
                    }
                }
            }
        }
    }

    #[cfg(test)]
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
