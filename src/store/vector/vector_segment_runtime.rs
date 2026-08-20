#[derive(Clone)]
struct VectorSegmentRuntime {
    meta: VectorSegmentMeta,
    source: Option<Arc<VectorSegmentBlob>>,
    index: Option<Arc<VectorHnswIndexBlob>>,
}

struct VectorRuntime {
    /// Mutable LSM component.  Values are already durable as per-document KV
    /// records; this table only batches them into immutable source blobs.
    memtable: HashMap<String, Arc<VectorDocRecord>>,
    segments: Vec<VectorSegmentRuntime>,
    next_segment_id: u64,
    /// Lightweight latest-version directory.  Full vectors and attributes
    /// live only in the bounded memtable, immutable segment blobs, or the
    /// per-document KV records; keeping them here would defeat the LSM memory
    /// bound.
    current_versions: Arc<DashMap<String, u64>>,
    memtable_generation: u64,
    memtable_snapshot: std::sync::Mutex<(u64, Arc<Vec<Arc<VectorDocRecord>>>)>,
    /// Ephemeral HNSW over the current mutable tail. It is rebuilt by
    /// maintenance and never participates in the durable write transaction.
    /// Documents newer than `delta_index_through` remain an exact-scan tail.
    delta_index: Option<Arc<VectorHnswIndexBlob>>,
    delta_index_through: u64,
    config: VectorRuntimeConfig,
}

struct VectorRuntimeSearchSnapshot {
    segments: Vec<VectorSegmentRuntime>,
    memtable: Arc<Vec<Arc<VectorDocRecord>>>,
    current_versions: Arc<DashMap<String, u64>>,
    delta_index: Option<Arc<VectorHnswIndexBlob>>,
    delta_index_through: u64,
    config: VectorRuntimeConfig,
}

trait VectorSearchEntry {
    fn id(&self) -> &str;
    fn doc_version(&self) -> u64;
    fn vector(&self) -> &[f32];
    fn deleted(&self) -> bool {
        false
    }
}

impl VectorSearchEntry for VectorDocRecord {
    fn id(&self) -> &str {
        &self.id
    }

    fn doc_version(&self) -> u64 {
        self.doc_version
    }

    fn vector(&self) -> &[f32] {
        &self.vector
    }

    fn deleted(&self) -> bool {
        self.deleted
    }
}

impl VectorSearchEntry for VectorSegmentEntry {
    fn id(&self) -> &str {
        &self.id
    }

    fn doc_version(&self) -> u64 {
        self.doc_version
    }

    fn vector(&self) -> &[f32] {
        &self.vector
    }
}

impl VectorRuntime {
    fn new(
        config: VectorRuntimeConfig,
        next_segment_id: u64,
    ) -> Self {
        Self {
            memtable: HashMap::new(),
            segments: Vec::new(),
            next_segment_id,
            current_versions: Arc::new(DashMap::new()),
            memtable_generation: 0,
            memtable_snapshot: std::sync::Mutex::new((0, Arc::new(Vec::new()))),
            delta_index: None,
            delta_index_through: 0,
            config,
        }
    }

    fn with_segments(
        config: VectorRuntimeConfig,
        next_segment_id: u64,
        segments: Vec<VectorSegmentRuntime>,
    ) -> Self {
        Self {
            memtable: HashMap::new(),
            segments,
            next_segment_id,
            current_versions: Arc::new(DashMap::new()),
            memtable_generation: 0,
            memtable_snapshot: std::sync::Mutex::new((0, Arc::new(Vec::new()))),
            delta_index: None,
            delta_index_through: 0,
            config,
        }
    }

    #[cfg(test)]
    fn upsert(&mut self, id: String, doc_version: u64, vector: Vec<f32>) -> Result<(), Error> {
        self.upsert_with_attrs(id, doc_version, vector, "{}".to_string())
    }

    fn upsert_with_attrs(
        &mut self,
        id: String,
        doc_version: u64,
        vector: Vec<f32>,
        attrs_json: String,
    ) -> Result<(), Error> {
        validate_vector(&vector, self.config.dim)?;
        validate_vector_for_distance(&vector, self.config.distance)?;
        self.apply_doc(VectorDocRecord {
            id,
            doc_version,
            vector,
            attrs_json,
            deleted: false,
        });
        Ok(())
    }

    fn apply_doc(&mut self, doc: VectorDocRecord) {
        if doc.deleted {
            self.current_versions.remove(&doc.id);
        } else {
            self.current_versions
                .insert(doc.id.clone(), doc.doc_version);
        }
        self.memtable.insert(doc.id.clone(), Arc::new(doc));
        self.mark_memtable_changed();
    }

    fn mark_deleted(&mut self, doc: VectorDocRecord) {
        debug_assert!(doc.deleted);
        self.apply_doc(doc);
    }

    fn reconcile_docs(&mut self, docs: Vec<VectorDocRecord>, flushed_through: u64) {
        self.current_versions.clear();
        self.memtable.clear();
        for doc in docs {
            if doc.doc_version > flushed_through {
                self.memtable
                    .insert(doc.id.clone(), Arc::new(doc.clone()));
            }
            if !doc.deleted {
                self.current_versions.insert(doc.id, doc.doc_version);
            }
        }
        self.mark_memtable_changed();
    }

    fn restore_version_state(
        &mut self,
        current_versions: HashMap<String, u64>,
        tail_docs: Vec<VectorDocRecord>,
    ) {
        self.current_versions.clear();
        for (id, version) in current_versions {
            self.current_versions.insert(id, version);
        }
        self.memtable = tail_docs
            .into_iter()
            .map(|doc| (doc.id.clone(), Arc::new(doc)))
            .collect();
        self.mark_memtable_changed();
        self.delta_index = None;
        self.delta_index_through = 0;
    }

    fn len(&self) -> usize {
        self.current_versions.len()
    }

    fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    fn mark_memtable_changed(&mut self) {
        self.memtable_generation = self.memtable_generation.wrapping_add(1);
    }

    fn segment_stats(&self) -> (usize, usize, usize) {
        let total_nodes = self
            .segments
            .iter()
            .map(|segment| segment.meta.doc_count as usize)
            .sum::<usize>()
            .saturating_add(self.memtable.len());
        let deleted_nodes = self
            .segments
            .iter()
            .map(|segment| {
                if let Some(source) = &segment.source {
                    source
                        .entries
                        .iter()
                        .filter(|entry| !self.is_current(&entry.id, entry.doc_version))
                        .count()
                } else if let Some(index) = &segment.index {
                    index
                        .ids
                        .iter()
                        .zip(&index.doc_versions)
                        .filter(|(id, version)| !self.is_current(id, **version))
                        .count()
                } else {
                    0
                }
            })
            .sum::<usize>()
            .saturating_add(self.memtable.values().filter(|doc| doc.deleted).count());
        (self.segments.len(), total_nodes, deleted_nodes)
    }

    fn delta_stats(&self) -> (usize, usize) {
        let delta_nodes = self.delta_index.as_ref().map_or(0, |index| index.node_count());
        let exact_tail_docs = self
            .memtable
            .values()
            .filter(|doc| {
                doc.doc_version > self.delta_index_through
                    && !doc.deleted
                    && self.is_current(&doc.id, doc.doc_version)
            })
            .count();
        (delta_nodes, exact_tail_docs)
    }

    fn rerank_source_stats(&self) -> (usize, usize) {
        let sources = self
            .segments
            .iter()
            .filter_map(|segment| segment.source.as_ref())
            ;
        sources.fold((0usize, 0usize), |(docs, bytes), source| {
            let source_docs = source.entries.len();
            let source_bytes = source
                .entries
                .iter()
                .map(|entry| entry.vector.len().saturating_mul(std::mem::size_of::<f32>()))
                .sum::<usize>();
            (
                docs.saturating_add(source_docs),
                bytes.saturating_add(source_bytes),
            )
        })
    }

    fn memtable_batch(&self, limit: usize, force: bool) -> Option<Vec<VectorDocRecord>> {
        if self.memtable.is_empty() || (!force && self.memtable.len() < limit) {
            return None;
        }
        let mut entries = self
            .memtable
            .values()
            .map(|doc| doc.as_ref().clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.doc_version
                .cmp(&right.doc_version)
                .then_with(|| left.id.cmp(&right.id))
        });
        entries.truncate(limit.max(1));
        Some(entries)
    }

    fn publish_segment(
        &mut self,
        meta: VectorSegmentMeta,
        source: Arc<VectorSegmentBlob>,
        index: Option<Arc<VectorHnswIndexBlob>>,
    ) {
        for doc in &source.entries {
            if self
                .memtable
                .get(&doc.id)
                .is_some_and(|current| current.doc_version == doc.doc_version)
            {
                self.memtable.remove(&doc.id);
            }
        }
        self.mark_memtable_changed();
        self.next_segment_id = self
            .next_segment_id
            .max(meta.segment_id.saturating_add(1));
        self.segments.push(VectorSegmentRuntime {
            meta,
            source: Some(source),
            index,
        });
        self.segments
            .sort_by_key(|segment| (segment.meta.level, segment.meta.segment_id));
    }

    fn acknowledge_memtable(&mut self, entries: &[VectorDocRecord]) {
        let flushed_through = entries
            .iter()
            .map(|doc| doc.doc_version)
            .max()
            .unwrap_or(0);
        for doc in entries {
            if self
                .memtable
                .get(&doc.id)
                .is_some_and(|current| current.doc_version == doc.doc_version)
            {
                self.memtable.remove(&doc.id);
            }
        }
        self.mark_memtable_changed();
        if self.delta_index_through > 0 && flushed_through >= self.delta_index_through {
            self.delta_index = None;
            self.delta_index_through = 0;
        }
    }

    fn publish_delta_index(
        &mut self,
        expected_previous_through: u64,
        through: u64,
        index: Option<Arc<VectorHnswIndexBlob>>,
    ) -> bool {
        if self.delta_index_through != expected_previous_through {
            return false;
        }
        // The memtable is already the durable delta source of truth. Keep the
        // packed graph resident, and reconstruct an aligned FP32 view only
        // for an explicit RERANK query.
        self.delta_index = index;
        self.delta_index_through = through;
        true
    }

    fn cache_segment_source(&mut self, segment_id: u64, source: Arc<VectorSegmentBlob>) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.meta.segment_id == segment_id)
        {
            segment.source = Some(source);
        }
    }

    fn cache_segment_index(&mut self, segment_id: u64, index: Arc<VectorHnswIndexBlob>) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.meta.segment_id == segment_id)
        {
            segment.index = Some(index);
        }
    }

    fn publish_segment_index(
        &mut self,
        segment_id: u64,
        index_key: Vec<u8>,
        index: Arc<VectorHnswIndexBlob>,
    ) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.meta.segment_id == segment_id)
        {
            segment.meta.index_key = index_key;
            segment.index = Some(index);
            // Indexed source blobs are loaded on demand only for an explicit
            // FP32 rerank query. Keeping them resident doubles vector memory.
            segment.source = None;
        }
    }

    fn replace_segments_with_index(
        &mut self,
        removed: &HashSet<u64>,
        replacement: VectorSegmentMeta,
        source: Arc<VectorSegmentBlob>,
        index: Arc<VectorHnswIndexBlob>,
    ) {
        self.segments
            .retain(|segment| !removed.contains(&segment.meta.segment_id));
        let replacement_id = replacement.segment_id;
        self.publish_segment(replacement, source, Some(index));
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.meta.segment_id == replacement_id)
        {
            segment.source = None;
        }
    }

    fn remove_segments(&mut self, removed: &HashSet<u64>) {
        self.segments
            .retain(|segment| !removed.contains(&segment.meta.segment_id));
    }

    fn is_current(&self, id: &str, doc_version: u64) -> bool {
        self.current_versions
            .get(id)
            .is_some_and(|version| *version == doc_version)
    }

    fn set_attrs(&mut self, id: &str, attrs_json: String) {
        if let Some(doc) = self.memtable.get_mut(id) {
            Arc::make_mut(doc).attrs_json = attrs_json;
            self.mark_memtable_changed();
        }
    }

    fn links(&self, id: &str) -> Option<Vec<Vec<(String, f32)>>> {
        if let Some(layers) = self
            .delta_index
            .as_ref()
            .and_then(|index| index.links(id, &self.current_versions))
        {
            return Some(layers);
        }
        if let Some(layers) = self
            .segments
            .iter()
            .rev()
            .filter_map(|segment| segment.index.as_ref())
            .find_map(|index| index.links(id, &self.current_versions))
        {
            return Some(layers);
        }

        // A memtable or source-only segment has no graph topology yet.  Keep
        // VLINKS distinct from "element missing" by exposing an exact,
        // temporary layer from the mutable/source-only component until the
        // background HNSW publication completes.
        let origin = self
            .memtable
            .get(id)
            .filter(|doc| !doc.deleted && self.is_current(id, doc.doc_version))
            .map(|doc| doc.vector.as_slice())
            .or_else(|| {
                self.segments
                    .iter()
                    .filter_map(|segment| segment.source.as_ref())
                    .flat_map(|source| source.entries.iter())
                    .find(|entry| entry.id == id && self.is_current(id, entry.doc_version))
                    .map(|entry| entry.vector.as_slice())
            })?;
        let mut neighbors = self
            .memtable
            .values()
            .filter(|candidate| {
                candidate.id != id
                    && !candidate.deleted
                    && self.is_current(&candidate.id, candidate.doc_version)
            })
            .map(|candidate| (candidate.id.as_str(), candidate.vector.as_slice()))
            .chain(
                self.segments
                    .iter()
                    .filter_map(|segment| segment.source.as_ref())
                    .flat_map(|source| source.entries.iter())
                    .filter(|candidate| {
                        candidate.id != id
                            && self.is_current(&candidate.id, candidate.doc_version)
                    })
                    .map(|candidate| (candidate.id.as_str(), candidate.vector.as_slice())),
            )
            .filter_map(|(candidate_id, vector)| {
                Some((
                    candidate_id.to_string(),
                    distance_score(self.config.distance, origin, vector).ok()?,
                ))
            })
            .collect::<Vec<_>>();
        neighbors.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        neighbors.truncate(self.config.m.max(1));
        Some(if neighbors.is_empty() {
            Vec::new()
        } else {
            vec![neighbors]
        })
    }

    fn search_snapshot(&self) -> VectorRuntimeSearchSnapshot {
        let memtable = {
            let mut cached = self
                .memtable_snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cached.0 != self.memtable_generation {
                *cached = (
                    self.memtable_generation,
                    Arc::new(self.memtable.values().cloned().collect()),
                );
            }
            Arc::clone(&cached.1)
        };
        VectorRuntimeSearchSnapshot {
            segments: self.segments.clone(),
            memtable,
            current_versions: Arc::clone(&self.current_versions),
            delta_index: self.delta_index.clone(),
            delta_index_through: self.delta_index_through,
            config: self.config.clone(),
        }
    }

    #[cfg(test)]
    fn search(
        &self,
        query: &[f32],
        candidate_limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        self.search_snapshot()
            .search(query, candidate_limit, ef, allow_doc_ids)
    }
}

impl VectorRuntimeSearchSnapshot {
    fn is_current(&self, id: &str, doc_version: u64) -> bool {
        self.current_versions
            .get(id)
            .is_some_and(|version| *version == doc_version)
    }

    fn brute_force_entries<'a, T: VectorSearchEntry + 'a>(
        &'a self,
        entries: impl Iterator<Item = &'a T>,
        query: &[f32],
        query_norm_squared: f64,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        let mut output = Vec::new();
        for entry in entries {
            if entry.deleted()
                || allow_doc_ids.is_some_and(|allowed| !allowed.contains(entry.id()))
            {
                continue;
            }
            if !self.is_current(entry.id(), entry.doc_version()) {
                continue;
            }
            output.push(VectorCandidate {
                id: entry.id().to_string(),
                doc_version: entry.doc_version(),
                distance: distance_score_prepared(
                    self.config.distance,
                    query,
                    query_norm_squared,
                    entry.vector(),
                )?,
                source_position: None,
            });
        }
        Ok(output)
    }

    #[cfg(test)]
    fn search(
        &self,
        query: &[f32],
        candidate_limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        self.search_with_base_limit(
            query,
            candidate_limit,
            ef,
            allow_doc_ids,
        )
    }

    fn search_with_base_limit(
        &self,
        query: &[f32],
        candidate_limit: usize,
        ef: usize,
        allow_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<VectorCandidate>, Error> {
        let mut candidates = Vec::new();
        let query_norm_squared = vector_norm_squared(query);
        let query_payload = hnsw_query_payload(
            self.config.distance,
            query,
            self.config.quantization,
        );
        let indexed_node_counts = self
            .segments
            .iter()
            .filter_map(|segment| segment.index.as_ref())
            .map(|index| index.node_count())
            .chain(self.delta_index.iter().map(|index| index.node_count()))
            .collect::<Vec<_>>();
        let indexed_nodes = indexed_node_counts.iter().sum::<usize>();
        let minimum_fanout = indexed_node_counts.len().min(indexed_nodes);
        let candidate_budget = candidate_limit
            .max(minimum_fanout)
            .min(indexed_nodes.max(1));
        let candidate_budgets =
            distribute_vector_budget(&indexed_node_counts, candidate_budget);
        let ef_budgets = distribute_vector_budget(
            &indexed_node_counts,
            ef.max(candidate_budget).min(indexed_nodes.max(1)),
        );
        global_metrics().record_vector_search_ef_budget(ef_budgets.iter().sum());
        let mut graph_position = 0usize;
        global_metrics().record_vector_search_graphs(indexed_node_counts.len());
        for segment in &self.segments {
            let mut segment_candidates = if let Some(index) = &segment.index {
                let segment_limit = candidate_budgets[graph_position];
                let segment_ef = ef_budgets[graph_position].max(segment_limit);
                graph_position += 1;
                index.search_prepared(
                    query,
                    &query_payload,
                    segment_limit,
                    segment_ef,
                    allow_doc_ids,
                    &self.current_versions,
                )?
            } else {
                let source = segment.source.as_ref().ok_or_else(|| {
                    Error::msg("ERR vector source segment is not loaded")
                })?;
                self.brute_force_entries(
                    source.entries.iter(),
                    query,
                    query_norm_squared,
                    allow_doc_ids,
                )?
            };
            segment_candidates.sort_by(|left, right| {
                left.distance
                    .total_cmp(&right.distance)
                    .then_with(|| left.id.cmp(&right.id))
            });
            segment_candidates.truncate(candidate_limit);
            candidates.extend(segment_candidates);
            if candidates.len() > candidate_limit.saturating_mul(2) {
                candidates = reduce_vector_candidates(candidates, candidate_limit)?;
            }
        }

        if let Some(index) = &self.delta_index {
            let delta_limit = candidate_budgets[graph_position];
            let delta_ef = ef_budgets[graph_position].max(delta_limit);
            let delta_candidates = index.search_prepared(
                query,
                &query_payload,
                delta_limit,
                delta_ef,
                allow_doc_ids,
                &self.current_versions,
            )?;
            candidates.extend(delta_candidates);
            if candidates.len() > candidate_limit.saturating_mul(2) {
                candidates = reduce_vector_candidates(candidates, candidate_limit)?;
            }
        }

        let exact_tail_docs = self
            .memtable
            .iter()
            .filter(|doc| doc.doc_version > self.delta_index_through)
            .count();
        global_metrics().record_vector_exact_tail_docs(exact_tail_docs);
        global_metrics().record_vector_memtable_scan(exact_tail_docs);
        candidates.extend(self.brute_force_entries(
            self.memtable
                .iter()
                .map(Arc::as_ref)
                .filter(|doc| doc.doc_version > self.delta_index_through),
            query,
            query_norm_squared,
            allow_doc_ids,
        )?);
        reduce_vector_candidates(candidates, candidate_limit)
    }
}

fn distribute_vector_budget(node_counts: &[usize], requested: usize) -> Vec<usize> {
    let total_nodes = node_counts.iter().sum::<usize>();
    if total_nodes == 0 {
        return vec![0; node_counts.len()];
    }
    let active = node_counts.iter().filter(|count| **count > 0).count();
    let mut remaining_budget = requested.max(active).min(total_nodes);
    let mut remaining_nodes = total_nodes;
    let mut remaining_graphs = active;
    let mut budgets = Vec::with_capacity(node_counts.len());
    for &nodes in node_counts {
        if nodes == 0 {
            budgets.push(0);
            continue;
        }
        let reserve = remaining_graphs.saturating_sub(1);
        let proportional = remaining_budget
            .saturating_mul(nodes)
            .div_ceil(remaining_nodes.max(1));
        let budget = proportional
            .max(1)
            .min(nodes)
            .min(remaining_budget.saturating_sub(reserve).max(1));
        budgets.push(budget);
        remaining_budget = remaining_budget.saturating_sub(budget);
        remaining_nodes = remaining_nodes.saturating_sub(nodes);
        remaining_graphs = remaining_graphs.saturating_sub(1);
    }
    budgets
}
