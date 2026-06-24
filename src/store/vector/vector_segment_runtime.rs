struct VectorSegmentRuntime {
    meta: VectorSegmentMeta,
    graph: HnswGraph,
}

struct VectorRuntime {
    active: HnswGraph,
    segments: Vec<VectorSegmentRuntime>,
    next_segment_id: u64,
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
