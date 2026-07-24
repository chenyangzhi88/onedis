type VectorRuntimeSegmentEntry = (u64, Vec<u8>, Vec<(String, u64)>);

impl Db {
    fn vector_runtime_segment_entries(
        &self,
        index: &str,
        version: u64,
    ) -> Result<Vec<VectorRuntimeSegmentEntry>, Error> {
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(Vec::new());
        };
        let runtime = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        Ok(runtime
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.meta.segment_id,
                    segment.meta.graph_key.clone(),
                    segment
                        .graph
                        .nodes
                        .iter()
                        .filter(|node| !node.deleted)
                        .map(|node| (node.id.clone(), node.doc_version))
                        .collect(),
                )
            })
            .collect())
    }

    fn read_vector_meta(&self, index: &str) -> Result<(u64, u64, VectorIndexMeta), Error> {
        self.expire_if_needed(index);
        let Some(raw) = self.store.get_raw(&main_key(self.db_index, index)) else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) =
            self.store
                .get_raw(&vector_meta_key(self.db_index, index, header.version))
        else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        Ok((
            header.expire_ms,
            header.version,
            decode_record::<VectorIndexMeta>(&meta_raw)?,
        ))
    }

    fn ensure_vector_runtime(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(), Error> {
        if self
            .vector_runtimes
            .get(self.db_index, index, version)
            .is_some()
        {
            return Ok(());
        }
        let (segments, _replay_after, next_segment_id) =
            self.load_vector_graph_segments(index, version, meta)?;
        self.vector_runtimes.indexes.insert(
            VectorRuntimeRegistry::key(self.db_index, index, version),
            Arc::new(RwLock::new(VectorRuntime::with_segments(
                meta.dim as usize,
                meta.distance,
                meta.m as usize,
                meta.ef_construction as usize,
                meta.initial_cap as usize,
                next_segment_id,
                segments,
            ))),
        );
        let prefix = vector_doc_prefix(self.db_index, index, version);
        let mut docs = Vec::new();
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            docs.push(decode_record::<VectorDocRecord>(&raw)?);
        }
        self.vector_runtimes
            .reconcile_docs(self.db_index, index, version, docs)?;
        Ok(())
    }

    fn load_vector_graph_segments(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(Vec<VectorSegmentRuntime>, u64, u64), Error> {
        let prefix = vector_segment_prefix(self.db_index, index, version);
        let mut segments = Vec::new();
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            let segment = decode_record::<VectorSegmentMeta>(&raw)?;
            if segment.graph_key.is_empty() {
                continue;
            }
            let Some(snapshot_raw) = self.store.get_raw(&segment.graph_key) else {
                continue;
            };
            let snapshot = decode_record::<HnswGraphSnapshot>(&snapshot_raw)?;
            segments.push(VectorSegmentRuntime {
                meta: segment,
                graph: HnswGraph::from_snapshot(snapshot)?,
            });
        }
        segments.sort_by_key(|segment| segment.meta.segment_id);
        let replay_after = segments
            .iter()
            .map(|segment| segment.meta.max_doc_version)
            .max()
            .unwrap_or(0);
        let next_segment_id = meta.next_segment_id.max(
            segments
                .iter()
                .map(|segment| segment.meta.segment_id.saturating_add(1))
                .max()
                .unwrap_or(1),
        );
        Ok((segments, replay_after, next_segment_id))
    }

    async fn read_vector_meta_async(
        &self,
        index: &str,
    ) -> Result<(u64, u64, VectorIndexMeta), Error> {
        self.expire_if_needed_async(index).await;
        let Some(raw) = self
            .store
            .get_raw_async(&main_key(self.db_index, index))
            .await
        else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) = self
            .store
            .get_raw_async(&vector_meta_key(self.db_index, index, header.version))
            .await
        else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        Ok((
            header.expire_ms,
            header.version,
            decode_record::<VectorIndexMeta>(&meta_raw)?,
        ))
    }
}
