impl Db {
    pub(in crate::store::db) fn reconcile_vector_runtime_index(&self, db_index: u16, index: &str) {
        let store = self.store.non_transactional_view().for_db_index(db_index);
        let current_version = store
            .get_raw(&self.key_layout.main_key(db_index, index))
            .and_then(|raw| decode_meta_header(&raw))
            .filter(|header| header.type_tag == TYPE_VECTOR)
            .map(|header| header.version);
        self.vector_runtimes
            .retain_index_version(db_index, index, current_version);
    }

    pub(in crate::store::db) fn reconcile_vector_runtimes_for_batch(&self, batch: &WriteBatch) {
        if !self.vector_runtimes.has_active_runtimes() {
            return;
        }
        let (keys, dbs) = super::collect_logical_mutations(self.key_layout, self.db_index, batch);
        if dbs.contains(&self.db_index) {
            self.vector_runtimes.remove_db(self.db_index);
            return;
        }
        let mut seen = HashSet::new();
        for key in keys {
            if !seen.insert(key.clone()) {
                continue;
            }
            if let Ok(index) = std::str::from_utf8(&key) {
                self.reconcile_vector_runtime_index(self.db_index, index);
            }
        }
    }

    pub(in crate::store::db) fn reconcile_vector_runtimes_for_known_keys(&self, keys: &[&str]) {
        if !self.vector_runtimes.has_active_runtimes() {
            return;
        }
        for index in keys {
            self.reconcile_vector_runtime_index(self.db_index, index);
        }
    }

    fn read_vector_meta_observed(
        &self,
        index: &str,
    ) -> Result<(u64, u64, VectorIndexMeta, Vec<u8>, Vec<u8>), Error> {
        let internal = is_internal_fulltext_vector_index(index);
        if !internal {
            self.expire_if_needed(index);
        }
        let marker_key = if internal {
            vector_internal_marker_key(self.key_layout, self.db_index, index)
        } else {
            self.mk(index)
        };
        let Some(raw) = self.store.get_raw(&marker_key) else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) = self.store.get_raw(&vector_meta_key(
            self.key_layout,
            self.db_index,
            index,
            header.version,
        )) else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        let meta = decode_vector_meta(&meta_raw)?;
        validate_vector_meta_config(&meta)?;
        if meta.internal != internal {
            return Err(Error::msg("ERR invalid vector index ownership"));
        }
        Ok((
            header.expire_ms,
            header.version,
            meta,
            raw.to_vec(),
            meta_raw.to_vec(),
        ))
    }

    fn read_vector_meta(&self, index: &str) -> Result<(u64, u64, VectorIndexMeta), Error> {
        let (expire_ms, version, meta, _, _) = self.read_vector_meta_observed(index)?;
        Ok((expire_ms, version, meta))
    }

    fn vector_marker_key(&self, index: &str, internal: bool) -> Vec<u8> {
        if internal {
            vector_internal_marker_key(self.key_layout, self.db_index, index)
        } else {
            self.mk(index)
        }
    }

    fn commit_vector_batch_if_marker_unchanged(
        &self,
        index: &str,
        internal: bool,
        version: u64,
        expected_marker: &[u8],
        expected_meta: &[u8],
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        let marker_key = self.vector_marker_key(index, internal);
        let meta_key = vector_meta_key(self.key_layout, self.db_index, index, version);
        if self.compare_and_write_batch_if_not_empty(
            &[
                CompareCondition::exists_with(&marker_key, expected_marker),
                CompareCondition::exists_with(&meta_key, expected_meta),
            ],
            batch,
        )? {
            Ok(())
        } else {
            Err(Error::msg("ERR vector index changed during write"))
        }
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
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        self.ensure_vector_runtime_unlocked(index, version, meta)
    }

    fn ensure_vector_runtime_unlocked(
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
        if meta.algorithm == VectorIndexAlgorithm::Flat {
            return Ok(());
        }
        let (segments, _replay_after, next_segment_id) =
            self.load_vector_graph_segments(index, version, meta)?;
        let needs_maintenance = segments
            .iter()
            .any(|segment| segment.meta.index_key.is_empty())
            || meta.snapshot_doc_version.saturating_add(1) < meta.next_doc_version;
        let (current_versions, tail_docs) =
            self.load_vector_version_state(index, version, meta)?;
        let mut runtime = VectorRuntime::with_segments(
            meta.dim as usize,
            meta.distance,
            meta.m as usize,
            meta.ef_construction as usize,
            meta.initial_cap as usize,
            next_segment_id,
            segments,
            meta.quantization,
        );
        runtime.restore_version_state(current_versions, tail_docs);
        // Publish only after recovery is complete. A concurrent reader can no
        // longer observe the old partially initialized empty runtime.
        self.vector_runtimes.insert_runtime(
            VectorRuntimeRegistry::key(self.db_index, index, version),
            Arc::new(RwLock::new(runtime)),
        );
        if needs_maintenance {
            self.vector_runtimes
                .mark_dirty(self.db_index, index, version);
        }
        Ok(())
    }

    fn load_vector_version_state(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(HashMap<String, u64>, Vec<VectorDocRecord>), Error> {
        let checkpoint = self
            .store
            .get_raw(&vector_version_checkpoint_key(
                self.key_layout,
                self.db_index,
                index,
                version,
            ))
            .map(|raw| decode_record::<VectorVersionCheckpoint>(&raw))
            .transpose()?;
        let checkpoint_through = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.through_doc_version);
        let mut current_versions = checkpoint
            .map(|checkpoint| checkpoint.current_versions.into_iter().collect::<HashMap<_, _>>())
            .unwrap_or_default();
        let mutation_prefix = vector_version_mutation_prefix(
            self.key_layout,
            self.db_index,
            index,
            version,
        );
        let mutations = self.store.scan_prefix_raw(&mutation_prefix);
        let mut expected_version = checkpoint_through.saturating_add(1);
        let mut latest_tail = HashMap::<String, u64>::new();
        let mut complete = checkpoint_through < meta.next_doc_version;
        for (key, raw) in mutations {
            let mutation = decode_record::<VectorVersionMutation>(&raw)?;
            if mutation.doc_version != expected_version
                || key
                    != vector_version_mutation_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        mutation.doc_version,
                    )
            {
                complete = false;
                break;
            }
            if mutation.deleted {
                current_versions.remove(&mutation.id);
            } else {
                current_versions.insert(mutation.id.clone(), mutation.doc_version);
            }
            if mutation.doc_version > meta.snapshot_doc_version {
                latest_tail.insert(mutation.id, mutation.doc_version);
            }
            expected_version = expected_version.saturating_add(1);
        }
        complete &= expected_version == meta.next_doc_version;

        if complete {
            let ids = latest_tail.keys().collect::<Vec<_>>();
            let keys = ids
                .iter()
                .map(|id| {
                    vector_doc_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        id,
                    )
                })
                .collect::<Vec<_>>();
            let mut tail_docs = Vec::with_capacity(keys.len());
            for (id, raw) in ids.into_iter().zip(self.store.multi_get_raw(&keys)) {
                let raw = raw.ok_or_else(|| Error::msg("ERR vector mutation document missing"))?;
                let doc = decode_record::<VectorDocRecord>(&raw)?;
                if latest_tail.get(id).copied() != Some(doc.doc_version) {
                    return Err(Error::msg("ERR vector mutation version mismatch"));
                }
                tail_docs.push(doc);
            }
            return Ok((current_versions, tail_docs));
        }

        // Old indexes do not have a mutation journal. Keep them readable and
        // migrate naturally as the next explicit compaction writes a checkpoint.
        let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let docs = self
            .store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .map(|(_, raw)| decode_record::<VectorDocRecord>(&raw))
            .collect::<Result<Vec<_>, Error>>()?;
        let mut current_versions = HashMap::with_capacity(docs.len());
        let mut tail_docs = Vec::new();
        for doc in docs {
            if !doc.deleted {
                current_versions.insert(doc.id.clone(), doc.doc_version);
            }
            if doc.doc_version > meta.snapshot_doc_version {
                tail_docs.push(doc);
            }
        }
        Ok((current_versions, tail_docs))
    }

    fn load_vector_graph_segments(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(Vec<VectorSegmentRuntime>, u64, u64), Error> {
        let prefix = vector_segment_prefix(self.key_layout, self.db_index, index, version);
        let mut segments = Vec::new();
        for (key, raw) in self.store.scan_prefix_raw(&prefix) {
            let segment = decode_record::<VectorSegmentMeta>(&raw)?;
            if segment.source_key.is_empty()
                || segment.doc_count == 0
                || segment.min_doc_version == 0
                || segment.min_doc_version > segment.max_doc_version
                || segment.doc_count > meta.max_segment_docs
                || key
                    != vector_segment_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        segment.segment_id,
                    )
                || segment.source_key
                    != vector_segment_source_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        segment.segment_id,
                    )
                || (!segment.index_key.is_empty()
                    && segment.index_key
                        != vector_segment_index_key(
                            self.key_layout,
                            self.db_index,
                            index,
                            version,
                            segment.segment_id,
                        ))
            {
                return Err(Error::msg("ERR invalid persisted vector LSM segment"));
            }
            segments.push(VectorSegmentRuntime {
                meta: segment,
                source: None,
                index: None,
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

    fn decode_vector_segment_source(
        &self,
        segment: &VectorSegmentMeta,
        meta: &VectorIndexMeta,
    ) -> Result<Arc<VectorSegmentBlob>, Error> {
        let raw = self
            .store
            .get_raw(&segment.source_key)
            .ok_or_else(|| Error::msg("ERR vector source segment blob missing"))?;
        let source = decode_record::<VectorSegmentBlob>(&raw)?;
        if source.entries.len() != segment.doc_count as usize || source.entries.is_empty() {
            return Err(Error::msg("ERR invalid vector source segment blob"));
        }
        let mut ids = HashSet::with_capacity(source.entries.len());
        for doc in &source.entries {
            if doc.id.is_empty()
                || doc.doc_version == 0
                || !ids.insert(doc.id.as_str())
            {
                return Err(Error::msg("ERR invalid vector source segment document"));
            }
            validate_vector(&doc.vector, meta.dim as usize)?;
            validate_vector_for_distance(&doc.vector, meta.distance)?;
        }
        let actual_min = source
            .entries
            .iter()
            .map(|doc| doc.doc_version)
            .min()
            .unwrap_or(0);
        let actual_max = source
            .entries
            .iter()
            .map(|doc| doc.doc_version)
            .max()
            .unwrap_or(0);
        if actual_min != segment.min_doc_version || actual_max != segment.max_doc_version {
            return Err(Error::msg("ERR vector source segment version mismatch"));
        }
        Ok(Arc::new(source))
    }

    fn decode_vector_segment_index(
        &self,
        segment: &VectorSegmentMeta,
        meta: &VectorIndexMeta,
    ) -> Result<Arc<VectorHnswIndexBlob>, Error> {
        let raw = self
            .store
            .get_raw(&segment.index_key)
            .ok_or_else(|| Error::msg("ERR vector HNSW index blob missing"))?;
        let index_blob = decode_vector_hnsw_index(&raw)?;
        index_blob.validate()?;
        if index_blob.dim != meta.dim
            || index_blob.distance != meta.distance
            || index_blob.m != meta.m
            || index_blob.ef_construction != meta.ef_construction
            || index_blob.quantization != meta.quantization
            || index_blob.node_count() != segment.doc_count as usize
        {
            return Err(Error::msg("ERR persisted vector HNSW config mismatch"));
        }
        Ok(Arc::new(index_blob))
    }

    fn ensure_vector_search_segments_loaded(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
    ) -> Result<(), Error> {
        let runtime = self
            .vector_runtimes
            .get(self.db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let missing = {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            runtime
                .segments
                .iter()
                .filter_map(|segment| {
                    if segment.meta.index_key.is_empty() && segment.source.is_none() {
                        Some((segment.meta.clone(), true))
                    } else if !segment.meta.index_key.is_empty() && segment.index.is_none() {
                        Some((segment.meta.clone(), false))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return Ok(());
        }
        let mut loaded_sources = Vec::new();
        let mut loaded_indexes = Vec::new();
        for (segment, source_needed) in missing {
            if source_needed {
                loaded_sources.push((
                    segment.segment_id,
                    self.decode_vector_segment_source(&segment, meta)?,
                ));
            } else {
                loaded_indexes.push((
                    segment.segment_id,
                    self.decode_vector_segment_index(&segment, meta)?,
                ));
            }
        }
        let mut runtime = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        for (segment_id, source) in loaded_sources {
            runtime.cache_segment_source(segment_id, source);
        }
        for (segment_id, index_blob) in loaded_indexes {
            runtime.cache_segment_index(segment_id, index_blob);
        }
        Ok(())
    }

    fn ensure_vector_segment_sources_loaded(
        &self,
        index: &str,
        version: u64,
        meta: &VectorIndexMeta,
        segment_ids: &HashSet<u64>,
    ) -> Result<(), Error> {
        let runtime = self
            .vector_runtimes
            .get(self.db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let missing = {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            runtime
                .segments
                .iter()
                .filter(|segment| {
                    segment.source.is_none()
                        && segment_ids.contains(&segment.meta.segment_id)
                })
                .map(|segment| segment.meta.clone())
                .collect::<Vec<_>>()
        };
        let loaded = missing
            .iter()
            .map(|segment| {
                Ok((
                    segment.segment_id,
                    self.decode_vector_segment_source(segment, meta)?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut runtime = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        for (segment_id, source) in loaded {
            runtime.cache_segment_source(segment_id, source);
        }
        Ok(())
    }

    async fn read_vector_meta_async(
        &self,
        index: &str,
    ) -> Result<(u64, u64, VectorIndexMeta), Error> {
        let internal = is_internal_fulltext_vector_index(index);
        if !internal {
            self.expire_if_needed_async(index).await;
        }
        let marker_key = if internal {
            vector_internal_marker_key(self.key_layout, self.db_index, index)
        } else {
            self.mk(index)
        };
        let Some(raw) = self.store.get_raw_async(&marker_key).await else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        let header = decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        let Some(meta_raw) = self
            .store
            .get_raw_async(&vector_meta_key(
                self.key_layout,
                self.db_index,
                index,
                header.version,
            ))
            .await
        else {
            return Err(Error::msg("ERR vector index metadata missing"));
        };
        let meta = decode_vector_meta(&meta_raw)?;
        validate_vector_meta_config(&meta)?;
        if meta.internal != internal {
            return Err(Error::msg("ERR invalid vector index ownership"));
        }
        Ok((header.expire_ms, header.version, meta))
    }
}
