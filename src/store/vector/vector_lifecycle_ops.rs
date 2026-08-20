impl Db {
    pub fn vector_drop(&self, index: &str) -> Result<usize, Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (_, version, meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        let mut batch = WriteBatch::new();
        if meta.internal {
            batch.delete(&vector_internal_marker_key(
                self.key_layout,
                self.db_index,
                index,
            ))?;
        } else {
            batch.delete(&self.mk(index))?;
        }
        delete_vector_namespace_to_batch(
            &mut batch,
            self.key_layout,
            self.db_index,
            index,
            version,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;
        self.vector_runtimes.remove(self.db_index, index, version);
        drop(_guard);
        drop(write_lock);
        self.vector_runtimes
            .cleanup_write_lock_if_idle(self.db_index, index);
        Ok(1)
    }

    pub async fn vector_drop_async(&self, index: &str) -> Result<usize, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_drop(&index))
            .await
    }

    pub fn vector_rebuild(&self, index: &str) -> Result<(), Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (expire_ms, version, mut meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        let mut batch = WriteBatch::new();
        delete_vector_segments_to_batch(
            &mut batch,
            self.key_layout,
            self.db_index,
            index,
            version,
        )?;
        batch.delete(&vector_version_checkpoint_key(
            self.key_layout,
            self.db_index,
            index,
            version,
        ))?;
        meta.next_segment_id = 1;
        meta.snapshot_doc_version = 0;
        put_vector_marker_to_batch(
            &mut batch,
            VectorMarker {
                layout: self.key_layout,
                db_index: self.db_index,
                index,
                expire_ms,
                version,
                dim: meta.dim,
                internal: meta.internal,
            },
        )?;
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(&meta)?,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;

        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.vector_runtimes.reset(
                self.db_index,
                index,
                version,
                VectorRuntimeConfig::from(&meta),
            );
            let prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
            let docs = self
                .store
                .scan_prefix_raw(&prefix)?
                .into_iter()
                .map(|(_, raw)| decode_record::<VectorDocRecord>(&raw))
                .collect::<Result<Vec<_>, Error>>()?;
            self.vector_runtimes
                .reconcile_docs(self.db_index, index, version, docs, 0)?;
            if meta.doc_count > 0 {
                self.vector_runtimes
                    .mark_dirty(self.db_index, index, version);
            }
        }
        Ok(())
    }

    pub async fn vector_rebuild_async(&self, index: &str) -> Result<(), Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_rebuild(&index))
            .await
    }

    fn build_vector_hnsw_index(
        source: &VectorSegmentBlob,
        meta: &VectorIndexMeta,
    ) -> Result<VectorHnswIndexBlob, Error> {
        VectorHnswIndexBlob::build(source, meta)
    }

    fn flush_vector_memtable_locked(
        &self,
        index: &str,
        version: u64,
        meta: &mut VectorIndexMeta,
        expected_marker: &[u8],
        expected_meta: &[u8],
        force: bool,
    ) -> Result<bool, Error> {
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(false);
        };
        let entries = {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            runtime.memtable_batch(meta.segment_max_docs.max(1) as usize, force)
        };
        let Some(entries) = entries else {
            return Ok(false);
        };
        let flushed_through = entries
            .iter()
            .map(|doc| doc.doc_version)
            .max()
            .unwrap_or(meta.snapshot_doc_version);
        let mut source_entries = {
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            entries
                .iter()
                .filter(|doc| {
                    !doc.deleted
                        && runtime.is_current(&doc.id, doc.doc_version)
                })
                .map(VectorSegmentEntry::from)
                .collect::<Vec<_>>()
        };
        source_entries.sort_by(|left, right| {
            left.doc_version
                .cmp(&right.doc_version)
                .then_with(|| left.id.cmp(&right.id))
        });

        let mut batch = WriteBatch::new();
        let published = if source_entries.is_empty() {
            None
        } else {
            let segment_id = meta.next_segment_id.max(1);
            let source_key = vector_segment_source_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            let source = Arc::new(VectorSegmentBlob {
                entries: source_entries,
            });
            let segment = VectorSegmentMeta {
                segment_id,
                level: 0,
                source_key: source_key.clone(),
                index_key: Vec::new(),
                doc_count: source.entries.len() as u64,
                min_doc_version: source
                    .entries
                    .first()
                    .map(|doc| doc.doc_version)
                    .unwrap_or(flushed_through),
                max_doc_version: source
                    .entries
                    .last()
                    .map(|doc| doc.doc_version)
                    .unwrap_or(flushed_through),
            };
            // Publish the immutable blob before its metadata.  An interrupted
            // write can leave an unreachable blob, but never a visible segment
            // whose source is missing.
            self.store
                .blob_put_raw(&source_key, &encode_record(source.as_ref())?)
                .map_err(|error| Error::msg(error.to_string()))?;
            batch.put(
                &vector_segment_key(
                    self.key_layout,
                    self.db_index,
                    index,
                    version,
                    segment_id,
                ),
                &encode_record(&segment)?,
            )?;
            meta.next_segment_id = segment_id.saturating_add(1);
            Some((segment, source))
        };
        meta.snapshot_doc_version = meta.snapshot_doc_version.max(flushed_through);
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(meta)?,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            expected_marker,
            expected_meta,
            &batch,
        )?;

        let mut runtime = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
        runtime.acknowledge_memtable(&entries);
        if let Some((segment, source)) = published {
            runtime.publish_segment(segment, source, None);
            global_metrics().record_vector_segments_persisted(1);
        }
        Ok(true)
    }

    fn flush_vector_memtable(
        &self,
        index: &str,
        expected_version: u64,
        force: bool,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (_, version, mut meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        if version != expected_version {
            return Err(Error::msg("ERR vector index changed during write"));
        }
        self.flush_vector_memtable_locked(
            index,
            version,
            &mut meta,
            &expected_marker,
            &expected_meta,
            force,
        )
    }

    /// Build outside the collection write lock.  Publication is guarded by
    /// the exact source-segment record, so concurrent VADD/VDEL operations do
    /// not stall and a rebuild/drop cannot resurrect the old segment.
    fn build_one_vector_segment_index(
        &self,
        index: &str,
        expected_version: u64,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let (meta, original_segment, source) = {
            let _guard = write_lock.blocking_lock();
            let (_, version, meta) = self.read_vector_meta(index)?;
            if version != expected_version {
                return Err(Error::msg("ERR vector index changed during write"));
            }
            let runtime = self
                .vector_runtimes
                .get(self.db_index, index, version)
                .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
            let pending_segment_id = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
                .segments
                .iter()
                .find(|segment| segment.meta.index_key.is_empty())
                .map(|segment| segment.meta.segment_id);
            let Some(pending_segment_id) = pending_segment_id else {
                return Ok(false);
            };
            self.ensure_vector_segment_sources_loaded(
                index,
                version,
                &meta,
                &HashSet::from([pending_segment_id]),
            )?;
            let (segment, source) = {
                let runtime = runtime
                    .read()
                    .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
                runtime
                    .segments
                    .iter()
                    .find(|segment| segment.meta.segment_id == pending_segment_id)
                    .and_then(|segment| {
                        segment
                            .source
                            .as_ref()
                            .map(|source| (segment.meta.clone(), Arc::clone(source)))
                    })
                    .ok_or_else(|| Error::msg("ERR vector source segment is not loaded"))?
            };
            (meta, segment, source)
        };

        let persisted = Arc::new(Self::build_vector_hnsw_index(source.as_ref(), &meta)?);
        let encoded_index = encode_record(persisted.as_ref())?;

        let _guard = write_lock.blocking_lock();
        let (_, version, current_meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        if version != expected_version {
            return Err(Error::msg("ERR vector index changed during write"));
        }
        let segment_key = vector_segment_key(
            self.key_layout,
            self.db_index,
            index,
            version,
            original_segment.segment_id,
        );
        let original_segment_raw = encode_record(&original_segment)?;
        if self.store.get_raw(&segment_key)?.as_deref() != Some(original_segment_raw.as_slice()) {
            return Ok(false);
        }
        let index_key = vector_segment_index_key(
            self.key_layout,
            self.db_index,
            index,
            version,
            original_segment.segment_id,
        );
        self.store
            .blob_put_raw(&index_key, &encoded_index)
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut published_segment = original_segment;
        published_segment.index_key = index_key.clone();
        let mut batch = WriteBatch::new();
        batch.put(&segment_key, &encode_record(&published_segment)?)?;
        let conditions = [
            CompareCondition::exists_with(
                self.vector_marker_key(index, current_meta.internal),
                &expected_marker,
            ),
            CompareCondition::exists_with(
                vector_meta_key(self.key_layout, self.db_index, index, version),
                &expected_meta,
            ),
            CompareCondition::exists_with(&segment_key, &original_segment_raw),
        ];
        if !self.compare_and_write_vector_batch_if_not_empty(&conditions, &batch)? {
            return Ok(false);
        }
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) {
            runtime
                .write()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
                .publish_segment_index(published_segment.segment_id, index_key, persisted);
        }
        Ok(true)
    }

    /// Snapshot a same-level merge group under the write lock, build
    /// their replacement outside it, then CAS all source metadata at publish.
    fn merge_one_vector_segment_group(
        &self,
        index: &str,
        expected_version: u64,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let (build_meta, selected, removed, source, replacement_level, segment_id) = {
            let _guard = write_lock.blocking_lock();
            let (_, version, meta) = self.read_vector_meta(index)?;
            if version != expected_version {
                return Err(Error::msg("ERR vector index changed during write"));
            }
            let runtime = self
                .vector_runtimes
                .get(self.db_index, index, version)
                .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
            let selected_meta = {
                let runtime = runtime
                    .read()
                    .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
                let mut levels = runtime
                    .segments
                    .iter()
                    .filter(|segment| !segment.meta.index_key.is_empty())
                    .map(|segment| segment.meta.level)
                    .collect::<Vec<_>>();
                levels.sort_unstable();
                levels.dedup();
                levels.into_iter().find_map(|level| {
                    let mut same_level = runtime
                        .segments
                        .iter()
                        .filter(|segment| {
                            segment.meta.level == level && !segment.meta.index_key.is_empty()
                        })
                        .collect::<Vec<_>>();
                    same_level.sort_by_key(|segment| segment.meta.segment_id);
                    let group = same_level
                        .into_iter()
                        .take(VECTOR_LSM_MERGE_FACTOR)
                        .collect::<Vec<_>>();
                    (group.len() == VECTOR_LSM_MERGE_FACTOR
                        && group.iter().map(|segment| segment.meta.doc_count).sum::<u64>()
                            <= meta.max_segment_docs)
                        .then(|| {
                            group
                                .into_iter()
                                .map(|segment| segment.meta.clone())
                                .collect::<Vec<_>>()
                        })
                })
            };
            let Some(selected_meta) = selected_meta else {
                return Ok(false);
            };
            let selected_ids = selected_meta
                .iter()
                .map(|segment| segment.segment_id)
                .collect::<HashSet<_>>();
            self.ensure_vector_segment_sources_loaded(index, version, &meta, &selected_ids)?;
            let selected = {
                let runtime = runtime
                    .read()
                    .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
                selected_meta
                    .into_iter()
                    .map(|segment_meta| {
                        let source = runtime
                            .segments
                            .iter()
                            .find(|segment| segment.meta.segment_id == segment_meta.segment_id)
                            .and_then(|segment| segment.source.as_ref())
                            .ok_or_else(|| Error::msg("ERR vector source segment is not loaded"))?;
                        Ok((segment_meta, Arc::clone(source)))
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            };
            let (mut merged_entries, removed) = {
                let runtime = runtime
                    .read()
                    .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
                let mut merged = HashMap::<String, VectorSegmentEntry>::new();
                let mut removed = HashSet::new();
                for (segment, source) in &selected {
                    removed.insert(segment.segment_id);
                    for doc in &source.entries {
                        if runtime.is_current(&doc.id, doc.doc_version) {
                            merged.insert(doc.id.clone(), doc.clone());
                        }
                    }
                }
                (merged.into_values().collect::<Vec<_>>(), removed)
            };
            merged_entries.sort_by(|left, right| {
                left.doc_version
                    .cmp(&right.doc_version)
                    .then_with(|| left.id.cmp(&right.id))
            });
            let replacement_level = selected[0].0.level.saturating_add(1);
            let segment_id = meta.next_segment_id.max(1);
            (
                meta,
                selected,
                removed,
                Arc::new(VectorSegmentBlob {
                    entries: merged_entries,
                }),
                replacement_level,
                segment_id,
            )
        };

        let persisted = (!source.entries.is_empty())
            .then(|| Self::build_vector_hnsw_index(source.as_ref(), &build_meta))
            .transpose()?
            .map(Arc::new);
        let encoded_source = (!source.entries.is_empty())
            .then(|| encode_record(source.as_ref()))
            .transpose()?;
        let encoded_index = persisted
            .as_ref()
            .map(|index| encode_record(index.as_ref()))
            .transpose()?;

        let _guard = write_lock.blocking_lock();
        let (_, version, mut meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        if version != expected_version || meta.next_segment_id.max(1) != segment_id {
            return Err(Error::msg("ERR vector index changed during write"));
        }
        let mut conditions = vec![
            CompareCondition::exists_with(
                self.vector_marker_key(index, meta.internal),
                &expected_marker,
            ),
            CompareCondition::exists_with(
                vector_meta_key(self.key_layout, self.db_index, index, version),
                &expected_meta,
            ),
        ];
        let mut batch = WriteBatch::new();
        for (segment, _) in &selected {
            let segment_key = vector_segment_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment.segment_id,
            );
            conditions.push(CompareCondition::exists_with(
                &segment_key,
                encode_record(segment)?,
            ));
            batch.delete(&segment_key)?;
            batch.delete(&segment.source_key)?;
            batch.delete(&segment.index_key)?;
        }

        let replacement = if source.entries.is_empty() {
            None
        } else {
            let source_key = vector_segment_source_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            let index_key = vector_segment_index_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            let segment_key = vector_segment_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            conditions.push(CompareCondition::absent(&segment_key));
            self.store
                .blob_put_raw(&source_key, encoded_source.as_deref().unwrap())
                .map_err(|error| Error::msg(error.to_string()))?;
            self.store
                .blob_put_raw(&index_key, encoded_index.as_deref().unwrap())
                .map_err(|error| Error::msg(error.to_string()))?;
            let replacement = VectorSegmentMeta {
                segment_id,
                level: replacement_level,
                source_key,
                index_key,
                doc_count: source.entries.len() as u64,
                min_doc_version: source.entries.first().unwrap().doc_version,
                max_doc_version: source.entries.last().unwrap().doc_version,
            };
            batch.put(&segment_key, &encode_record(&replacement)?)?;
            meta.next_segment_id = segment_id.saturating_add(1);
            batch.put(
                &vector_meta_key(self.key_layout, self.db_index, index, version),
                &encode_record(&meta)?,
            )?;
            Some(replacement)
        };

        if !self.compare_and_write_vector_batch_if_not_empty(&conditions, &batch)? {
            return Ok(false);
        }
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            if let (Some(replacement), Some(persisted)) = (replacement, persisted) {
                runtime.replace_segments_with_index(&removed, replacement, source, persisted);
            } else {
                runtime.remove_segments(&removed);
            }
        }
        global_metrics().record_vector_compaction();
        Ok(true)
    }

    /// Build an ephemeral HNSW for the mutable LSM tail without adding graph
    /// maintenance to VADD/VDEL latency. Newer writes remain visible through
    /// the exact tail and superseded nodes are rejected by current_versions.
    fn build_vector_delta_index(
        &self,
        index: &str,
        expected_version: u64,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let (meta, previous_through, through, source) = {
            let _guard = write_lock.blocking_lock();
            let (_, version, meta) = self.read_vector_meta(index)?;
            if version != expected_version {
                return Err(Error::msg("ERR vector index changed during write"));
            }
            let runtime = self
                .vector_runtimes
                .get(self.db_index, index, version)
                .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?;
            let previous_through = runtime.delta_index_through;
            let changed = runtime
                .memtable
                .values()
                .filter(|doc| doc.doc_version > previous_through)
                .count();
            if changed < vector_delta_hnsw_min_changes() {
                return Ok(false);
            }
            let through = runtime
                .memtable
                .values()
                .map(|doc| doc.doc_version)
                .max()
                .unwrap_or(previous_through);
            let mut entries = runtime
                .memtable
                .values()
                .filter(|doc| {
                    !doc.deleted && runtime.is_current(&doc.id, doc.doc_version)
                })
                .map(|doc| VectorSegmentEntry::from(doc.as_ref()))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| {
                left.doc_version
                    .cmp(&right.doc_version)
                    .then_with(|| left.id.cmp(&right.id))
            });
            (
                meta,
                previous_through,
                through,
                Arc::new(VectorSegmentBlob { entries }),
            )
        };

        let delta_index = (!source.entries.is_empty())
            .then(|| Self::build_vector_hnsw_index(source.as_ref(), &meta))
            .transpose()?
            .map(Arc::new);
        let _guard = write_lock.blocking_lock();
        let (_, version, current_meta) = self.read_vector_meta(index)?;
        if version != expected_version || VectorRuntimeConfig::from(&current_meta) != VectorRuntimeConfig::from(&meta) {
            return Err(Error::msg("ERR vector index changed during write"));
        }
        let runtime = self
            .vector_runtimes
            .get(self.db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let published = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .publish_delta_index(previous_through, through, delta_index);
        if published {
            global_metrics().record_vector_delta_build(source.entries.len());
        }
        Ok(published)
    }

    fn compacted_segment_level(base: u64, count: usize) -> u32 {
        let mut level = 0u32;
        let mut level_size = base.max(1);
        while level_size < count as u64 {
            let Some(next) = level_size.checked_mul(VECTOR_LSM_MERGE_FACTOR as u64) else {
                break;
            };
            level_size = next;
            level = level.saturating_add(1);
        }
        level
    }

    fn checkpoint_vector_mutations(
        &self,
        index: &str,
        expected_version: u64,
        force: bool,
    ) -> Result<bool, Error> {
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (_, version, meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        if version != expected_version || meta.algorithm != VectorIndexAlgorithm::Hnsw {
            return Ok(false);
        }
        let checkpoint_key = vector_version_checkpoint_key(
            self.key_layout,
            self.db_index,
            index,
            version,
        );
        let previous_checkpoint = self
            .store
            .get_raw(&checkpoint_key)?
            .map(|raw| decode_record::<VectorVersionCheckpoint>(&raw))
            .transpose()?;
        let checkpoint_through = previous_checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.through_doc_version);
        let through_doc_version = meta.next_doc_version.saturating_sub(1);
        // Checkpointing rewrites the complete latest-version directory. Grow
        // the journal interval with the previous directory size so sequential
        // ingestion produces logarithmically many checkpoints instead of
        // rewriting an O(N) map every fixed 1,024 documents.
        let checkpoint_interval = vector_mutation_checkpoint_interval().max(
            previous_checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.current_versions.len() as u64),
        );
        if through_doc_version <= checkpoint_through
            || (!force
                && through_doc_version.saturating_sub(checkpoint_through)
                    < checkpoint_interval)
        {
            return Ok(false);
        }
        let runtime = self
            .vector_runtimes
            .get(self.db_index, index, version)
            .ok_or_else(|| Error::msg("ERR vector runtime is not initialized"))?;
        let mut current_versions = runtime
            .read()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .current_versions
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect::<Vec<_>>();
        current_versions.sort_by(|left, right| left.0.cmp(&right.0));

        let mutation_prefix = vector_version_mutation_prefix(
            self.key_layout,
            self.db_index,
            index,
            version,
        );
        let mutation_upper = super::prefix_exclusive_upper_bound(&mutation_prefix)
            .ok_or_else(|| Error::msg("ERR invalid vector mutation prefix"))?;
        let mut batch = WriteBatch::new();
        batch.delete_range(&mutation_prefix, &mutation_upper)?;
        batch.put(
            &checkpoint_key,
            &encode_record(&VectorVersionCheckpoint {
                through_doc_version,
                current_versions,
            })?,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;
        global_metrics().record_vector_mutation_checkpoint();
        Ok(true)
    }

    pub fn vector_compact(&self, index: &str) -> Result<(), Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (_, version, mut meta, expected_marker, expected_meta, expected_state) =
            self.read_vector_meta_observed(index)?;
        if meta.algorithm == VectorIndexAlgorithm::Flat {
            let doc_prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
            let mut live_count = 0u64;
            let mut tombstone_keys = Vec::new();
            for (key, raw) in self.store.scan_prefix_raw(&doc_prefix)? {
                if decode_record::<VectorDocRecord>(&raw)?.deleted {
                    tombstone_keys.push(key);
                } else {
                    live_count = live_count.saturating_add(1);
                }
            }
            let mut batch = WriteBatch::new();
            delete_vector_segments_to_batch(
                &mut batch,
                self.key_layout,
                self.db_index,
                index,
                version,
            )?;
            batch.delete(&vector_version_checkpoint_key(
                self.key_layout,
                self.db_index,
                index,
                version,
            ))?;
            for key in tombstone_keys {
                batch.delete(&key)?;
            }
            meta.doc_count = live_count;
            meta.next_segment_id = 1;
            meta.snapshot_doc_version = 0;
            batch.put(
                &vector_meta_key(self.key_layout, self.db_index, index, version),
                &encode_record(&meta)?,
            )?;
            batch.put(
                &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
                &encode_record(&VectorMutableState::from_meta(&meta))?,
            )?;
            self.commit_vector_batch_with_state_if_unchanged(VectorStateCommit {
                index,
                internal: meta.internal,
                version,
                expected_marker: &expected_marker,
                expected_meta: &expected_meta,
                expected_state,
                batch: &batch,
            })?;
            self.vector_runtimes.remove(self.db_index, index, version);
            return Ok(());
        }
        self.ensure_vector_runtime_unlocked(index, version, &meta)?;

        // Per-document records are the source of truth.  This is an explicit
        // compaction operation; ordinary recovery only reconstructs memory.
        let doc_prefix = vector_doc_prefix(self.key_layout, self.db_index, index, version);
        let mut live_docs = Vec::new();
        let mut tombstone_keys = Vec::new();
        let mut max_doc_version = 0u64;
        for (key, raw) in self.store.scan_prefix_raw(&doc_prefix)? {
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            max_doc_version = max_doc_version.max(doc.doc_version);
            if doc.deleted {
                tombstone_keys.push(key);
            } else {
                live_docs.push(doc);
            }
        }
        live_docs.sort_by(|left, right| {
            left.doc_version
                .cmp(&right.doc_version)
                .then_with(|| left.id.cmp(&right.id))
        });

        let old_segments = self
            .store
            .scan_prefix_raw(&vector_segment_prefix(
                self.key_layout,
                self.db_index,
                index,
                version,
            ))?
            .into_iter()
            .map(|(key, raw)| Ok((key, decode_record::<VectorSegmentMeta>(&raw)?)))
            .collect::<Result<Vec<_>, Error>>()?;
        let old_blob_keys = self
            .store
            .scan_prefix_raw(&vector_prefix(
                self.key_layout,
                self.db_index,
                &VECTOR_GRAPH_NAMESPACE,
                index,
                version,
            ))?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        let mut next_segment_id = old_segments
            .iter()
            .map(|(_, segment)| segment.segment_id.saturating_add(1))
            .max()
            .unwrap_or(1)
            .max(meta.next_segment_id)
            .max(1);

        let mut rebuilt_segments = Vec::new();
        for chunk in live_docs.chunks(meta.max_segment_docs.max(1) as usize) {
            let segment_id = next_segment_id;
            next_segment_id = next_segment_id.saturating_add(1);
            let source_key = vector_segment_source_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            let index_key = vector_segment_index_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                segment_id,
            );
            let source = Arc::new(VectorSegmentBlob {
                entries: chunk.iter().map(VectorSegmentEntry::from).collect(),
            });
            let persisted = Arc::new(Self::build_vector_hnsw_index(source.as_ref(), &meta)?);
            let segment = VectorSegmentMeta {
                segment_id,
                level: Self::compacted_segment_level(meta.segment_max_docs, chunk.len()),
                source_key: source_key.clone(),
                index_key: index_key.clone(),
                doc_count: chunk.len() as u64,
                min_doc_version: chunk.first().unwrap().doc_version,
                max_doc_version: chunk.last().unwrap().doc_version,
            };
            self.store
                .blob_put_raw(&source_key, &encode_record(source.as_ref())?)
                .map_err(|error| Error::msg(error.to_string()))?;
            self.store
                .blob_put_raw(&index_key, &encode_record(persisted.as_ref())?)
                .map_err(|error| Error::msg(error.to_string()))?;
            rebuilt_segments.push((segment, source, persisted));
        }

        let mut batch = WriteBatch::new();
        for key in old_segments
            .into_iter()
            .map(|(key, _)| key)
            .chain(old_blob_keys)
            .chain(tombstone_keys)
        {
            batch.delete(&key)?;
        }
        for (segment, _, _) in &rebuilt_segments {
            batch.put(
                &vector_segment_key(
                    self.key_layout,
                    self.db_index,
                    index,
                    version,
                    segment.segment_id,
                ),
                &encode_record(segment)?,
            )?;
        }
        meta.doc_count = live_docs.len() as u64;
        meta.next_segment_id = next_segment_id;
        meta.snapshot_doc_version = max_doc_version;
        batch.put(
            &vector_version_checkpoint_key(
                self.key_layout,
                self.db_index,
                index,
                version,
            ),
            &encode_record(&VectorVersionCheckpoint {
                through_doc_version: max_doc_version,
                current_versions: live_docs
                    .iter()
                    .map(|doc| (doc.id.clone(), doc.doc_version))
                    .collect(),
            })?,
        )?;
        let mutation_prefix = vector_version_mutation_prefix(
            self.key_layout,
            self.db_index,
            index,
            version,
        );
        let mutation_upper = super::prefix_exclusive_upper_bound(&mutation_prefix)
            .ok_or_else(|| Error::msg("ERR invalid vector mutation prefix"))?;
        batch.delete_range(&mutation_prefix, &mutation_upper)?;
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(&meta)?,
        )?;
        batch.put(
            &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
            &encode_record(&VectorMutableState::from_meta(&meta))?,
        )?;
        self.commit_vector_batch_with_state_if_unchanged(VectorStateCommit {
            index,
            internal: meta.internal,
            version,
            expected_marker: &expected_marker,
            expected_meta: &expected_meta,
            expected_state,
            batch: &batch,
        })?;

        let runtime_segments = rebuilt_segments
            .into_iter()
            .map(|(meta, _source, index)| VectorSegmentRuntime {
                meta,
                source: None,
                index: Some(index),
            })
            .collect::<Vec<_>>();
        let mut rebuilt = VectorRuntime::with_segments(
            VectorRuntimeConfig::from(&meta),
            meta.next_segment_id,
            runtime_segments,
        );
        rebuilt.reconcile_docs(live_docs, meta.snapshot_doc_version);
        self.vector_runtimes.insert_runtime(
            VectorRuntimeRegistry::key(self.db_index, index, version),
            Arc::new(RwLock::new(rebuilt)),
        );
        global_metrics().record_vector_compaction();
        Ok(())
    }

    pub async fn vector_compact_async(&self, index: &str) -> Result<(), Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_compact(&index))
            .await
    }

    pub(crate) fn vector_maintenance_tick(&self) -> Result<(), Error> {
        const INDEX_BUDGET: usize = 4;
        let indexes = self
            .vector_runtimes
            .take_dirty_indexes_for_db(self.db_index, INDEX_BUDGET);
        let mut first_error = None;
        for (index, expected_version) in indexes {
            let result = (|| -> Result<(), Error> {
                // Advance an already-published source before flushing the
                // current memtable.  This keeps source/index publication as
                // two observable crash-safe stages.  HNSW construction and
                // merge rebuilding run outside the collection write lock.
                let built = self.build_one_vector_segment_index(&index, expected_version)?;
                let flushed = self.flush_vector_memtable(&index, expected_version, false)?;
                let merged = self.merge_one_vector_segment_group(&index, expected_version)?;
                let _delta_built = self.build_vector_delta_index(&index, expected_version)?;
                let _checkpointed =
                    self.checkpoint_vector_mutations(&index, expected_version, false)?;
                if built || flushed || merged {
                    self.vector_runtimes
                        .mark_dirty(self.db_index, &index, expected_version);
                }
                Ok(())
            })();
            if let Err(err) = result {
                if matches!(
                    err.to_string().as_str(),
                    "ERR vector index changed during write" | "ERR vector index does not exist"
                ) {
                    self.vector_runtimes
                        .remove(self.db_index, &index, expected_version);
                    continue;
                }
                self.vector_runtimes
                    .mark_dirty(self.db_index, &index, expected_version);
                if first_error.is_none() {
                    first_error = Some(Error::msg(format!("index {index}: {err}")));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn vector_maintenance_tick_async(&self) -> Result<(), Error> {
        self.run_blocking_store_task(|db| db.vector_maintenance_tick())
            .await
    }
}
