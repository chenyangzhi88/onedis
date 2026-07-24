impl Db {
    pub fn vector_drop(&self, index: &str) -> Result<usize, Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (_, version, _) = self.read_vector_meta(index)?;
        let mut batch = WriteBatch::new();
        batch.delete(&main_key(self.db_index, index));
        delete_vector_namespace_to_batch(&self.store, &mut batch, self.db_index, index, version);
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.remove(self.db_index, index, version);
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
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta) = self.read_vector_meta(index)?;
        let mut batch = WriteBatch::new();
        delete_vector_segments_to_batch(&self.store, &mut batch, self.db_index, index, version);
        meta.next_segment_id = 1;
        meta.snapshot_doc_version = 0;
        put_vector_marker_to_batch(
            &mut batch,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
        );
        batch.put(
            &vector_meta_key(self.db_index, index, version),
            &encode_record(&meta)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.vector_runtimes.reset(
            self.db_index,
            index,
            version,
            VectorRuntimeConfig::from(&meta),
        );
        let prefix = vector_doc_prefix(self.db_index, index, version);
        for (_, raw) in self.store.scan_prefix_raw(&prefix) {
            let doc = decode_record::<VectorDocRecord>(&raw)?;
            if !doc.deleted {
                self.vector_runtimes.upsert(
                    self.db_index,
                    index,
                    version,
                    VectorRuntimeConfig::from(&meta),
                    VectorRuntimeEntry::from(&doc),
                )?;
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

    pub fn vector_compact(&self, index: &str) -> Result<(), Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (_, version, meta) = self.read_vector_meta(index)?;
        self.ensure_vector_runtime(index, version, &meta)?;
        self.gc_obsolete_vector_segments(index, version)
    }

    pub async fn vector_compact_async(&self, index: &str) -> Result<(), Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_compact(&index))
            .await
    }

    fn maybe_freeze_vector_segment(
        &self,
        index: &str,
        version: u64,
        meta: &mut VectorIndexMeta,
    ) -> Result<(), Error> {
        let max_docs = meta.segment_max_docs.max(1);
        let latest_doc_version = meta.next_doc_version.saturating_sub(1);
        if latest_doc_version <= meta.snapshot_doc_version {
            return Ok(());
        }
        if meta.doc_count < max_docs && latest_doc_version - meta.snapshot_doc_version < max_docs {
            return Ok(());
        }
        let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version) else {
            return Ok(());
        };
        let Some((mut segment, snapshot)) = runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .freeze_active()
        else {
            return Ok(());
        };
        let segment_id = segment.segment_id;
        let graph_key = vector_graph_key(self.db_index, index, version, segment_id);
        segment.graph_key = graph_key.clone();
        runtime
            .write()
            .map_err(|_| Error::msg("ERR vector runtime lock poisoned"))?
            .set_segment_graph_key(segment_id, graph_key);
        meta.next_segment_id = meta.next_segment_id.max(segment_id.saturating_add(1));
        meta.snapshot_doc_version = meta.snapshot_doc_version.max(segment.max_doc_version);
        persist_vector_segment_snapshot(
            &self.store,
            self.db_index,
            index,
            version,
            &segment,
            &encode_record(&snapshot)?,
        )?;
        self.gc_obsolete_vector_segments(index, version)?;
        Ok(())
    }

    fn gc_obsolete_vector_segments(&self, index: &str, version: u64) -> Result<(), Error> {
        let segments = self.vector_runtime_segment_entries(index, version)?;
        let mut obsolete_ids = HashSet::new();
        let mut obsolete_graph_keys = Vec::new();
        for (segment_id, graph_key, entries) in segments {
            let mut has_live_doc = false;
            for (id, doc_version) in entries {
                let Some(raw) =
                    self.store
                        .get_raw(&vector_doc_key(self.db_index, index, version, &id))
                else {
                    continue;
                };
                let doc = decode_record::<VectorDocRecord>(&raw)?;
                if !doc.deleted && doc.doc_version == doc_version {
                    has_live_doc = true;
                    break;
                }
            }
            if !has_live_doc {
                obsolete_ids.insert(segment_id);
                obsolete_graph_keys.push(graph_key);
            }
        }
        if obsolete_ids.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for segment_id in &obsolete_ids {
            batch.delete(&vector_segment_key(
                self.db_index,
                index,
                version,
                *segment_id,
            ));
        }
        for graph_key in obsolete_graph_keys {
            if !graph_key.is_empty() {
                batch.delete(&graph_key);
            }
        }
        self.write_batch_if_not_empty(&batch);
        if let Some(runtime) = self.vector_runtimes.get(self.db_index, index, version)
            && let Ok(mut runtime) = runtime.write()
        {
            runtime.remove_segments(&obsolete_ids);
        }
        Ok(())
    }
}
