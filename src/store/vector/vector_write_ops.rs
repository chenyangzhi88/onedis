impl Db {
    pub fn vector_create(&self, index: &str, options: VectorCreateOptions) -> Result<(), Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let internal = is_internal_fulltext_vector_index(index);
        let raw_key = if internal {
            vector_internal_marker_key(self.key_layout, self.db_index, index)
        } else {
            self.mk(index)
        };
        if let Some(raw) = self.store.get_raw(&raw_key) {
            let header =
                decode_meta_header(&raw).ok_or_else(|| Error::msg("Type parsing error"))?;
            if header.type_tag == TYPE_VECTOR {
                return Err(Error::msg("ERR vector index already exists"));
            }
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }

        let distance = parse_distance(&options.distance)?;
        if options.dim == 0 || options.dim > MAX_VECTOR_DIMENSIONS {
            return Err(Error::msg("ERR invalid vector dimension"));
        }
        if let Some(source_dim) = options.source_dim {
            validate_vector_projection(source_dim, options.dim)?;
        }
        validate_schema(&options.schema)?;
        let segment_max_docs = options
            .segment_max_docs
            .unwrap_or_else(vector_segment_max_docs);
        if segment_max_docs == 0 || segment_max_docs > MAX_VECTOR_INITIAL_CAP as u64 {
            return Err(Error::msg("ERR invalid vector segment size"));
        }
        let m = normalize_hnsw_m(options.m)?;
        let requested_ef_construction = options
            .ef_construction
            .unwrap_or(DEFAULT_HNSW_EF_CONSTRUCTION as usize);
        if requested_ef_construction == 0 || requested_ef_construction > MAX_VECTOR_HNSW_EF {
            return Err(Error::msg("ERR invalid vector EF_CONSTRUCTION"));
        }
        let ef_construction = requested_ef_construction.max(m);
        let ef_runtime = options
            .ef_runtime
            .unwrap_or(DEFAULT_HNSW_EF_RUNTIME as usize);
        if ef_runtime == 0 || ef_runtime > MAX_VECTOR_HNSW_EF {
            return Err(Error::msg("ERR invalid vector EF_RUNTIME"));
        }
        let initial_cap = options.initial_cap.unwrap_or(segment_max_docs as usize);
        if initial_cap == 0 || initial_cap > MAX_VECTOR_INITIAL_CAP {
            return Err(Error::msg("ERR invalid vector INITIAL_CAP"));
        }

        let version = self.next_version();
        let meta = VectorIndexMeta {
            dim: options.dim as u32,
            projection: options.source_dim.map(|input_dim| VectorProjection {
                input_dim: input_dim as u32,
                seed: vector_projection_seed(version),
            }),
            distance,
            schema: options.schema,
            m: m as u32,
            ef_construction: ef_construction as u32,
            ef_runtime: ef_runtime as u32,
            initial_cap: initial_cap as u64,
            next_doc_version: 1,
            doc_count: 0,
            next_segment_id: 1,
            snapshot_doc_version: 0,
            segment_max_docs,
            max_segment_docs: vector_lsm_max_segment_docs(segment_max_docs),
            quantization: options.quantization,
            internal,
        };
        let marker = Structure::VectorCollection(Vector {
            dimension: options.dim,
            vectors: Default::default(),
            norms: Default::default(),
        });
        let mut batch = WriteBatch::new();
        batch.put(&raw_key, &encode_entry(&marker, 0, version))?;
        if internal {
            super::version_compaction::put_version_owner_to_batch(
                &mut batch,
                self.db_index,
                index.as_bytes(),
                version,
                TYPE_VECTOR,
            )?;
        }
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(&meta)?,
        )?;
        if !self
            .compare_and_write_batch_if_not_empty(&[CompareCondition::absent(&raw_key)], &batch)?
        {
            return Err(Error::msg("ERR vector index already exists"));
        }
        if internal {
            self.store.register_live_version(version);
        }
        self.vector_runtimes.reset(
            self.db_index,
            index,
            version,
            VectorRuntimeConfig::from(&meta),
        );
        Ok(())
    }

    pub async fn vector_create_async(
        &self,
        index: &str,
        options: VectorCreateOptions,
    ) -> Result<(), Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        self.run_blocking_store_task(move |db| db.vector_create(&index, options))
            .await
    }

    pub fn vector_add(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
    ) -> Result<bool, Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta, expected_marker, expected_meta) =
            self.read_vector_meta_observed(index)?;
        if expire_ms > 0 && super::now_ms() >= expire_ms {
            return Err(Error::msg("ERR vector index does not exist"));
        }
        self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        let vector = match meta.projection {
            Some(projection) => project_vector(&vector, projection, meta.dim as usize)?,
            None => {
                validate_vector(&vector, meta.dim as usize)?;
                vector
            }
        };
        validate_vector_for_distance(&vector, meta.distance)?;
        let old_doc = self
            .store
            .get_raw(&vector_doc_key(
                self.key_layout,
                self.db_index,
                index,
                version,
                id,
            ))
            .map(|raw| decode_record::<VectorDocRecord>(&raw))
            .transpose()?
            .filter(|doc| !doc.deleted);
        // Omitting SETATTR updates only the embedding.  Attribute removal is
        // explicit through VSETATTR key element "" or a new empty object.
        let attrs_json = attrs_json.unwrap_or_else(|| {
            old_doc
                .as_ref()
                .map(|doc| doc.attrs_json.clone())
                .unwrap_or_else(|| "{}".to_string())
        });
        let attrs = parse_attrs(&attrs_json)?;
        validate_attrs_against_schema(&meta.schema, &attrs)?;
        let doc_version = meta.next_doc_version;
        meta.next_doc_version = meta.next_doc_version.saturating_add(1);
        let added = old_doc.is_none();
        if added {
            meta.doc_count = meta.doc_count.saturating_add(1);
        }

        let doc = VectorDocRecord {
            id: id.to_string(),
            doc_version,
            vector,
            attrs_json: attrs_json.clone(),
            deleted: false,
        };

        let mut batch = WriteBatch::new();
        put_vector_marker_to_batch(
            &mut batch,
            self.key_layout,
            self.db_index,
            index,
            expire_ms,
            version,
            meta.dim,
            meta.internal,
        )?;
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(&meta)?,
        )?;
        batch.put(
            &vector_doc_key(self.key_layout, self.db_index, index, version, id),
            &encode_record(&doc)?,
        )?;
        if let Some(old_doc) = old_doc {
            let old_attrs = parse_attrs(&old_doc.attrs_json)?;
            delete_attr_index_entries_to_batch(
                &mut batch,
                &VectorAttrIndexContext {
                    layout: self.key_layout,
                    db_index: self.db_index,
                    index,
                    version,
                    schema: &meta.schema,
                    doc_id: id,
                },
                &old_attrs,
            )?;
        }
        put_attr_index_entries_to_batch(
            &mut batch,
            &VectorAttrIndexContext {
                layout: self.key_layout,
                db_index: self.db_index,
                index,
                version,
                schema: &meta.schema,
                doc_id: id,
            },
            doc_version,
            &attrs,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            meta.internal,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;
        self.vector_runtimes.upsert(
            self.db_index,
            index,
            version,
            VectorRuntimeConfig::from(&meta),
            VectorRuntimeEntry::from(&doc),
        )?;
        Ok(added)
    }

    pub async fn vector_add_async(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
    ) -> Result<bool, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        let id = id.to_string();
        self.run_blocking_store_task(move |db| db.vector_add(&index, &id, vector, attrs_json))
            .await
    }

    pub fn vector_add_autocreate(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
        m: Option<usize>,
        ef_construction: Option<usize>,
        quantization: Option<VectorQuantization>,
        reduce_dim: Option<usize>,
    ) -> Result<bool, Error> {
        if (quantization.is_some() || reduce_dim.is_some())
            && let Ok((_, _, meta)) = self.read_vector_meta(index)
        {
            if quantization.is_some_and(|requested| requested != meta.quantization) {
                return Err(Error::msg(
                    "ERR vector quantization mode does not match existing index",
                ));
            }
            if reduce_dim.is_some_and(|requested| {
                meta.projection.is_none_or(|projection| {
                    requested != meta.dim as usize || projection.input_dim as usize != vector.len()
                })
            }) {
                return Err(Error::msg(
                    "ERR vector REDUCE mode does not match existing index",
                ));
            }
        }
        match self.vector_add(index, id, vector.clone(), attrs_json.clone()) {
            Ok(added) => return Ok(added),
            Err(err) if err.to_string() == "ERR vector index does not exist" => {}
            Err(err) => return Err(err),
        }
        if let Err(err) = self.vector_create(
            index,
            VectorCreateOptions {
                dim: reduce_dim.unwrap_or(vector.len()),
                source_dim: reduce_dim.map(|_| vector.len()),
                distance: "COSINE".to_string(),
                schema: Vec::new(),
                segment_max_docs: None,
                m,
                ef_construction,
                ef_runtime: None,
                initial_cap: None,
                quantization: quantization.unwrap_or(VectorQuantization::Q8),
            },
        ) && err.to_string() != "ERR vector index already exists"
        {
            return Err(err);
        }
        self.vector_add(index, id, vector, attrs_json)
    }

    pub async fn vector_add_autocreate_async(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
        m: Option<usize>,
        ef_construction: Option<usize>,
        quantization: Option<VectorQuantization>,
        reduce_dim: Option<usize>,
    ) -> Result<bool, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        let id = id.to_string();
        self.run_blocking_store_task(move |db| {
            db.vector_add_autocreate(
                &index,
                &id,
                vector,
                attrs_json,
                m,
                ef_construction,
                quantization,
                reduce_dim,
            )
        })
        .await
    }

    pub fn vector_del(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock
            .lock()
            .map_err(|_| Error::msg("ERR vector write lock poisoned"))?;
        let (expire_ms, version, mut meta, expected_marker, expected_meta) =
            match self.read_vector_meta_observed(index) {
                Ok(value) => value,
                Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(0),
                Err(err) => return Err(err),
            };
        self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        let mut seen_ids = HashSet::new();
        let ids = ids
            .iter()
            .filter(|id| seen_ids.insert(id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let keys = ids
            .iter()
            .map(|id| vector_doc_key(self.key_layout, self.db_index, index, version, id))
            .collect::<Vec<_>>();
        let current_docs = ids
            .into_iter()
            .zip(self.store.multi_get_raw(&keys))
            .filter_map(|(id, raw)| raw.map(|raw| (id, raw)))
            .map(|(id, raw)| Ok((id, decode_record::<VectorDocRecord>(&raw)?)))
            .filter_map(|result: Result<_, Error>| match result {
                Ok((_, doc)) if doc.deleted => None,
                other => Some(other),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        let mut deleted_docs = Vec::new();
        for (id, mut doc) in current_docs {
            let key = vector_doc_key(self.key_layout, self.db_index, index, version, &id);
            let attrs = parse_attrs(&doc.attrs_json)?;
            delete_attr_index_entries_to_batch(
                &mut batch,
                &VectorAttrIndexContext {
                    layout: self.key_layout,
                    db_index: self.db_index,
                    index,
                    version,
                    schema: &meta.schema,
                    doc_id: &doc.id,
                },
                &attrs,
            )?;
            doc.doc_version = meta.next_doc_version;
            meta.next_doc_version = meta.next_doc_version.saturating_add(1);
            doc.deleted = true;
            batch.put(&key, &encode_record(&doc)?)?;
            deleted_docs.push(doc.clone());
            deleted += 1;
        }
        if deleted > 0 {
            meta.doc_count = meta.doc_count.saturating_sub(deleted as u64);
            put_vector_marker_to_batch(
                &mut batch,
                self.key_layout,
                self.db_index,
                index,
                expire_ms,
                version,
                meta.dim,
                meta.internal,
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
            for doc in deleted_docs {
                self.vector_runtimes
                    .mark_deleted(self.db_index, index, version, doc);
            }
        }
        Ok(deleted)
    }

    pub async fn vector_del_async(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let index = index.to_string();
        let ids = ids.to_vec();
        self.run_blocking_store_task(move |db| db.vector_del(&index, &ids))
            .await
    }
}
