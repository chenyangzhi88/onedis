impl Db {
    pub fn vector_create(&self, index: &str, options: VectorCreateOptions) -> Result<(), Error> {
        self.vector_create_with_algorithm(index, options, VectorIndexAlgorithm::Hnsw)
    }

    pub(in crate::store::db) fn vector_create_internal(
        &self,
        index: &str,
        options: VectorCreateOptions,
        flat: bool,
    ) -> Result<(), Error> {
        if !is_internal_fulltext_vector_index(index) {
            return Err(Error::msg("ERR invalid internal vector index name"));
        }
        self.vector_create_with_algorithm(
            index,
            options,
            if flat {
                VectorIndexAlgorithm::Flat
            } else {
                VectorIndexAlgorithm::Hnsw
            },
        )
    }

    fn vector_create_with_algorithm(
        &self,
        index: &str,
        options: VectorCreateOptions,
        algorithm: VectorIndexAlgorithm,
    ) -> Result<(), Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let internal = is_internal_fulltext_vector_index(index);
        let raw_key = if internal {
            vector_internal_marker_key(self.key_layout, self.db_index, index)
        } else {
            self.mk(index)
        };
        if let Some(raw) = self.store.get_raw(&raw_key)? {
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
            algorithm,
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
        batch.put(
            &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
            &encode_record(&VectorMutableState::from_meta(&meta))?,
        )?;
        if !self
            .compare_and_write_batch_if_not_empty(&[CompareCondition::absent(&raw_key)], &batch)?
        {
            return Err(Error::msg("ERR vector index already exists"));
        }
        if internal {
            self.store.register_live_version(version);
        }
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.vector_runtimes.reset(
                self.db_index,
                index,
                version,
                VectorRuntimeConfig::from(&meta),
            );
        }
        Ok(())
    }

    pub(in crate::store::db) fn vector_set_internal_algorithm(
        &self,
        index: &str,
        flat: bool,
    ) -> Result<(), Error> {
        let desired = if flat {
            VectorIndexAlgorithm::Flat
        } else {
            VectorIndexAlgorithm::Hnsw
        };
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (_, version, mut meta, expected_marker, expected_meta, _expected_state) =
            self.read_vector_meta_observed(index)?;
        if !meta.internal {
            return Err(Error::msg("ERR vector index is not internal"));
        }
        if meta.algorithm == desired {
            return Ok(());
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
        meta.algorithm = desired;
        meta.next_segment_id = 1;
        meta.snapshot_doc_version = 0;
        batch.put(
            &vector_meta_key(self.key_layout, self.db_index, index, version),
            &encode_record(&meta)?,
        )?;
        self.commit_vector_batch_if_marker_unchanged(
            index,
            true,
            version,
            &expected_marker,
            &expected_meta,
            &batch,
        )?;
        self.vector_runtimes.remove(self.db_index, index, version);
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
        let _guard = write_lock.blocking_lock();
        let (
            expire_ms,
            version,
            mut meta,
            expected_marker,
            expected_meta,
            expected_state,
        ) =
            self.read_vector_meta_observed(index)?;
        if expire_ms > 0 && super::now_ms() >= expire_ms {
            return Err(Error::msg("ERR vector index does not exist"));
        }
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        }
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
            ))?
            .map(|raw| decode_record::<VectorDocRecord>(&raw))
            .transpose()?
            .filter(|doc| !doc.deleted);
        // Omitting SETATTR updates only the embedding.  Attribute removal is
        // explicit through VSETATTR key element "" or a new empty object.
        let attrs_were_supplied = attrs_json.is_some();
        let update_attrs = attrs_were_supplied || old_doc.is_none();
        let attrs_json = attrs_json.unwrap_or_else(|| {
            old_doc
                .as_ref()
                .map(|doc| doc.attrs_json.clone())
                .unwrap_or_else(|| "{}".to_string())
        });
        let attrs = if update_attrs {
            Some(parse_attrs(&attrs_json)?)
        } else {
            None
        };
        if let Some(attrs) = attrs.as_ref() {
            validate_attrs_against_schema(&meta.schema, attrs)?;
        }
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
        batch.put(
            &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
            &encode_record(&VectorMutableState::from_meta(&meta))?,
        )?;
        batch.put(
            &vector_doc_key(self.key_layout, self.db_index, index, version, id),
            &encode_record(&doc)?,
        )?;
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            batch.put(
                &vector_version_mutation_key(
                    self.key_layout,
                    self.db_index,
                    index,
                    version,
                    doc_version,
                ),
                &encode_record(&VectorVersionMutation {
                    id: id.to_string(),
                    doc_version,
                    deleted: false,
                })?,
            )?;
        }
        if update_attrs
            && !meta.schema.is_empty()
            && let Some(old_doc) = old_doc.as_ref()
        {
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
        if update_attrs && !meta.schema.is_empty() {
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
                attrs.as_ref().expect("schema requires parsed attributes"),
            )?;
        }
        self.commit_vector_batch_with_state_if_unchanged(VectorStateCommit {
            index,
            internal: meta.internal,
            version,
            expected_marker: &expected_marker,
            expected_meta: &expected_meta,
            expected_state,
            batch: &batch,
        })?;
        self.record_public_vector_mutation(index, meta.internal);
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.vector_runtimes.upsert(
                self.db_index,
                index,
                version,
                VectorRuntimeConfig::from(&meta),
                VectorRuntimeEntry::from(&doc),
            )?;
        }
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
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let lock_started = Instant::now();
        let _vector_write_guard = write_lock.lock().await;
        let lock_wait_us = elapsed_us(lock_started);
        global_metrics().record_vector_write();
        let mut point_reads = 0usize;
        let internal = is_internal_fulltext_vector_index(index);
        if !internal {
            self.expire_if_needed_async(index).await?;
        }
        let marker_key = self.vector_marker_key(index, internal);
        let Some(expected_marker) = self.store.get_raw_async(&marker_key).await? else {
            return Err(Error::msg("ERR vector index does not exist"));
        };
        point_reads += 1;
        let header = decode_meta_header(&expected_marker)
            .ok_or_else(|| Error::msg("Type parsing error"))?;
        if header.type_tag != TYPE_VECTOR {
            return Err(Error::msg(WRONG_TYPE_ERROR));
        }
        if header.expire_ms > 0 && super::now_ms() >= header.expire_ms {
            return Err(Error::msg("ERR vector index does not exist"));
        }
        let version = header.version;
        let state_key =
            vector_mutable_state_key(self.key_layout, self.db_index, index, version);
        let mut expected_state = self.store.get_raw_async(&state_key).await?;
        point_reads += 1;
        let mut state = expected_state
            .as_deref()
            .map(decode_record::<VectorMutableState>)
            .transpose()?;
        let mut config = self.vector_runtimes.config(self.db_index, index, version)?;
        if config.is_none() || state.is_none() {
            // Legacy collections do not have a mutable-state record, and a
            // restarted process has no cached immutable configuration. Pay
            // the full metadata/recovery cost once, then keep the hot path to
            // marker + state + document point reads.
            let (_, observed_version, meta, _, _, observed_state) =
                self.read_vector_meta_observed_async(index).await?;
            point_reads += 3;
            if observed_version != version {
                return Err(Error::msg("ERR vector index changed during write"));
            }
            if meta.algorithm == VectorIndexAlgorithm::Hnsw {
                self.ensure_vector_runtime_unlocked(index, version, &meta)?;
            }
            state = Some(VectorMutableState::from_meta(&meta));
            expected_state = observed_state;
            config = Some(VectorRuntimeConfig::from(&meta));
        }
        let config = config.ok_or_else(|| Error::msg("ERR vector runtime config missing"))?;
        let mut state = state.ok_or_else(|| Error::msg("ERR vector mutable state missing"))?;
        if config.internal != internal {
            return Err(Error::msg("ERR invalid vector index ownership"));
        }
        let vector = match config.projection {
            Some(projection) => project_vector(&vector, projection, config.dim)?,
            None => {
                validate_vector(&vector, config.dim)?;
                vector
            }
        };
        validate_vector_for_distance(&vector, config.distance)?;
        let doc_key = vector_doc_key(self.key_layout, self.db_index, index, version, id);
        let old_doc = self
            .store
            .get_raw_async(&doc_key)
            .await?
            .map(|raw| decode_record::<VectorDocRecord>(&raw))
            .transpose()?
            .filter(|doc| !doc.deleted);
        point_reads += 1;
        let attrs_were_supplied = attrs_json.is_some();
        let update_attrs = attrs_were_supplied || old_doc.is_none();
        let attrs_json = attrs_json.unwrap_or_else(|| {
            old_doc
                .as_ref()
                .map(|doc| doc.attrs_json.clone())
                .unwrap_or_else(|| "{}".to_string())
        });
        let attrs = if update_attrs {
            Some(parse_attrs(&attrs_json)?)
        } else {
            None
        };
        if let Some(attrs) = attrs.as_ref() {
            validate_attrs_against_schema(config.schema.as_ref(), attrs)?;
        }
        let doc_version = state.next_doc_version;
        state.next_doc_version = state.next_doc_version.saturating_add(1);
        let added = old_doc.is_none();
        if added {
            state.doc_count = state.doc_count.saturating_add(1);
        }
        let doc = VectorDocRecord {
            id: id.to_string(),
            doc_version,
            vector,
            attrs_json: attrs_json.clone(),
            deleted: false,
        };
        let mut batch = WriteBatch::new();
        batch.put(&state_key, &encode_record(&state)?)?;
        batch.put(&doc_key, &encode_record(&doc)?)?;
        if config.algorithm == VectorIndexAlgorithm::Hnsw {
            batch.put(
                &vector_version_mutation_key(
                    self.key_layout,
                    self.db_index,
                    index,
                    version,
                    doc_version,
                ),
                &encode_record(&VectorVersionMutation {
                    id: id.to_string(),
                    doc_version,
                    deleted: false,
                })?,
            )?;
            global_metrics().record_vector_write_mutation_records(1);
        }
        if update_attrs
            && !config.schema.is_empty()
            && let Some(old_doc) = old_doc.as_ref()
        {
            let old_attrs = parse_attrs(&old_doc.attrs_json)?;
            delete_attr_index_entries_to_batch(
                &mut batch,
                &VectorAttrIndexContext {
                    layout: self.key_layout,
                    db_index: self.db_index,
                    index,
                    version,
                    schema: config.schema.as_ref(),
                    doc_id: id,
                },
                &old_attrs,
            )?;
        }
        if update_attrs && !config.schema.is_empty() {
            put_attr_index_entries_to_batch(
                &mut batch,
                &VectorAttrIndexContext {
                    layout: self.key_layout,
                    db_index: self.db_index,
                    index,
                    version,
                    schema: config.schema.as_ref(),
                    doc_id: id,
                },
                doc_version,
                attrs.as_ref().expect("schema requires parsed attributes"),
            )?;
        }
        let batch_bytes = batch.iter().fold(0usize, |bytes, (_, key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        });
        global_metrics().record_vector_write_work(
            point_reads,
            2,
            batch.count() as usize,
            batch_bytes,
            lock_wait_us,
        );
        self.commit_vector_state_batch_if_unchanged_async(
            index,
            config.internal,
            version,
            &expected_marker,
            expected_state,
            &batch,
        )
        .await?;
        self.record_public_vector_mutation(index, config.internal);
        if config.algorithm == VectorIndexAlgorithm::Hnsw {
            self.vector_runtimes.upsert(
                self.db_index,
                index,
                version,
                config,
                VectorRuntimeEntry::from(&doc),
            )?;
        }
        Ok(added)
    }

    pub fn vector_add_autocreate(
        &self,
        index: &str,
        id: &str,
        vector: Vec<f32>,
        attrs_json: Option<String>,
        options: VectorAutocreateOptions,
    ) -> Result<bool, Error> {
        let VectorAutocreateOptions {
            m,
            ef_construction,
            quantization,
            reduce_dim,
        } = options;
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
        options: VectorAutocreateOptions,
    ) -> Result<bool, Error> {
        let VectorAutocreateOptions {
            m,
            ef_construction,
            quantization,
            reduce_dim,
        } = options;
        match self.read_vector_meta_async(index).await {
            Ok((_, _, meta)) => {
                if quantization.is_some_and(|requested| requested != meta.quantization) {
                    return Err(Error::msg(
                        "ERR vector quantization mode does not match existing index",
                    ));
                }
                if reduce_dim.is_some_and(|requested| {
                    meta.projection.is_none_or(|projection| {
                        requested != meta.dim as usize
                            || projection.input_dim as usize != vector.len()
                    })
                }) {
                    return Err(Error::msg(
                        "ERR vector REDUCE mode does not match existing index",
                    ));
                }
                return self.vector_add_async(index, id, vector, attrs_json).await;
            }
            Err(err) if err.to_string() == "ERR vector index does not exist" => {}
            Err(err) => return Err(err),
        }
        if let Err(err) = self
            .vector_create_async(
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
            )
            .await
            && err.to_string() != "ERR vector index already exists"
        {
            return Err(err);
        }
        self.vector_add_async(index, id, vector, attrs_json).await
    }

    pub(in crate::store::db) fn vector_apply_internal_batch(
        &self,
        index: &str,
        mutations: Vec<(String, Option<Vec<f32>>)>,
    ) -> Result<(), Error> {
        if mutations.is_empty() {
            return Ok(());
        }
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (
            _expire_ms,
            version,
            mut meta,
            expected_marker,
            expected_meta,
            expected_state,
        ) =
            self.read_vector_meta_observed(index)?;
        if !meta.internal || !meta.schema.is_empty() {
            return Err(Error::msg("ERR invalid internal vector index"));
        }
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        }

        let mut latest = HashMap::<String, Option<Vec<f32>>>::new();
        for (id, vector) in mutations {
            latest.insert(id, vector);
        }
        let mut mutations = latest.into_iter().collect::<Vec<_>>();
        mutations.sort_by(|left, right| left.0.cmp(&right.0));
        let keys = mutations
            .iter()
            .map(|(id, _)| vector_doc_key(self.key_layout, self.db_index, index, version, id))
            .collect::<Vec<_>>();
        let old_docs = self.store.multi_get_raw(&keys)?;
        let mut batch = WriteBatch::new();
        let mut changed_docs = Vec::new();
        for (((id, vector), key), old_raw) in mutations
            .into_iter()
            .zip(keys)
            .zip(old_docs)
        {
            let old_doc = old_raw
                .map(|raw| decode_record::<VectorDocRecord>(&raw))
                .transpose()?;
            let doc_version = meta.next_doc_version;
            let mut doc = match vector {
                Some(vector) => {
                    let vector = match meta.projection {
                        Some(projection) => {
                            project_vector(&vector, projection, meta.dim as usize)?
                        }
                        None => {
                            validate_vector(&vector, meta.dim as usize)?;
                            vector
                        }
                    };
                    validate_vector_for_distance(&vector, meta.distance)?;
                    if old_doc.as_ref().is_none_or(|doc| doc.deleted) {
                        meta.doc_count = meta.doc_count.saturating_add(1);
                    }
                    VectorDocRecord {
                        id: id.clone(),
                        doc_version,
                        vector,
                        attrs_json: old_doc
                            .as_ref()
                            .map(|doc| doc.attrs_json.clone())
                            .unwrap_or_else(|| "{}".to_string()),
                        deleted: false,
                    }
                }
                None => {
                    let Some(mut doc) = old_doc.filter(|doc| !doc.deleted) else {
                        continue;
                    };
                    meta.doc_count = meta.doc_count.saturating_sub(1);
                    doc.doc_version = doc_version;
                    doc.deleted = true;
                    doc
                }
            };
            doc.doc_version = doc_version;
            meta.next_doc_version = meta.next_doc_version.saturating_add(1);
            batch.put(&key, &encode_record(&doc)?)?;
            if meta.algorithm == VectorIndexAlgorithm::Hnsw {
                batch.put(
                    &vector_version_mutation_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        doc_version,
                    ),
                    &encode_record(&VectorVersionMutation {
                        id,
                        doc_version,
                        deleted: doc.deleted,
                    })?,
                )?;
            }
            changed_docs.push(doc);
        }
        if changed_docs.is_empty() {
            return Ok(());
        }
        batch.put(
            &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
            &encode_record(&VectorMutableState::from_meta(&meta))?,
        )?;
        self.commit_vector_batch_with_state_if_unchanged(VectorStateCommit {
            index,
            internal: true,
            version,
            expected_marker: &expected_marker,
            expected_meta: &expected_meta,
            expected_state,
            batch: &batch,
        })?;
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.vector_runtimes.apply_docs(
                self.db_index,
                index,
                version,
                changed_docs,
            )?;
        }
        Ok(())
    }

    pub fn vector_del(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        global_metrics().record_vector_write();
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let _guard = write_lock.blocking_lock();
        let (
            _expire_ms,
            version,
            mut meta,
            expected_marker,
            expected_meta,
            expected_state,
        ) =
            match self.read_vector_meta_observed(index) {
                Ok(value) => value,
                Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(0),
                Err(err) => return Err(err),
            };
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        }
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
            .zip(self.store.multi_get_raw(&keys)?)
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
            if !meta.schema.is_empty() {
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
            }
            doc.doc_version = meta.next_doc_version;
            meta.next_doc_version = meta.next_doc_version.saturating_add(1);
            doc.deleted = true;
            batch.put(&key, &encode_record(&doc)?)?;
            if meta.algorithm == VectorIndexAlgorithm::Hnsw {
                batch.put(
                    &vector_version_mutation_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        doc.doc_version,
                    ),
                    &encode_record(&VectorVersionMutation {
                        id: doc.id.clone(),
                        doc_version: doc.doc_version,
                        deleted: true,
                    })?,
                )?;
            }
            deleted_docs.push(doc.clone());
            deleted += 1;
        }
        if deleted > 0 {
            meta.doc_count = meta.doc_count.saturating_sub(deleted as u64);
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
            self.record_public_vector_mutation(index, meta.internal);
            if meta.algorithm == VectorIndexAlgorithm::Hnsw {
                for doc in deleted_docs {
                    self.vector_runtimes
                        .mark_deleted(self.db_index, index, version, doc);
                }
            }
        }
        Ok(deleted)
    }

    pub async fn vector_del_async(&self, index: &str, ids: &[String]) -> Result<usize, Error> {
        let _key_write_guard = self.set_write_lock(index).lock().await;
        let write_lock = self.vector_runtimes.write_lock(self.db_index, index);
        let lock_started = Instant::now();
        let _vector_write_guard = write_lock.lock().await;
        let lock_wait_us = elapsed_us(lock_started);
        global_metrics().record_vector_write();
        let (_expire_ms, version, mut meta, expected_marker, _expected_meta, expected_state) =
            match self.read_vector_meta_observed_async(index).await {
                Ok(value) => value,
                Err(err) if err.to_string() == "ERR vector index does not exist" => return Ok(0),
                Err(err) => return Err(err),
            };
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            self.ensure_vector_runtime_unlocked(index, version, &meta)?;
        }
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
            .zip(self.store.multi_get_raw_async(&keys).await?)
            .filter_map(|(id, raw)| raw.map(|raw| (id, raw)))
            .map(|(id, raw)| Ok((id, decode_record::<VectorDocRecord>(&raw)?)))
            .filter_map(|result: Result<_, Error>| match result {
                Ok((_, doc)) if doc.deleted => None,
                other => Some(other),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut batch = WriteBatch::new();
        let mut deleted_docs = Vec::new();
        for (id, mut doc) in current_docs {
            if !meta.schema.is_empty() {
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
            }
            doc.doc_version = meta.next_doc_version;
            meta.next_doc_version = meta.next_doc_version.saturating_add(1);
            doc.deleted = true;
            batch.put(
                &vector_doc_key(self.key_layout, self.db_index, index, version, &id),
                &encode_record(&doc)?,
            )?;
            if meta.algorithm == VectorIndexAlgorithm::Hnsw {
                batch.put(
                    &vector_version_mutation_key(
                        self.key_layout,
                        self.db_index,
                        index,
                        version,
                        doc.doc_version,
                    ),
                    &encode_record(&VectorVersionMutation {
                        id: doc.id.clone(),
                        doc_version: doc.doc_version,
                        deleted: true,
                    })?,
                )?;
            }
            deleted_docs.push(doc);
        }
        let deleted = deleted_docs.len();
        if deleted == 0 {
            return Ok(0);
        }
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            global_metrics().record_vector_write_mutation_records(deleted);
        }
        meta.doc_count = meta.doc_count.saturating_sub(deleted as u64);
        batch.put(
            &vector_mutable_state_key(self.key_layout, self.db_index, index, version),
            &encode_record(&VectorMutableState::from_meta(&meta))?,
        )?;
        let batch_bytes = batch.iter().fold(0usize, |bytes, (_, key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        });
        global_metrics().record_vector_write_work(
            keys.len().saturating_add(3),
            2,
            batch.count() as usize,
            batch_bytes,
            lock_wait_us,
        );
        self.commit_vector_state_batch_if_unchanged_async(
            index,
            meta.internal,
            version,
            &expected_marker,
            expected_state,
            &batch,
        )
        .await?;
        self.record_public_vector_mutation(index, meta.internal);
        if meta.algorithm == VectorIndexAlgorithm::Hnsw {
            for doc in deleted_docs {
                self.vector_runtimes
                    .mark_deleted(self.db_index, index, version, doc);
            }
        }
        Ok(deleted)
    }
}
