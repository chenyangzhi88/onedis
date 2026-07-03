impl Db {
    pub fn vector_card(&self, index: &str) -> Result<u64, Error> {
        match self.read_vector_meta(index) {
            Ok((_, _, meta)) => Ok(meta.doc_count),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub async fn vector_card_async(&self, index: &str) -> Result<u64, Error> {
        match self.read_vector_meta_async(index).await {
            Ok((_, _, meta)) => Ok(meta.doc_count),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub fn vector_dim(&self, index: &str) -> Result<Option<u32>, Error> {
        match self.read_vector_meta(index) {
            Ok((_, _, meta)) => Ok(Some(meta.dim)),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn vector_dim_async(&self, index: &str) -> Result<Option<u32>, Error> {
        match self.read_vector_meta_async(index).await {
            Ok((_, _, meta)) => Ok(Some(meta.dim)),
            Err(err) if err.to_string() == "ERR vector index does not exist" => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn vector_ids(&self, index: &str) -> Result<Vec<String>, Error> {
        let (_, version, _) = match self.read_vector_meta(index) {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.db_index, index, version);
        let mut ids = self
            .store
            .scan_prefix_raw(&prefix)
            .into_iter()
            .filter_map(|(_, raw)| decode_record::<VectorDocRecord>(&raw).ok())
            .filter(|doc| !doc.deleted)
            .map(|doc| doc.id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub async fn vector_ids_async(&self, index: &str) -> Result<Vec<String>, Error> {
        let (_, version, _) = match self.read_vector_meta_async(index).await {
            Ok(value) => value,
            Err(err) if err.to_string() == "ERR vector index does not exist" => {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let prefix = vector_doc_prefix(self.db_index, index, version);
        let mut ids = self
            .store
            .scan_prefix_raw_async(&prefix)
            .await
            .into_iter()
            .filter_map(|(_, raw)| decode_record::<VectorDocRecord>(&raw).ok())
            .filter(|doc| !doc.deleted)
            .map(|doc| doc.id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub fn vector_info(&self, index: &str) -> Result<Vec<(String, String)>, Error> {
        let (_, version, meta) = self.read_vector_meta(index)?;
        Ok(vec![
            ("dim".to_string(), meta.dim.to_string()),
            (
                "distance".to_string(),
                distance_name(meta.distance).to_string(),
            ),
            ("doc_count".to_string(), meta.doc_count.to_string()),
            ("schema_fields".to_string(), meta.schema.len().to_string()),
            ("m".to_string(), meta.m.to_string()),
            (
                "ef_construction".to_string(),
                meta.ef_construction.to_string(),
            ),
            ("ef_runtime".to_string(), meta.ef_runtime.to_string()),
            (
                "hnsw_nodes".to_string(),
                self.vector_runtime_len(index, version, meta.doc_count)
                    .to_string(),
            ),
            (
                "snapshot_doc_version".to_string(),
                meta.snapshot_doc_version.to_string(),
            ),
        ])
    }

    pub async fn vector_info_async(&self, index: &str) -> Result<Vec<(String, String)>, Error> {
        let (_, version, meta) = self.read_vector_meta_async(index).await?;
        Ok(vec![
            ("dim".to_string(), meta.dim.to_string()),
            (
                "distance".to_string(),
                distance_name(meta.distance).to_string(),
            ),
            ("doc_count".to_string(), meta.doc_count.to_string()),
            ("schema_fields".to_string(), meta.schema.len().to_string()),
            ("m".to_string(), meta.m.to_string()),
            (
                "ef_construction".to_string(),
                meta.ef_construction.to_string(),
            ),
            ("ef_runtime".to_string(), meta.ef_runtime.to_string()),
            (
                "hnsw_nodes".to_string(),
                self.vector_runtime_len(index, version, meta.doc_count)
                    .to_string(),
            ),
            (
                "snapshot_doc_version".to_string(),
                meta.snapshot_doc_version.to_string(),
            ),
        ])
    }

    pub fn vector_observability_snapshot(&self) -> VectorObservabilitySnapshot {
        let mut snapshot = VectorObservabilitySnapshot::default();
        let now = super::now_ms();
        for key in self.logical_keys() {
            let Some(raw) = self.store.get_raw(&self.mk(&key)) else {
                continue;
            };
            let Some(header) = decode_meta_header(&raw) else {
                continue;
            };
            if header.type_tag == TYPE_VECTOR && (header.expire_ms == 0 || now < header.expire_ms) {
                snapshot.indexes += 1;
            }
        }
        snapshot
    }
}
