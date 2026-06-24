fn put_vector_marker_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    expire_ms: u64,
    version: u64,
    dim: u32,
) {
    let marker = Structure::VectorCollection(Vector {
        dimension: dim as usize,
        vectors: Default::default(),
        norms: Default::default(),
    });
    batch.put(
        &main_key(db_index, index),
        &encode_entry(&marker, expire_ms, version),
    );
}

fn persist_vector_segment_snapshot(
    store: &crate::store::kv_store::KvStore,
    db_index: u16,
    index: &str,
    version: u64,
    segment: &VectorSegmentMeta,
    snapshot_raw: &[u8],
) -> Result<(), Error> {
    store.blob_put_raw(&segment.graph_key, snapshot_raw);
    let meta_key = vector_meta_key(db_index, index, version);
    let Some(meta_raw) = store.get_raw(&meta_key) else {
        return Err(Error::msg("ERR vector index metadata missing"));
    };
    let mut meta = decode_record::<VectorIndexMeta>(&meta_raw)?;
    meta.next_segment_id = meta
        .next_segment_id
        .max(segment.segment_id.saturating_add(1));
    meta.snapshot_doc_version = meta.snapshot_doc_version.max(segment.max_doc_version);
    let mut batch = WriteBatch::new();
    batch.put(
        &vector_segment_key(db_index, index, version, segment.segment_id),
        &encode_record(segment)?,
    );
    batch.put(&meta_key, &encode_record(&meta)?);
    store.write_batch(&batch);
    Ok(())
}

fn delete_vector_namespace_to_batch(
    store: &crate::store::kv_store::KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
) {
    for namespace in [
        VECTOR_META_NAMESPACE,
        VECTOR_DOC_NAMESPACE,
        VECTOR_TAG_NAMESPACE,
        VECTOR_NUMERIC_NAMESPACE,
        VECTOR_SEGMENT_NAMESPACE,
        VECTOR_GRAPH_NAMESPACE,
    ] {
        let prefix = vector_prefix(db_index, &namespace, index, version);
        for (key, _) in store.scan_prefix_raw(&prefix) {
            batch.delete(&key);
        }
    }
}

fn delete_vector_segments_to_batch(
    store: &crate::store::kv_store::KvStore,
    batch: &mut WriteBatch,
    db_index: u16,
    index: &str,
    version: u64,
) {
    for namespace in [VECTOR_SEGMENT_NAMESPACE, VECTOR_GRAPH_NAMESPACE] {
        let prefix = vector_prefix(db_index, &namespace, index, version);
        for (key, _) in store.scan_prefix_raw(&prefix) {
            batch.delete(&key);
        }
    }
}
