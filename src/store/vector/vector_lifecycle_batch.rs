struct VectorMarker<'a> {
    layout: KeyEncodingLayout,
    db_index: u16,
    index: &'a str,
    expire_ms: u64,
    version: u64,
    dim: u32,
    internal: bool,
}

fn put_vector_marker_to_batch(
    batch: &mut WriteBatch,
    marker: VectorMarker<'_>,
) -> Result<(), Error> {
    let value = Structure::VectorCollection(Vector {
        dimension: marker.dim as usize,
        vectors: Default::default(),
        norms: Default::default(),
    });
    let marker_key = if marker.internal {
        vector_internal_marker_key(marker.layout, marker.db_index, marker.index)
    } else {
        marker.layout.main_key(marker.db_index, marker.index)
    };
    batch.put(
        &marker_key,
        &encode_entry(&value, marker.expire_ms, marker.version),
    )?;
    Ok(())
}

fn delete_vector_namespace_to_batch(
    batch: &mut WriteBatch,
    layout: KeyEncodingLayout,
    db_index: u16,
    index: &str,
    version: u64,
) -> Result<(), Error> {
    for namespace in [
        VECTOR_META_NAMESPACE,
        VECTOR_DOC_NAMESPACE,
        VECTOR_TAG_NAMESPACE,
        VECTOR_NUMERIC_NAMESPACE,
        VECTOR_SEGMENT_NAMESPACE,
        VECTOR_GRAPH_NAMESPACE,
    ] {
        let start = vector_prefix(layout, db_index, &namespace, index, version);
        let end = layout.sub_key_range_end_bytes(
            db_index,
            &namespace,
            index.as_bytes(),
            version,
        );
        batch.delete_range(&start, &end)?;
    }
    Ok(())
}

fn delete_vector_segments_to_batch(
    batch: &mut WriteBatch,
    layout: KeyEncodingLayout,
    db_index: u16,
    index: &str,
    version: u64,
) -> Result<(), Error> {
    for namespace in [VECTOR_SEGMENT_NAMESPACE, VECTOR_GRAPH_NAMESPACE] {
        let start = vector_prefix(layout, db_index, &namespace, index, version);
        let end = layout.sub_key_range_end_bytes(
            db_index,
            &namespace,
            index.as_bytes(),
            version,
        );
        batch.delete_range(&start, &end)?;
    }
    Ok(())
}
