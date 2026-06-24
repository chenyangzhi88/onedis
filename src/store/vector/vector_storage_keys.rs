fn vector_prefix(db_index: u16, ns: &[u8; 3], index: &str, version: u64) -> Vec<u8> {
    sub_key_range_start_bytes(db_index, ns, index.as_bytes(), version)
}

fn vector_meta_key(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_META_NAMESPACE, index, version);
    key.extend_from_slice(b"meta");
    key
}

fn vector_doc_prefix(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    vector_prefix(db_index, &VECTOR_DOC_NAMESPACE, index, version)
}

fn vector_doc_key(db_index: u16, index: &str, version: u64, id: &str) -> Vec<u8> {
    let mut key = vector_doc_prefix(db_index, index, version);
    key.extend_from_slice(id.as_bytes());
    key
}

fn vector_segment_prefix(db_index: u16, index: &str, version: u64) -> Vec<u8> {
    vector_prefix(db_index, &VECTOR_SEGMENT_NAMESPACE, index, version)
}

fn vector_segment_key(db_index: u16, index: &str, version: u64, segment_id: u64) -> Vec<u8> {
    let mut key = vector_segment_prefix(db_index, index, version);
    key.extend_from_slice(&segment_id.to_be_bytes());
    key
}

fn vector_graph_key(db_index: u16, index: &str, version: u64, segment_id: u64) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_GRAPH_NAMESPACE, index, version);
    key.extend_from_slice(&segment_id.to_be_bytes());
    key
}

fn vector_tag_key(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: &str,
    doc_id: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_TAG_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    append_len_prefixed(&mut key, value.as_bytes());
    key.extend_from_slice(doc_id.as_bytes());
    key
}

fn vector_tag_prefix(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_TAG_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    append_len_prefixed(&mut key, value.as_bytes());
    key
}

fn vector_numeric_key(
    db_index: u16,
    index: &str,
    version: u64,
    field: &str,
    value: f64,
    doc_id: &str,
) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_NUMERIC_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    key.extend_from_slice(&sortable_f64(value).to_be_bytes());
    key.extend_from_slice(doc_id.as_bytes());
    key
}

fn vector_numeric_field_prefix(db_index: u16, index: &str, version: u64, field: &str) -> Vec<u8> {
    let mut key = vector_prefix(db_index, &VECTOR_NUMERIC_NAMESPACE, index, version);
    append_len_prefixed(&mut key, field.as_bytes());
    key
}

fn append_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn sortable_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn unsortable_f64(value: u64) -> f64 {
    let bits = if value & (1 << 63) != 0 {
        value ^ (1 << 63)
    } else {
        !value
    };
    f64::from_bits(bits)
}
