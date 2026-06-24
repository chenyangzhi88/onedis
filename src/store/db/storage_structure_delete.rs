impl Db {
    fn delete_structure_for_db_to_batch(
        batch: &mut WriteBatch,
        db_index: u16,
        key: &str,
        raw: &[u8],
    ) {
        let key_bytes = main_key(db_index, key);
        batch.delete(&key_bytes);
        if let Some(header) = decode_meta_header(raw) {
            delete_sub_keys_to_batch(batch, db_index, key, header.version, header.type_tag);
        }
    }
}
