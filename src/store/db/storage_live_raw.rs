impl Db {
    fn load_live_raw_for_db_with_backend(
        store: &KvStore,
        db_index: u16,
        key: &str,
    ) -> Option<Vec<u8>> {
        let key_bytes = main_key(db_index, key);
        if let Some(raw) = store.get_raw(&key_bytes) {
            let raw = raw.clone();
            let expire_ms = decode_expire_ms(&raw);
            if expire_ms > 0 && now_ms() >= expire_ms {
                let mut batch = WriteBatch::new();
                Self::delete_structure_for_db_to_batch(&mut batch, db_index, key, &raw);
                store.write_batch(&batch);
                return None;
            }
            return Some(raw);
        }
        None
    }
}
