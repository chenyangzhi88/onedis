impl Db {
    fn get_expire_ms(&self, key: &str) -> u64 {
        if let Some(raw) = self.store.get_raw(&main_key(self.db_index, key)) {
            decode_expire_ms(&raw)
        } else {
            0
        }
    }

    fn get_expire_and_version(&self, key: &str) -> (u64, u64) {
        if let Some(raw) = self.store.get_raw(&main_key(self.db_index, key)) {
            if let Some(header) = decode_meta_header(&raw) {
                return (header.expire_ms, header.version);
            }
        }
        (0, 0)
    }
}
