use super::*;

impl Db {
    pub(in crate::store::db) fn mk(&self, key: &str) -> Vec<u8> {
        self.key_layout.main_key(self.db_index, key)
    }

    /// Read the encoded top-level key value used by Redis WATCH snapshots.
    ///
    /// The command watches logical keys, so a changed structure header, TTL, or
    /// type version must invalidate the transaction even if nested data lives in
    /// secondary namespaces.
    pub fn raw_main_value_for_watch(&self, key: &str) -> Option<Vec<u8>> {
        self.expire_if_needed(key);
        self.store.get_raw(&self.mk(key))
    }

    pub fn watch_version_snapshot(&self, key: &str) -> (u64, u64) {
        self.mutation_tracker.enable();
        self.expire_if_needed(key);
        let key_version = self.mutation_tracker.key_version(&self.mk(key));
        let db_version = self.mutation_tracker.db_version(self.db_index);
        (key_version, db_version)
    }

    pub fn watch_version_changed(&self, key: &str, key_version: u64, db_version: u64) -> bool {
        self.expire_if_needed(key);
        self.mutation_tracker.key_version(&self.mk(key)) != key_version
            || self.mutation_tracker.db_version(self.db_index) != db_version
    }
}
