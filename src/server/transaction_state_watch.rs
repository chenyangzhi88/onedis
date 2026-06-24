impl Handler {
    // 事务相关方法
    pub fn start_transaction(&mut self) -> Result<(), Error> {
        self.session.start_transaction();
        let db = self.session.get_db().clone();
        self.transaction_db = Some(db.transactional_view()?);
        Ok(())
    }

    pub fn is_in_transaction(&self) -> bool {
        self.session.is_in_transaction()
    }

    pub fn add_transaction_frame(&mut self, frame: Frame) {
        self.session.add_transaction_frame(frame);
    }

    pub fn get_transaction_frames(&self) -> Vec<Frame> {
        self.session.get_transaction_frames().clone()
    }

    pub fn clear_transaction(&mut self) {
        self.clear_transaction_and_watches();
        self.transaction_db = None;
    }

    pub fn watch_keys(&mut self, keys: Vec<String>) -> Result<(), Error> {
        if self.session.is_in_transaction() {
            return Err(Error::msg("ERR WATCH inside MULTI is not allowed"));
        }
        let db_index = self.session.get_current_db();
        let db = self.session.get_db().clone();
        for key in keys {
            let (key_version, db_version) = db.watch_version_snapshot(&key);
            self.session.watch_key(WatchedKey {
                db_index,
                key,
                key_version,
                db_version,
            });
        }
        Ok(())
    }

    pub fn clear_watches(&mut self) {
        self.session.clear_watches();
    }

    fn clear_transaction_and_watches(&mut self) {
        self.session.clear_transaction();
        self.session.clear_watches();
    }

    fn watched_keys_modified(&self) -> bool {
        self.session.watched_keys().iter().any(|watched| {
            let db = self.db_manager.get_db(watched.db_index);
            db.watch_version_changed(&watched.key, watched.key_version, watched.db_version)
        })
    }
}
