impl Handler {
    // 事务相关方法
    pub fn start_transaction(&mut self) -> Result<(), Error> {
        if self.session.is_in_transaction() {
            return Err(Error::msg("ERR MULTI calls can not be nested"));
        }
        let db = self.session.get_db().clone();
        let transaction_db = db.transactional_view()?;
        self.session.start_transaction();
        self.transaction_db = Some(transaction_db);
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
            if self
                .session
                .watched_keys()
                .iter()
                .any(|watched| watched.db_index == db_index && watched.key == key)
            {
                continue;
            }
            let (key_version, db_version) = db.watch_version_snapshot(&key)?;
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
        for watched in self.session.take_watches() {
            self.db_manager
                .get_db(watched.db_index)
                .release_watch(&watched.key);
        }
    }

    fn clear_transaction_and_watches(&mut self) {
        self.session.clear_transaction();
        self.clear_watches();
    }

    fn watched_keys_modified(&self) -> Result<bool, Error> {
        self.session.watched_keys().iter().try_fold(false, |changed, watched| {
            if changed {
                return Ok(true);
            }
            let db = self.db_manager.get_db(watched.db_index);
            db.watch_version_changed(&watched.key, watched.key_version, watched.db_version)
        })
    }
}
