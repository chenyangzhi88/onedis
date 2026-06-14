use anyhow::Error;

use crate::{frame::Frame, store::db_manager::DatabaseManager};
use kv_engine::db::DB;

pub struct Bgsave {}

impl Bgsave {
    pub fn parse_from_frame(_frame: Frame) -> Result<Self, Error> {
        Ok(Bgsave {})
    }

    pub fn apply_sync(self, db_manager: &DatabaseManager) -> Result<Frame, Error> {
        db_manager
            .store()
            .db()
            .manual_compaction()
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(Frame::Ok)
    }
}
