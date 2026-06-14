use anyhow::Error;

use crate::{frame::Frame, store::db_manager::DatabaseManager};
use kv_engine::db::DB;

pub struct Save {}

impl Save {
    pub fn parse_from_frame(_frame: Frame) -> Result<Self, Error> {
        Ok(Save {})
    }

    pub fn apply_sync(self, db_manager: &DatabaseManager) -> Result<Frame, Error> {
        let db = db_manager.store().db();
        db.manual_compaction()
            .map_err(|err| Error::msg(err.to_string()))?;
        db.sync_wal().map_err(|err| Error::msg(err.to_string()))?;
        Ok(Frame::Ok)
    }
}
