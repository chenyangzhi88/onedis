use anyhow::Error;

use crate::{frame::Frame, store::db_manager::DatabaseManager};

pub struct Save {}

impl Save {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'save' command",
            ));
        }
        Ok(Save {})
    }

    pub fn apply_sync(self, db_manager: &DatabaseManager) -> Result<Frame, Error> {
        db_manager
            .store()
            .manual_compaction()
            .map_err(|err| Error::msg(err.to_string()))?;
        db_manager
            .store()
            .sync_wal()
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(Frame::Ok)
    }
}
