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

    pub fn apply_sync(self, _db_manager: &DatabaseManager) -> Result<Frame, Error> {
        // Compatibility-only command. The engine persists writes continuously, so forcing a
        // compaction/WAL sync here would turn an otherwise harmless probe into a blocking
        // resource spike.
        Ok(Frame::Ok)
    }
}
