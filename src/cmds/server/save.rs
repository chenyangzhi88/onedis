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
        Ok(Frame::Error(
            "ERR SAVE is unsupported; durability and checkpoints are managed by kv-engine"
                .to_string(),
        ))
    }
}
