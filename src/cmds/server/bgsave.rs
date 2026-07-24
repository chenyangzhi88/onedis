use anyhow::Error;

use crate::{frame::Frame, store::db_manager::DatabaseManager};

pub struct Bgsave {}

impl Bgsave {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() > 2
            || (frame.arg_len() == 2
                && !frame
                    .get_arg(1)
                    .is_some_and(|arg| arg.eq_ignore_ascii_case("SCHEDULE")))
        {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bgsave' command",
            ));
        }
        Ok(Bgsave {})
    }

    pub fn apply_sync(self, db_manager: &DatabaseManager) -> Result<Frame, Error> {
        db_manager
            .store()
            .manual_compaction()
            .map_err(|err| Error::msg(err.to_string()))?;
        Ok(Frame::Ok)
    }
}
