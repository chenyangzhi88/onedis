use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Flushdb {}

impl Default for Flushdb {
    fn default() -> Self {
        Self::new()
    }
}

impl Flushdb {
    pub fn new() -> Flushdb {
        Flushdb {}
    }

    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() > 2
            || (frame.arg_len() == 2
                && !frame.get_arg(1).is_some_and(|arg| {
                    arg.eq_ignore_ascii_case("ASYNC") || arg.eq_ignore_ascii_case("SYNC")
                }))
        {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Flushdb {})
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.clear();
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.clear_async().await;
        Ok(Frame::Ok)
    }
}
