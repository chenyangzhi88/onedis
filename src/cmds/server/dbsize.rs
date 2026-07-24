use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Dbsize {}

impl Dbsize {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'dbsize' command",
            ));
        }
        Ok(Dbsize {})
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let size = db.len();
        Ok(Frame::Integer(size as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let size = db.len_async().await;
        Ok(Frame::Integer(size as i64))
    }
}
