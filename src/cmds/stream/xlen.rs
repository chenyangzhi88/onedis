use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Xlen {
    key: String,
}

impl Xlen {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'xlen' command"))?
            .to_string();
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xlen' command",
            ));
        }
        Ok(Self { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_len(&self.key) {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_len_async(&self.key).await {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
