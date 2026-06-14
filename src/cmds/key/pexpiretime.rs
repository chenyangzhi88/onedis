use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct PexpireTime {
    key: String,
}

impl PexpireTime {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pexpiretime' command",
            ));
        }
        Ok(PexpireTime {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(db.expire_time_millis_readonly(&self.key)))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.expire_time_millis_readonly_async(&self.key).await,
        ))
    }
}
