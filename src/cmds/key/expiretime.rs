use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct ExpireTime {
    key: String,
}

impl ExpireTime {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'expiretime' command",
            ));
        }
        Ok(ExpireTime {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let expire_ms = db.expire_time_millis_readonly(&self.key);
        let value = if expire_ms > 0 {
            expire_ms / 1000
        } else {
            expire_ms
        };
        Ok(Frame::Integer(value))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let expire_ms = db.expire_time_millis_readonly_async(&self.key).await;
        let value = if expire_ms > 0 {
            expire_ms / 1000
        } else {
            expire_ms
        };
        Ok(Frame::Integer(value))
    }
}
