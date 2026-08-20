use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Pttl {
    key: String,
}

impl Pttl {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pttl' command",
            ));
        }
        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'pttl' command"))?;
        Ok(Pttl { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly(&self.key)?;
        Ok(Frame::Integer(millis))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly_async(&self.key).await?;
        Ok(Frame::Integer(millis))
    }
}
