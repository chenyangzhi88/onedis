use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Persist {
    key: String,
}

impl Persist {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'persist' command",
            ));
        }
        let key_str = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'persist' command"))?;
        Ok(Persist { key: key_str })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if db.persist(&self.key) {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if db.persist_async(&self.key).await {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        }
    }
}
