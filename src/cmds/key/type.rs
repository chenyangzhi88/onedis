use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Type {
    pub key: String,
}

impl Type {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'type' command",
            ));
        }
        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'type' command"))?;
        Ok(Type { key })
    }

    pub fn new(key: String) -> Self {
        Type { key }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::SimpleString(
            db.type_name_readonly(&self.key)?.to_string(),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::SimpleString(
            db.type_name_readonly_async(&self.key).await?.to_string(),
        ))
    }
}
