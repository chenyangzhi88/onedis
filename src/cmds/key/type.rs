use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Type {
    pub key: String,
}

impl Type {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'type' command",
            ));
        }
        let final_key = key.unwrap().to_string();
        Ok(Type { key: final_key })
    }

    pub fn new(key: String) -> Self {
        Type { key }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::SimpleString(
            db.type_name_readonly(&self.key).to_string(),
        ))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::SimpleString(
            db.type_name_readonly_async(&self.key).await.to_string(),
        ))
    }
}
