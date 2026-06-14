use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Get {
    pub key: String,
}

impl Get {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'get' command",
            ));
        }

        let fianl_key = key.unwrap().to_string();

        Ok(Get { key: fianl_key })
    }

    pub fn new(key: String) -> Self {
        Get { key }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_bytes(&self.key)? {
            Some(value) => Ok(Frame::bulk_string(value)),
            None => Ok(Frame::Null),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_bytes_async(&self.key).await? {
            Some(value) => Ok(Frame::bulk_string(value)),
            None => Ok(Frame::Null),
        }
    }
}
