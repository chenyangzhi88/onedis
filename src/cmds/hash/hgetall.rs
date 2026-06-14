use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hgetall {
    key: String,
}

impl Hgetall {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hgetall' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        Ok(Hgetall { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_all(&self.key) {
            Ok(entries) => {
                let mut result = Vec::with_capacity(entries.len() * 2);
                for (field, value) in entries {
                    result.push(Frame::bulk_string(field));
                    result.push(Frame::bulk_string(value));
                }
                Ok(Frame::Array(result))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_all_async(&self.key).await {
            Ok(entries) => {
                let mut result = Vec::with_capacity(entries.len() * 2);
                for (field, value) in entries {
                    result.push(Frame::bulk_string(field));
                    result.push(Frame::bulk_string(value));
                }
                Ok(Frame::Array(result))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
