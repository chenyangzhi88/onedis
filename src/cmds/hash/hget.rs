use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hget {
    pub key: String,
    pub field: String,
}

impl Hget {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let field = frame.get_arg(2);

        if frame.arg_len() != 3 || key.is_none() || field.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hget' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键
        let final_field = field.unwrap().to_string(); // 字段

        Ok(Hget {
            key: final_key,
            field: final_field,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get(&self.key, &self.field) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_async(&self.key, &self.field).await {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
