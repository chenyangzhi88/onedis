use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hexists {
    pub key: String,
    pub field: String,
}

impl Hexists {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let field = frame.get_arg(2);

        if key.is_none() || field.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hexists' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键
        let final_field = field.unwrap().to_string(); // 字段

        Ok(Hexists {
            key: final_key,
            field: final_field,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_exists(&self.key, &self.field) {
            Ok(exists) => Ok(Frame::Integer(if exists { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_exists_async(&self.key, &self.field).await {
            Ok(exists) => Ok(Frame::Integer(if exists { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
