use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Persist {
    key: String,
}

impl Persist {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'persist' command",
            ));
        }
        let key_str = key.unwrap().to_string(); // 键
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
