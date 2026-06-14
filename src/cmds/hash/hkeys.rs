use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hkeys {
    key: String,
}

impl Hkeys {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hkeys' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        Ok(Hkeys { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_keys(&self.key) {
            Ok(keys) => Ok(Frame::Array(
                keys.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_keys_async(&self.key).await {
            Ok(keys) => Ok(Frame::Array(
                keys.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
