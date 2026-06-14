use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hlen {
    pub key: String,
}

impl Hlen {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hlen' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        Ok(Hlen { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_len(&self.key) {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_len_async(&self.key).await {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
