use crate::{cmds::hash::common::checked_hash_entries, frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hgetall {
    key: String,
}

impl Hgetall {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if frame.arg_len() != 2 || key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hgetall' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        Ok(Hgetall { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_all_bytes(&self.key) {
            Ok(entries) => checked_hash_entries(entries, true),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_all_bytes_async(&self.key).await {
            Ok(entries) => checked_hash_entries(entries, true),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
