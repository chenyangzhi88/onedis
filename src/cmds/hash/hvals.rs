use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hvals {
    key: String,
}

impl Hvals {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if frame.arg_len() != 2 || key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hvals' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        Ok(Hvals { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_values(&self.key) {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_values_async(&self.key).await {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
