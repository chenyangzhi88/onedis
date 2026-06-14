use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Strlen {
    key: String,
}

impl Strlen {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'strlen' command",
            ));
        }

        let final_key = key.unwrap().to_string();

        Ok(Strlen { key: final_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string(&self.key)? {
            Some(value) => Ok(Frame::Integer(value.len() as i64)),
            None => Ok(Frame::Integer(0)),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_async(&self.key).await? {
            Some(value) => Ok(Frame::Integer(value.len() as i64)),
            None => Ok(Frame::Integer(0)),
        }
    }
}
