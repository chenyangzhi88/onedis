use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Pttl {
    key: String,
}

impl Pttl {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        if frame.arg_len() != 2 || key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pttl' command",
            ));
        }
        let fianl_key = key.unwrap().to_string();
        Ok(Pttl { key: fianl_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly(&self.key);
        Ok(Frame::Integer(millis))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly_async(&self.key).await;
        Ok(Frame::Integer(millis))
    }
}
