use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Ttl {
    key: String,
}

impl Ttl {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ttl' command",
            ));
        }
        let fianl_key = key.unwrap().to_string();
        Ok(Ttl { key: fianl_key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly(&self.key);
        let second = if millis < 0 { millis } else { millis / 1000 };
        Ok(Frame::Integer(second))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let millis = db.ttl_millis_readonly_async(&self.key).await;
        let second = if millis < 0 { millis } else { millis / 1000 };
        Ok(Frame::Integer(second))
    }
}
