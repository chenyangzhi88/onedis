use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Incr {
    pub key: String,
}

impl Incr {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'incr' command",
            ));
        }
        let key = args[1].to_string(); // 键
        Ok(Incr { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.increment_integer_string(&self.key, 1) {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.increment_integer_string_async(&self.key, 1).await {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
