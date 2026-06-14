use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Lpop {
    pub key: String,
}

impl Lpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lpop' command",
            ));
        }

        let key = args[1].to_string(); // 键

        Ok(Lpop { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_pop_left(&self.key) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_pop_left_async(&self.key).await {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
