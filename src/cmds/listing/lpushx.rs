use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Lpushx {
    key: String,
    values: Vec<String>,
}

impl Lpushx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lpushx' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let values: Vec<String> = args.iter().skip(2).map(|v| v.to_string()).collect(); // 值

        Ok(Lpushx { key, values })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_push_left(&self.key, &self.values, true) {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_push_left_async(&self.key, &self.values, true).await {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
