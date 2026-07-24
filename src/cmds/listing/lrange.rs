use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Lrange {
    key: String,
    start: i64,
    stop: i64,
}

impl Lrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let start = frame.get_arg(2);
        let stop = frame.get_arg(3);

        if frame.arg_len() != 4 || key.is_none() || start.is_none() || stop.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lrange' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键

        let start = match start.unwrap().parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        let stop = match stop.unwrap().parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        Ok(Lrange {
            key: final_key,
            start,
            stop,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_range(&self.key, self.start, self.stop) {
            Ok(items) => Ok(Frame::Array(
                items.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_range_async(&self.key, self.start, self.stop).await {
            Ok(items) => Ok(Frame::Array(
                items.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
