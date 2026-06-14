use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Ltrim {
    pub key: String,
    pub start: i64,
    pub stop: i64,
}

impl Ltrim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let start = frame.get_arg(2);
        let stop = frame.get_arg(3);

        if key.is_none() || start.is_none() || stop.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ltrim' command",
            ));
        }

        let final_key = key.unwrap().to_string();

        let start = match start.unwrap().parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        let stop = match stop.unwrap().parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        Ok(Ltrim {
            key: final_key,
            start,
            stop,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_trim(&self.key, self.start, self.stop) {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_trim_async(&self.key, self.start, self.stop).await {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
