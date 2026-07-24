use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Ltrim {
    pub key: String,
    pub start: i64,
    pub stop: i64,
}

impl Ltrim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ltrim' command",
            ));
        }
        let final_key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'ltrim' command"))?;
        let start = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let stop = frame
            .get_arg(3)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

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
