use crate::{cmds::listing::list_array, frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Lrange {
    key: String,
    start: i64,
    stop: i64,
}

impl Lrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lrange' command",
            ));
        }
        let final_key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'lrange' command"))?;
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

        Ok(Lrange {
            key: final_key,
            start,
            stop,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_range(&self.key, self.start, self.stop) {
            Ok(items) => list_array(items),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_range_async(&self.key, self.start, self.stop).await {
            Ok(items) => list_array(items),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
