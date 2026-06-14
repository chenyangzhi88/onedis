use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zremrangebyrank {
    key: String,
    start: i64,
    stop: i64,
}

impl Zremrangebyrank {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zremrangebyrank' command",
            ));
        }
        Ok(Zremrangebyrank {
            key: args[1].to_string(),
            start: args[2]
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            stop: args[3]
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_remove_range_by_rank(&self.key, self.start, self.stop) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_remove_range_by_rank_async(&self.key, self.start, self.stop)
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
