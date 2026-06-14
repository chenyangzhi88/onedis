use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zremrangebyscore {
    key: String,
    min: f64,
    max: f64,
}

impl Zremrangebyscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zremrangebyscore' command",
            ));
        }
        Ok(Zremrangebyscore {
            key: args[1].to_string(),
            min: args[2]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR min is not a valid float"))?,
            max: args[3]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR max is not a valid float"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_remove_range_by_score(&self.key, self.min, self.max) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_remove_range_by_score_async(&self.key, self.min, self.max)
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
