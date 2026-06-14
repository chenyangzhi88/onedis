use anyhow::Error;

use crate::{cmds::sorted_set::zrange::flatten_entries, frame::Frame, store::db::Db};

pub struct Zrangebyscore {
    key: String,
    min: f64,
    max: f64,
    withscores: bool,
}

impl Zrangebyscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 || args.len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrangebyscore' command",
            ));
        }

        let key = args[1].to_string();
        let min = args[2]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR min is not a valid float"))?;
        let max = args[3]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR max is not a valid float"))?;
        let withscores = args.len() == 5 && args[4].eq_ignore_ascii_case("WITHSCORES");

        if args.len() == 5 && !withscores {
            return Err(Error::msg("ERR syntax error"));
        }

        Ok(Self {
            key,
            min,
            max,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_range_by_score(&self.key, self.min, self.max) {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_range_by_score_async(&self.key, self.min, self.max)
            .await
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
