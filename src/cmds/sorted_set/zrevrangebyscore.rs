use anyhow::Error;

use crate::{cmds::sorted_set::zrange::flatten_entries, frame::Frame, store::db::Db};

pub struct Zrevrangebyscore {
    key: String,
    max: f64,
    min: f64,
    withscores: bool,
}

impl Zrevrangebyscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 || args.len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrevrangebyscore' command",
            ));
        }
        let withscores = args.len() == 5 && args[4].eq_ignore_ascii_case("WITHSCORES");
        if args.len() == 5 && !withscores {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self {
            key: args[1].clone(),
            max: args[2]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR max is not a valid float"))?,
            min: args[3]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR min is not a valid float"))?,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_rev_range_by_score(&self.key, self.max, self.min) {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_rev_range_by_score_async(&self.key, self.max, self.min)
            .await
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
