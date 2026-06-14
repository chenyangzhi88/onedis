use anyhow::Error;

use crate::{cmds::sorted_set::zrange::flatten_entries, frame::Frame, store::db::Db};

pub struct Zrevrange {
    key: String,
    start: i64,
    stop: i64,
    withscores: bool,
}

impl Zrevrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 || args.len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrevrange' command",
            ));
        }

        let key = args[1].to_string();
        let start = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let stop = args[3]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let withscores = args.len() == 5 && args[4].eq_ignore_ascii_case("WITHSCORES");

        if args.len() == 5 && !withscores {
            return Err(Error::msg("ERR syntax error"));
        }

        Ok(Self {
            key,
            start,
            stop,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_range(&self.key, self.start, self.stop, true) {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_range_async(&self.key, self.start, self.stop, true)
            .await
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
