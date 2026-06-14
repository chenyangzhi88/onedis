use anyhow::Error;

use crate::{cmds::sorted_set::zrange::flatten_entries, frame::Frame, store::db::Db};

pub struct Zrandmember {
    key: String,
    count: Option<i64>,
    withscores: bool,
}

impl Zrandmember {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 2 || args.len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrandmember' command",
            ));
        }
        let mut count = None;
        let mut withscores = false;
        if args.len() >= 3 {
            if args[2].eq_ignore_ascii_case("WITHSCORES") {
                withscores = true;
            } else {
                count = Some(
                    args[2]
                        .parse::<i64>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                );
            }
        }
        if args.len() == 4 {
            if !args[3].eq_ignore_ascii_case("WITHSCORES") {
                return Err(Error::msg("ERR syntax error"));
            }
            withscores = true;
        }
        Ok(Self {
            key: args[1].clone(),
            count,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_random_members(&self.key, self.count) {
            Ok(Some(entries)) if self.count.is_none() => {
                let Some((member, score)) = entries.into_iter().next() else {
                    return Ok(Frame::Null);
                };
                if self.withscores {
                    Ok(Frame::Array(flatten_entries(vec![(member, score)], true)))
                } else {
                    Ok(Frame::bulk_string(member))
                }
            }
            Ok(Some(entries)) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_random_members_async(&self.key, self.count).await {
            Ok(Some(entries)) if self.count.is_none() => {
                let Some((member, score)) = entries.into_iter().next() else {
                    return Ok(Frame::Null);
                };
                if self.withscores {
                    Ok(Frame::Array(flatten_entries(vec![(member, score)], true)))
                } else {
                    Ok(Frame::bulk_string(member))
                }
            }
            Ok(Some(entries)) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
