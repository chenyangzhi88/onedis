use anyhow::Error;

use crate::{cmds::sorted_set::common::entries_with_scores, frame::Frame, store::db::Db};

pub struct Zpopmin {
    pub(crate) key: String,
    pub(crate) count: usize,
    pub(crate) min: bool,
}

impl Zpopmin {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_zpop(frame, true, "zpopmin")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_pop(&self.key, self.min, self.count) {
            Ok(entries) => Ok(Frame::Array(entries_with_scores(entries))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_pop_async(&self.key, self.min, self.count).await {
            Ok(entries) => Ok(Frame::Array(entries_with_scores(entries))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_zpop(frame: Frame, min: bool, command: &str) -> Result<Zpopmin, Error> {
    if frame.arg_len() < 2 || frame.arg_len() > 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    }
    let count = if frame.arg_len() == 3 {
        frame
            .get_arg(2)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?
    } else {
        1
    };
    Ok(Zpopmin {
        key: frame.get_arg(1).unwrap(),
        count,
        min,
    })
}
