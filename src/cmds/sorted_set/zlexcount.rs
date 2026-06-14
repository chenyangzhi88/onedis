use anyhow::Error;

use crate::{cmds::sorted_set::zrange::parse_lex_bound, frame::Frame, store::db::Db};

pub struct Zlexcount {
    key: String,
    min: crate::cmds::sorted_set::zrange::LexBound,
    max: crate::cmds::sorted_set::zrange::LexBound,
}

impl Zlexcount {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zlexcount' command",
            ));
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            min: parse_lex_bound(&frame.get_arg(2).unwrap())?,
            max: parse_lex_bound(&frame.get_arg(3).unwrap())?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_lex_count(&self.key, &self.min, &self.max) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_lex_count_async(&self.key, &self.min, &self.max)
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
