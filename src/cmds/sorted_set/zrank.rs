use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zrank {
    pub key: String,
    pub member: String,
}

impl Zrank {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrank' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let member = args[2].to_string(); // 成员
        Ok(Zrank { key, member })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_rank(&self.key, &self.member) {
            Ok(Some(rank)) => Ok(Frame::Integer(rank as i64)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_rank_async(&self.key, &self.member).await {
            Ok(Some(rank)) => Ok(Frame::Integer(rank as i64)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
