use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zscore {
    pub key: String,
    pub member: String,
}

impl Zscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zscore' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let member = args[2].to_string(); // 成员
        Ok(Zscore { key, member })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_score(&self.key, &self.member) {
            Ok(Some(score)) => Ok(Frame::bulk_string(score.to_string())),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_score_async(&self.key, &self.member).await {
            Ok(Some(score)) => Ok(Frame::bulk_string(score.to_string())),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
