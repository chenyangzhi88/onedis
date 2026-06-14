use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zmscore {
    key: String,
    members: Vec<String>,
}

impl Zmscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zmscore' command",
            ));
        }
        Ok(Zmscore {
            key: args[1].to_string(),
            members: args[2..].iter().map(|arg| arg.to_string()).collect(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score(&self.key, &member) {
                Ok(Some(score)) => result.push(Frame::bulk_string(score.to_string())),
                Ok(None) => result.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Array(result))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.zset_score_async(&self.key, &member).await {
                Ok(Some(score)) => result.push(Frame::bulk_string(score.to_string())),
                Ok(None) => result.push(Frame::Null),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Array(result))
    }
}
