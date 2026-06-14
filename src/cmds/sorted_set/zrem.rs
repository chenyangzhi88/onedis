use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Zrem {
    pub key: String,
    pub members: Vec<String>,
}

impl Zrem {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrem' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let members = args[2..].iter().map(|arg| arg.to_string()).collect(); // 要移除的成员
        Ok(Zrem { key, members })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_remove(&self.key, &self.members) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_remove_async(&self.key, &self.members).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
