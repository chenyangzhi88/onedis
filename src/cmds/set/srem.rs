use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Srem {
    pub key: String,
    pub members: Vec<String>,
}

impl Srem {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'srem' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let members = args[2..].iter().map(|arg| arg.to_string()).collect(); // 要移除的成员

        Ok(Srem { key, members })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_remove(&self.key, &self.members) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_remove_async(&self.key, &self.members).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
