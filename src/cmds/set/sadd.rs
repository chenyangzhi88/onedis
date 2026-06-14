use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Sadd {
    pub key: String,
    pub members: Vec<String>,
}

impl Sadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sadd' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let members: Vec<String> = args.iter().skip(2).map(|v| v.to_string()).collect(); // 成员

        Ok(Sadd { key, members })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_add(&self.key, &self.members) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_add_async(&self.key, &self.members).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
