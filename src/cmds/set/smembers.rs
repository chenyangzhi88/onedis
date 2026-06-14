use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Smembers {
    pub key: String,
}

impl Smembers {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'smembers' command",
            ));
        }

        let key = args[1].to_string(); // 键
        Ok(Smembers { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_members(&self.key) {
            Ok(members) => Ok(Frame::Array(
                members.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_members_async(&self.key).await {
            Ok(members) => Ok(Frame::Array(
                members.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
