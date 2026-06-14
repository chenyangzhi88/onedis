use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Scard {
    pub key: String,
}

impl Scard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'scard' command",
            ));
        }

        let key = args[1].to_string(); // 键

        Ok(Scard { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_len(&self.key) {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_members_async(&self.key).await {
            Ok(members) => Ok(Frame::Integer(members.len() as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
