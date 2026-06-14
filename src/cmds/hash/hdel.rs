use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hdel {
    pub key: String,
    pub fields: Vec<String>,
}

impl Hdel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hdel' command",
            ));
        }

        let key: String = args[1].to_string();
        let fields = args[2..].iter().map(|arg| arg.to_string()).collect();

        Ok(Hdel { key, fields })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_delete(&self.key, &self.fields) {
            Ok(deleted) => Ok(Frame::Integer(deleted as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_delete_async(&self.key, &self.fields).await {
            Ok(deleted) => Ok(Frame::Integer(deleted as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
