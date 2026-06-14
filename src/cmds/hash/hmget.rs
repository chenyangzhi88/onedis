use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hmget {
    key: String,
    fields: Vec<String>,
}

impl Hmget {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hmget' command",
            ));
        }

        let key = args[1].to_string();
        let fields = args[2..].iter().map(|arg| arg.to_string()).collect();

        Ok(Hmget { key, fields })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_multi_get(&self.key, &self.fields) {
            Ok(values) => Ok(Frame::Array(
                values
                    .into_iter()
                    .map(|value| value.map(Frame::bulk_string).unwrap_or(Frame::Null))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_multi_get_async(&self.key, &self.fields).await {
            Ok(values) => Ok(Frame::Array(
                values
                    .into_iter()
                    .map(|value| value.map(Frame::bulk_string).unwrap_or(Frame::Null))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
