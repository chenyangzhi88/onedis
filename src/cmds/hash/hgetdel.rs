use anyhow::Error;

use crate::{cmds::hash::common::parse_hash_fields, frame::Frame, store::db::Db};

pub struct Hgetdel {
    key: String,
    fields: Vec<String>,
}

impl Hgetdel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hgetdel' command",
            ));
        }
        Ok(Self {
            key: args[1].clone(),
            fields: parse_hash_fields(&args, 2)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_del(&self.key, &self.fields) {
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
        match db.hash_get_del_async(&self.key, &self.fields).await {
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
