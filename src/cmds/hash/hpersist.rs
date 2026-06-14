use anyhow::Error;

use crate::{cmds::hash::common::parse_hash_fields, frame::Frame, store::db::Db};

pub struct Hpersist {
    key: String,
    fields: Vec<String>,
}

impl Hpersist {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hpersist' command",
            ));
        }
        Ok(Self {
            key: args[1].clone(),
            fields: parse_hash_fields(&args, 2)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_persist_fields(&self.key, &self.fields) {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_persist_fields_async(&self.key, &self.fields).await {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
