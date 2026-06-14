use anyhow::Error;

use crate::{cmds::hash::common::parse_hash_fields, frame::Frame, store::db::Db};

pub struct Httl {
    key: String,
    fields: Vec<String>,
    millis: bool,
    absolute: bool,
}

impl Httl {
    pub fn parse_from_frame(frame: Frame, millis: bool, absolute: bool) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for hash ttl command",
            ));
        }
        Ok(Self {
            key: args[1].clone(),
            fields: parse_hash_fields(&args, 2)?,
            millis,
            absolute,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_field_ttls(&self.key, &self.fields, self.millis, self.absolute) {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_field_ttls_async(&self.key, &self.fields, self.millis, self.absolute)
            .await
        {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
