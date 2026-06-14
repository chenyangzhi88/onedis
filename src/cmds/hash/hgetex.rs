use anyhow::Error;

use crate::{
    cmds::hash::common::{parse_expire_update, parse_hash_fields},
    frame::Frame,
    store::{db::Db, db::StringExpireUpdate},
};

pub struct Hgetex {
    key: String,
    fields: Vec<String>,
    expiration: Option<StringExpireUpdate>,
}

impl Hgetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hgetex' command",
            ));
        }
        let mut idx = 2;
        let expiration = parse_expire_update(&args, &mut idx)?;
        let fields = parse_hash_fields(&args, idx)?;
        Ok(Self {
            key: args[1].clone(),
            fields,
            expiration,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_ex(&self.key, &self.fields, self.expiration) {
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
        match db
            .hash_get_ex_async(&self.key, &self.fields, self.expiration)
            .await
        {
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
