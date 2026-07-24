use std::collections::HashMap;

use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hmset {
    pub key: String,
    pub fields: HashMap<String, String>,
}

impl Hmset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);

        if key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hmset' command",
            ));
        }

        let args = frame.get_args();

        if args.len() < 4 || !args.len().is_multiple_of(2) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hmset' command",
            ));
        }

        let mut fields = HashMap::new();

        for i in (2..args.len()).step_by(2) {
            let field = args[i].to_string();
            let value = args[i + 1].to_string();
            fields.insert(field, value);
        }

        Ok(Hmset {
            key: key.unwrap().to_string(),
            fields,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_multi_set(&self.key, &self.fields) {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_multi_set_async(&self.key, &self.fields).await {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
