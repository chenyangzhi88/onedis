use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hset {
    pub key: String,
    pub fields: Vec<(String, String)>,
}

impl Hset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || frame.arg_len() % 2 != 0 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hset' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let mut fields = Vec::with_capacity((frame.arg_len() - 2) / 2);
        for idx in (2..frame.arg_len()).step_by(2) {
            let field = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 hash field"))?;
            let value = frame
                .get_arg(idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 hash value"))?;
            fields.push((field, value));
        }

        Ok(Hset { key, fields })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if let [(field, value)] = self.fields.as_slice() {
            return match db.hash_set(&self.key, field, value) {
                Ok(added) => Ok(Frame::Integer(i64::from(added))),
                Err(err) => Ok(Frame::Error(err.to_string())),
            };
        }

        match db.hash_set_many(&self.key, &self.fields) {
            Ok(added) => Ok(Frame::Integer(added as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if let [(field, value)] = self.fields.as_slice() {
            return match db.hash_set_async(&self.key, field, value).await {
                Ok(added) => Ok(Frame::Integer(i64::from(added))),
                Err(err) => Ok(Frame::Error(err.to_string())),
            };
        }

        match db.hash_set_many_async(&self.key, &self.fields).await {
            Ok(added) => Ok(Frame::Integer(added as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
