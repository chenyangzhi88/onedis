use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hmset {
    pub key: String,
    pub fields: Vec<(String, Vec<u8>)>,
}

impl Hmset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || !frame.arg_len().is_multiple_of(2) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hmset' command",
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
                .get_arg_bytes(idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid hash value"))?;
            fields.push((field, value));
        }

        Ok(Hmset { key, fields })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_many_bytes(&self.key, &self.fields) {
            Ok(_) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_many_bytes_async(&self.key, &self.fields).await {
            Ok(_) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
