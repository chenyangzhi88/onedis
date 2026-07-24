use crate::{
    frame::Frame,
    store::db::{Db, Structure},
};
use anyhow::Error;

pub struct Append {
    pub key: String,
    pub val: Vec<u8>,
}

impl Append {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'append' command",
            ));
        }

        Ok(Append {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            val: frame
                .get_arg_bytes(2)
                .ok_or_else(|| Error::msg("ERR missing value"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let existing_value = match db.get(&self.key) {
            Some(Structure::String(s)) => s,
            Some(_) => return Err(Error::msg("ERR wrong type for 'append' command")),
            None => String::new(),
        };
        let suffix =
            String::from_utf8(self.val).map_err(|_| Error::msg("ERR value is not valid UTF-8"))?;
        let new_value = format!("{existing_value}{suffix}");
        let len = new_value.len();
        db.insert(self.key, Structure::String(new_value));
        Ok(Frame::Integer(len as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let suffix = self.val;
        let len = db
            .mutate_string_bytes_async(&self.key, |value, _| {
                value
                    .try_reserve(suffix.len())
                    .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                value.extend_from_slice(&suffix);
                Ok(value.len())
            })
            .await?;
        Ok(Frame::Integer(len as i64))
    }
}
