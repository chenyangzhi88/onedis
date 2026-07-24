use crate::{
    frame::{Frame, MAX_BULK_STRING_BYTES},
    store::db::Db,
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
        let suffix = self.val;
        let len = db.mutate_string_bytes(&self.key, |value, _| {
            let required_len = value
                .len()
                .checked_add(suffix.len())
                .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
            value
                .try_reserve(suffix.len())
                .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
            value.extend_from_slice(&suffix);
            Ok(required_len)
        })?;
        Ok(Frame::Integer(len as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let suffix = self.val;
        let len = db
            .mutate_string_bytes_async(&self.key, |value, _| {
                let required_len = value
                    .len()
                    .checked_add(suffix.len())
                    .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                    .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
                value
                    .try_reserve(suffix.len())
                    .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                value.extend_from_slice(&suffix);
                Ok(required_len)
            })
            .await?;
        Ok(Frame::Integer(len as i64))
    }
}
