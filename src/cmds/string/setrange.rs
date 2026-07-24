use anyhow::Error;

use crate::{
    frame::{Frame, MAX_BULK_STRING_BYTES},
    store::db::Db,
};

pub struct SetRange {
    pub key: String,
    pub offset: i64,
    pub value: Vec<u8>,
}

impl SetRange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setrange' command",
            ));
        }
        let final_key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'setrange' command"))?;
        let final_value = frame
            .get_arg_bytes(3)
            .ok_or_else(|| Error::msg("ERR missing value"))?;
        let offset_int = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        if offset_int < 0 {
            return Err(Error::msg("ERR offset is out of range, must be positive"));
        }

        Ok(SetRange {
            key: final_key,
            offset: offset_int,
            value: final_value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let offset = usize::try_from(self.offset)
            .map_err(|_| Error::msg("ERR offset is out of range, must be positive"))?;
        let value = self.value;
        let length = db.mutate_string_bytes_if_changed(&self.key, |bytes, _| {
            if value.is_empty() {
                return Ok((bytes.len(), false));
            }
            let required_len = offset
                .checked_add(value.len())
                .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
            if required_len > bytes.len() {
                bytes
                    .try_reserve_exact(required_len - bytes.len())
                    .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                bytes.resize(required_len, 0);
            }
            bytes[offset..required_len].copy_from_slice(&value);
            Ok((bytes.len(), true))
        })?;
        Ok(Frame::Integer(length as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let offset = usize::try_from(self.offset)
            .map_err(|_| Error::msg("ERR offset is out of range, must be positive"))?;
        let value = self.value;
        let length = db
            .mutate_string_bytes_if_changed_async(&self.key, |bytes, _| {
                if value.is_empty() {
                    return Ok((bytes.len(), false));
                }
                let required_len = offset
                    .checked_add(value.len())
                    .filter(|len| *len <= MAX_BULK_STRING_BYTES)
                    .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
                if required_len > bytes.len() {
                    bytes
                        .try_reserve_exact(required_len - bytes.len())
                        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                    bytes.resize(required_len, 0);
                }
                bytes[offset..required_len].copy_from_slice(&value);
                Ok((bytes.len(), true))
            })
            .await?;

        Ok(Frame::Integer(length as i64))
    }
}
