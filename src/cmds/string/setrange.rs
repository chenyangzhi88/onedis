use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration},
};

pub struct SetRange {
    pub key: String,
    pub offset: i64,
    pub value: Vec<u8>,
}

impl SetRange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let offset = frame.get_arg(2);
        if frame.arg_len() != 4 || key.is_none() || offset.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setrange' command",
            ));
        }

        let final_key = key.unwrap().to_string();
        let final_offset = offset.unwrap().to_string();
        let final_value = frame
            .get_arg_bytes(3)
            .ok_or_else(|| Error::msg("ERR missing value"))?;

        let offset_int = match final_offset.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

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
        let mut bytes = db.get_string_bytes(&self.key)?.unwrap_or_default();
        if self.value.is_empty() {
            return Ok(Frame::Integer(bytes.len() as i64));
        }
        let offset = usize::try_from(self.offset)
            .map_err(|_| Error::msg("ERR offset is out of range, must be positive"))?;
        let required_len = offset
            .checked_add(self.value.len())
            .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
        if required_len > bytes.len() {
            bytes
                .try_reserve_exact(required_len - bytes.len())
                .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
            bytes.resize(required_len, 0);
        }
        bytes[offset..required_len].copy_from_slice(&self.value);
        let length = bytes.len();
        db.set_string_bytes(
            self.key,
            bytes,
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )?;
        Ok(Frame::Integer(length as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let offset = self.offset as usize;
        let value = self.value;
        let length = db
            .mutate_string_bytes_async(&self.key, |bytes, _| {
                if value.is_empty() {
                    return Ok(bytes.len());
                }
                let required_len = offset
                    .checked_add(value.len())
                    .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
                if required_len > bytes.len() {
                    bytes
                        .try_reserve_exact(required_len - bytes.len())
                        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                    bytes.resize(required_len, 0);
                }
                bytes[offset..required_len].copy_from_slice(&value);
                Ok(bytes.len())
            })
            .await?;

        Ok(Frame::Integer(length as i64))
    }
}
