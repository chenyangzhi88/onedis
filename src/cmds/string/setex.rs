use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Setex {
    key: String,
    seconds: u64,
    value: Vec<u8>,
}

impl Setex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setex' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let seconds = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR invalid expire time in 'setex' command"))?
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR invalid expire time in 'setex' command"))?;
        if seconds == 0 {
            return Err(Error::msg("ERR invalid expire time in 'setex' command"));
        }
        let value = frame
            .get_arg_bytes(3)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'setex' command"))?;

        Ok(Setex {
            key,
            seconds,
            value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let ttl_ms = self
            .seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::msg("ERR invalid expire time in 'setex' command"))?;
        db.insert_string_bytes(self.key, self.value, Some(ttl_ms));
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let ttl_ms = self
            .seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::msg("ERR invalid expire time in 'setex' command"))?;
        db.insert_string_bytes_async(self.key, self.value, Some(ttl_ms))
            .await?;
        Ok(Frame::Ok)
    }
}
