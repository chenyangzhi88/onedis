use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Psetex {
    key: String,
    milliseconds: u64,
    value: Vec<u8>,
}

impl Psetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'psetex' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let milliseconds = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR invalid expire time in 'psetex' command"))?
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR invalid expire time in 'psetex' command"))?;
        if milliseconds == 0 {
            return Err(Error::msg("ERR invalid expire time in 'psetex' command"));
        }
        let value = frame
            .get_arg_bytes(3)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'psetex' command"))?;

        Ok(Psetex {
            key,
            milliseconds,
            value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.insert_string_bytes(self.key, self.value, Some(self.milliseconds));
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.insert_string_bytes_async(self.key, self.value, Some(self.milliseconds))
            .await;
        Ok(Frame::Ok)
    }
}
