use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StringExpireUpdate},
};

pub struct GetEx {
    key: String,
    expiration: Option<StringExpireUpdate>,
}

impl GetEx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 && frame.arg_len() != 3 && frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getex' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let expiration = if frame.arg_len() == 2 {
            None
        } else {
            let option = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR syntax error"))?;
            Some(parse_getex_expiration(&option, frame.get_arg(3))?)
        };

        Ok(GetEx { key, expiration })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.getex_string_bytes(&self.key, self.expiration)? {
            Some(value) => Ok(Frame::bulk_string(value)),
            None => Ok(Frame::Null),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .getex_string_bytes_async(&self.key, self.expiration)
            .await?
        {
            Some(value) => Ok(Frame::bulk_string(value)),
            None => Ok(Frame::Null),
        }
    }
}

fn parse_getex_expiration(
    option: &str,
    value: Option<String>,
) -> Result<StringExpireUpdate, Error> {
    match option.to_ascii_uppercase().as_str() {
        "PERSIST" => {
            if value.is_some() {
                return Err(Error::msg("ERR syntax error"));
            }
            Ok(StringExpireUpdate::Persist)
        }
        "EX" | "PX" | "EXAT" | "PXAT" => {
            let value = value
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid expire time in 'getex' command"))?;
            if value == 0 && matches!(option.to_ascii_uppercase().as_str(), "EX" | "PX") {
                return Err(Error::msg("ERR invalid expire time in 'getex' command"));
            }
            Ok(match option.to_ascii_uppercase().as_str() {
                "EX" => StringExpireUpdate::RelativeMs(value.saturating_mul(1000)),
                "PX" => StringExpireUpdate::RelativeMs(value),
                "EXAT" => StringExpireUpdate::AbsoluteMs(value.saturating_mul(1000)),
                "PXAT" => StringExpireUpdate::AbsoluteMs(value),
                _ => unreachable!(),
            })
        }
        _ => Err(Error::msg("ERR syntax error")),
    }
}
