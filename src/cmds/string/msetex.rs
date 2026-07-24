use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration},
};

pub struct Msetex {
    items: Vec<(String, Vec<u8>)>,
    expiration: SetExpiration,
}

impl Msetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'msetex' command",
            ));
        }

        let option = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR syntax error"))?;
        let value = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR syntax error"))?;
        let expiration = parse_expiration(&option, &value)?;
        if !(frame.arg_len() - 3).is_multiple_of(2) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'msetex' command",
            ));
        }

        let mut items = Vec::with_capacity((frame.arg_len() - 3) / 2);
        for idx in (3..frame.arg_len()).step_by(2) {
            let key = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
            let value = frame
                .get_arg_bytes(idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid bulk string value"))?;
            items.push((key, value));
        }
        Ok(Self { items, expiration })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        for (key, value) in self.items {
            db.set_string_bytes(key, value, self.expiration, SetCondition::Always, false)?;
        }
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        for (key, value) in self.items {
            db.set_string_bytes_async(key, value, self.expiration, SetCondition::Always, false)
                .await?;
        }
        Ok(Frame::Ok)
    }
}

fn parse_expiration(option: &str, value: &str) -> Result<SetExpiration, Error> {
    let value = value
        .parse::<u64>()
        .map_err(|_| Error::msg("ERR invalid expire time in 'msetex' command"))?;
    if value == 0 && matches!(option.to_ascii_uppercase().as_str(), "EX" | "PX") {
        return Err(Error::msg("ERR invalid expire time in 'msetex' command"));
    }
    let expire_ms = match option.to_ascii_uppercase().as_str() {
        "EX" => now_ms().saturating_add(value.saturating_mul(1000)),
        "PX" => now_ms().saturating_add(value),
        "EXAT" => value.saturating_mul(1000),
        "PXAT" => value,
        _ => return Err(Error::msg("ERR syntax error")),
    };
    Ok(SetExpiration::At(expire_ms))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
