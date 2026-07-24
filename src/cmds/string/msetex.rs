use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration},
};

pub struct Msetex {
    items: Vec<(String, Vec<u8>)>,
    expiration: MsetexExpiration,
    condition: SetCondition,
}

#[derive(Clone, Copy)]
enum MsetexExpiration {
    Clear,
    KeepTtl,
    RelativeMs(u64),
    AbsoluteMs(u64),
}

impl Msetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(wrong_arity());
        }
        let numkeys = frame
            .get_arg(1)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if numkeys == 0 {
            return Err(Error::msg("ERR numkeys should be greater than 0"));
        }
        let data_len = numkeys
            .checked_mul(2)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        let options_start = 2usize
            .checked_add(data_len)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        if frame.arg_len() < options_start {
            return Err(wrong_arity());
        }

        let mut items = Vec::with_capacity(numkeys);
        for idx in (2..options_start).step_by(2) {
            let key = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
            let value = frame
                .get_arg_bytes(idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid bulk string value"))?;
            items.push((key, value));
        }

        let mut condition = SetCondition::Always;
        let mut expiration = MsetexExpiration::Clear;
        let mut expiration_seen = false;
        let mut idx = options_start;
        while idx < frame.arg_len() {
            let option = frame.get_arg(idx).unwrap().to_ascii_uppercase();
            match option.as_str() {
                "NX" | "XX" => {
                    if condition != SetCondition::Always {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    condition = if option == "NX" {
                        SetCondition::Nx
                    } else {
                        SetCondition::Xx
                    };
                    idx += 1;
                }
                "KEEPTTL" => {
                    if expiration_seen {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    expiration = MsetexExpiration::KeepTtl;
                    expiration_seen = true;
                    idx += 1;
                }
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    if expiration_seen || idx + 1 >= frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    expiration = parse_expiration(&option, &frame.get_arg(idx + 1).unwrap())?;
                    expiration_seen = true;
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }

        Ok(Self {
            items,
            expiration,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let expiration = self.expiration.resolve()?;
        let written = db.set_string_bytes_many(self.items, expiration, self.condition)?;
        Ok(Frame::Integer(i64::from(written)))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let expiration = self.expiration.resolve()?;
        let written = db
            .set_string_bytes_many_async(self.items, expiration, self.condition)
            .await?;
        Ok(Frame::Integer(i64::from(written)))
    }
}

impl MsetexExpiration {
    fn resolve(self) -> Result<SetExpiration, Error> {
        match self {
            Self::Clear => Ok(SetExpiration::Clear),
            Self::KeepTtl => Ok(SetExpiration::KeepTtl),
            Self::RelativeMs(ttl_ms) => now_ms()
                .checked_add(ttl_ms)
                .map(SetExpiration::At)
                .ok_or_else(invalid_expiration),
            Self::AbsoluteMs(expire_ms) => Ok(SetExpiration::At(expire_ms)),
        }
    }
}

fn parse_expiration(option: &str, value: &str) -> Result<MsetexExpiration, Error> {
    let value = value.parse::<u64>().map_err(|_| invalid_expiration())?;
    match option {
        "EX" if value > 0 => value
            .checked_mul(1000)
            .map(MsetexExpiration::RelativeMs)
            .ok_or_else(invalid_expiration),
        "PX" if value > 0 => Ok(MsetexExpiration::RelativeMs(value)),
        "EXAT" => value
            .checked_mul(1000)
            .map(MsetexExpiration::AbsoluteMs)
            .ok_or_else(invalid_expiration),
        "PXAT" => Ok(MsetexExpiration::AbsoluteMs(value)),
        _ => Err(invalid_expiration()),
    }
}

fn wrong_arity() -> Error {
    Error::msg("ERR wrong number of arguments for 'msetex' command")
}

fn invalid_expiration() -> Error {
    Error::msg("ERR invalid expire time in 'msetex' command")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
