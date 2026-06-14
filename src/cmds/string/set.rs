use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration, SetOutcome},
};

pub struct Set {
    pub key: String,
    pub val: Vec<u8>,
    pub expiration: SetExpiration,
    pub condition: SetCondition,
    pub return_old: bool,
}

impl Set {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let fianl_key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'set' command"))?;
        let final_val = frame
            .get_arg_bytes(2)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'set' command"))?;

        if frame.arg_len() <= 3 {
            return Ok(Set {
                key: fianl_key,
                val: final_val,
                expiration: SetExpiration::Clear,
                condition: SetCondition::Always,
                return_old: false,
            });
        }

        let mut expiration = SetExpiration::Clear;
        let mut has_expiration = false;
        let mut condition = SetCondition::Always;
        let mut return_old = false;
        let mut idx = 3;
        while idx < frame.arg_len() {
            let Some(option) = frame.get_arg(idx) else {
                return Err(Error::msg("ERR syntax error"));
            };
            match option.to_ascii_uppercase().as_str() {
                "NX" => {
                    if condition != SetCondition::Always {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    condition = SetCondition::Nx;
                    idx += 1;
                }
                "XX" => {
                    if condition != SetCondition::Always {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    condition = SetCondition::Xx;
                    idx += 1;
                }
                "GET" => {
                    return_old = true;
                    idx += 1;
                }
                "KEEPTTL" => {
                    if has_expiration {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    expiration = SetExpiration::KeepTtl;
                    has_expiration = true;
                    idx += 1;
                }
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    if has_expiration || idx + 1 >= frame.arg_len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let ttl_arg = frame
                        .get_arg(idx + 1)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    expiration = parse_set_expiration(&option, &ttl_arg)?;
                    has_expiration = true;
                    idx += 2;
                }
                _ => {
                    return Err(Error::msg("ERR syntax error"));
                }
            }
        }

        Ok(Set {
            key: fianl_key,
            val: final_val,
            expiration,
            condition,
            return_old,
        })
    }

    pub fn new(key: String, val: String, ttl: Option<u64>) -> Self {
        Set {
            key,
            val: val.into_bytes(),
            expiration: ttl
                .map(|ttl| SetExpiration::At(now_ms().saturating_add(ttl)))
                .unwrap_or(SetExpiration::Clear),
            condition: SetCondition::Always,
            return_old: false,
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let return_old = self.return_old;
        match db.set_string_bytes(
            self.key,
            self.val,
            self.expiration,
            self.condition,
            return_old,
        )? {
            SetOutcome::Set { old_value } => {
                if return_old {
                    Ok(old_value.map_or(Frame::Null, Frame::bulk_string))
                } else {
                    Ok(Frame::Ok)
                }
            }
            SetOutcome::NotSet => Ok(Frame::Null),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let return_old = self.return_old;
        match db
            .set_string_bytes_async(
                self.key,
                self.val,
                self.expiration,
                self.condition,
                return_old,
            )
            .await?
        {
            SetOutcome::Set { old_value } => {
                if return_old {
                    Ok(old_value.map_or(Frame::Null, Frame::bulk_string))
                } else {
                    Ok(Frame::Ok)
                }
            }
            SetOutcome::NotSet => Ok(Frame::Null),
        }
    }
}

fn parse_set_expiration(option: &str, value: &str) -> Result<SetExpiration, Error> {
    let value = value
        .parse::<u64>()
        .map_err(|_| Error::msg("ERR invalid expire time in 'set' command"))?;
    if value == 0 && matches!(option.to_ascii_uppercase().as_str(), "EX" | "PX") {
        return Err(Error::msg("ERR invalid expire time in 'set' command"));
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
