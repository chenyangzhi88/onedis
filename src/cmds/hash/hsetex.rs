use anyhow::Error;

use crate::{
    cmds::hash::common::parse_expire_update_value,
    frame::Frame,
    store::{db::Db, db::StringExpireUpdate},
};

pub struct Hsetex {
    key: String,
    fields: Vec<(String, Vec<u8>)>,
    expiration: Option<StringExpireUpdate>,
    keep_ttl: bool,
    fnx: bool,
    fxx: bool,
}

impl Hsetex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hsetex' command",
            ));
        }
        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let mut idx = 2;
        let mut fnx = false;
        let mut fxx = false;
        let mut keep_ttl = false;
        let mut expiration = None;
        while idx < frame.arg_len() {
            let option = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .to_ascii_uppercase();
            match option.as_str() {
                "FIELDS" => break,
                "FNX" => {
                    if fnx || fxx {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    fnx = true;
                    idx += 1;
                }
                "FXX" => {
                    if fnx || fxx {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    fxx = true;
                    idx += 1;
                }
                "KEEPTTL" => {
                    if keep_ttl || expiration.is_some() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    keep_ttl = true;
                    idx += 1;
                }
                "PERSIST" => return Err(Error::msg("ERR syntax error")),
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    if keep_ttl || expiration.is_some() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let value = frame
                        .get_arg(idx + 1)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?;
                    expiration = Some(parse_expire_update_value(&option, &value)?);
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        if !frame
            .get_arg(idx)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("FIELDS"))
        {
            return Err(Error::msg("ERR syntax error"));
        }
        let count = frame
            .get_arg(idx + 1)
            .ok_or_else(|| Error::msg("ERR syntax error"))?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if count == 0 {
            return Err(Error::msg("ERR numfields should be greater than 0"));
        }
        let fields_start = idx + 2;
        let values_len = count
            .checked_mul(2)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        let values_end = fields_start
            .checked_add(values_len)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        if frame.arg_len() != values_end {
            return Err(Error::msg("ERR syntax error"));
        }
        let mut fields = Vec::with_capacity(count);
        for field_idx in (fields_start..values_end).step_by(2) {
            let field = frame
                .get_arg(field_idx)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 hash field"))?;
            let value = frame
                .get_arg_bytes(field_idx + 1)
                .ok_or_else(|| Error::msg("ERR invalid hash value"))?;
            fields.push((field, value));
        }
        Ok(Self {
            key,
            fields,
            expiration,
            keep_ttl,
            fnx,
            fxx,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_ex_bytes(
            &self.key,
            &self.fields,
            self.expiration,
            self.keep_ttl,
            self.fnx,
            self.fxx,
        ) {
            Ok(changed) => Ok(Frame::Integer(if changed { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_set_ex_bytes_async(
                &self.key,
                &self.fields,
                self.expiration,
                self.keep_ttl,
                self.fnx,
                self.fxx,
            )
            .await
        {
            Ok(changed) => Ok(Frame::Integer(if changed { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
