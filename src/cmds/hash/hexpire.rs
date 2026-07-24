use anyhow::Error;

use crate::{
    cmds::hash::common::{parse_expire_condition, parse_hash_fields},
    frame::Frame,
    store::db::{Db, StringExpireUpdate},
};

pub struct Hexpire {
    key: String,
    expiration: StringExpireUpdate,
    fields: Vec<String>,
    condition: crate::store::db::ExpireCondition,
}

impl Hexpire {
    pub fn parse_from_frame(frame: Frame, millis: bool, absolute: bool) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for hash expire command",
            ));
        }
        let value = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if value < 0 {
            return Err(Error::msg("ERR invalid expire time in hash command"));
        }
        let expiration = if value == 0 {
            StringExpireUpdate::AbsoluteMs(0)
        } else {
            let mut value = u64::try_from(value)
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
            if !millis {
                value = value
                    .checked_mul(1000)
                    .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
            }
            if absolute {
                StringExpireUpdate::AbsoluteMs(value)
            } else {
                StringExpireUpdate::RelativeMs(value)
            }
        };
        let mut idx = 3;
        let condition = parse_expire_condition(&args, &mut idx)?;
        let fields = parse_hash_fields(&args, idx)?;
        Ok(Self {
            key: args[1].clone(),
            expiration,
            fields,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let expire_ms = resolve_expiration(self.expiration)?;
        match db.hash_expire_fields_at_ms(&self.key, expire_ms, &self.fields, self.condition) {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let expire_ms = resolve_expiration(self.expiration)?;
        match db
            .hash_expire_fields_at_ms_async(&self.key, expire_ms, &self.fields, self.condition)
            .await
        {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn resolve_expiration(expiration: StringExpireUpdate) -> Result<u64, Error> {
    let expire_ms = match expiration {
        StringExpireUpdate::RelativeMs(ttl_ms) => crate::cmds::hash::common::now_ms()
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range")),
        StringExpireUpdate::AbsoluteMs(expire_ms) => Ok(expire_ms),
        StringExpireUpdate::Persist => unreachable!(),
    }?;
    if expire_ms > crate::store::db::HASH_FIELD_MAX_EXPIRE_MS {
        return Err(Error::msg("ERR invalid expire time in hash command"));
    }
    Ok(expire_ms)
}
