use anyhow::Error;

use crate::{
    cmds::hash::common::{parse_expire_condition, parse_hash_fields},
    frame::Frame,
    store::db::Db,
};

pub struct Hexpire {
    key: String,
    expire_ms: u64,
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
        let mut value = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if value < 0 {
            value = 0;
        }
        let expire_ms = if absolute {
            if millis {
                value as u64
            } else {
                (value as u64).saturating_mul(1000)
            }
        } else if millis {
            crate::cmds::hash::common::now_ms().saturating_add(value as u64)
        } else {
            crate::cmds::hash::common::now_ms().saturating_add((value as u64).saturating_mul(1000))
        };
        let mut idx = 3;
        let condition = parse_expire_condition(&args, &mut idx)?;
        let fields = parse_hash_fields(&args, idx)?;
        Ok(Self {
            key: args[1].clone(),
            expire_ms,
            fields,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_expire_fields_at_ms(&self.key, self.expire_ms, &self.fields, self.condition) {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_expire_fields_at_ms_async(&self.key, self.expire_ms, &self.fields, self.condition)
            .await
        {
            Ok(values) => Ok(Frame::Array(
                values.into_iter().map(Frame::Integer).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
