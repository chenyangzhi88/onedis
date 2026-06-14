use anyhow::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    cmds::key::expire::parse_expire_condition,
    frame::Frame,
    store::db::{Db, ExpireCondition},
};

pub struct ExpireAt {
    key: String,
    timestamp: i64,
    condition: ExpireCondition,
}

impl ExpireAt {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'expireat' command",
            ));
        }

        let key = args[1].to_string();
        let timestamp = match args[2].parse::<i64>() {
            Ok(val) => val,
            Err(_) => {
                return Err(Error::msg("ERR value is not an integer or out of range"));
            }
        };
        let condition = parse_expire_condition(&args, 3, "expireat")?;
        Ok(ExpireAt {
            key,
            timestamp,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;
        let ttl = if self.timestamp > now {
            ((self.timestamp - now) as u64)
                .checked_mul(1000)
                .ok_or_else(|| Error::msg("ERR invalid expire time in 'expireat' command"))?
        } else {
            0
        };
        let changed = db.expire_with_condition(self.key, ttl, self.condition);
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;
        let ttl = if self.timestamp > now {
            ((self.timestamp - now) as u64)
                .checked_mul(1000)
                .ok_or_else(|| Error::msg("ERR invalid expire time in 'expireat' command"))?
        } else {
            0
        };
        let changed = db
            .expire_with_condition_async(self.key, ttl, self.condition)
            .await;
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }
}
