use anyhow::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    cmds::key::expire::parse_expire_condition,
    frame::Frame,
    store::db::{Db, ExpireCondition},
};

pub struct PexpireAt {
    key: String,
    timestamp: i64,
    condition: ExpireCondition,
}

impl PexpireAt {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pexpireat' command",
            ));
        }

        let key = args[1].to_string();
        let timestamp = match args[2].parse::<i64>() {
            Ok(val) => val,
            Err(_) => {
                return Err(Error::msg("ERR value is not an integer or out of range"));
            }
        };
        let condition = parse_expire_condition(&args, 3, "pexpireat")?;
        Ok(PexpireAt {
            key,
            timestamp,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let ttl = expiration_ttl_ms(self.timestamp)?;
        let changed = db.expire_with_condition(self.key, ttl, self.condition);
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let ttl = expiration_ttl_ms(self.timestamp)?;
        let changed = db
            .expire_with_condition_async(self.key, ttl, self.condition)
            .await;
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }
}

fn expiration_ttl_ms(timestamp_ms: i64) -> Result<u64, Error> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    if timestamp_ms <= 0 || timestamp_ms as u128 <= now_ms {
        return Ok(0);
    }
    u64::try_from(timestamp_ms as u128 - now_ms)
        .map_err(|_| Error::msg("ERR invalid expire time in 'pexpireat' command"))
}
