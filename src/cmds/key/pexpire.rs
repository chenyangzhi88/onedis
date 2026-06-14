use anyhow::Error;

use crate::{
    cmds::key::expire::parse_expire_condition,
    frame::Frame,
    store::db::{Db, ExpireCondition},
};

pub struct Pexpire {
    key: String,
    ttl: u64,
    condition: ExpireCondition,
}

impl Pexpire {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pexpire' command",
            ));
        }

        let key = args[1].to_string();

        let ttl = match args[2].parse::<i64>() {
            Ok(val) if val <= 0 => 0,
            Ok(val) => val as u64,
            Err(_) => {
                return Err(Error::msg("ERR value is not an integer or out of range"));
            }
        };

        let condition = parse_expire_condition(&args, 3, "pexpire")?;

        Ok(Pexpire {
            key,
            ttl,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let changed = db.expire_with_condition(self.key, self.ttl, self.condition);
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let changed = db
            .expire_with_condition_async(self.key, self.ttl, self.condition)
            .await;
        Ok(Frame::Integer(if changed { 1 } else { 0 }))
    }
}
