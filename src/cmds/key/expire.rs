use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, ExpireCondition},
};

pub struct Expire {
    key: String,
    ttl: u64,
    condition: ExpireCondition,
}

impl Expire {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'expire' command",
            ));
        }

        let key = args[1].to_string();

        let ttl = match args[2].parse::<i64>() {
            Ok(val) if val <= 0 => 0,
            Ok(val) => (val as u64)
                .checked_mul(1000)
                .ok_or_else(|| Error::msg("ERR invalid expire time in 'expire' command"))?,
            Err(_) => {
                return Err(Error::msg("ERR value is not an integer or out of range"));
            }
        };

        let condition = parse_expire_condition(&args, 3, "expire")?;

        Ok(Expire {
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

pub(crate) fn parse_expire_condition(
    args: &[String],
    start_idx: usize,
    command_name: &str,
) -> Result<ExpireCondition, Error> {
    let mut condition = ExpireCondition::Always;
    for option in args.iter().skip(start_idx) {
        let next = match option.to_ascii_uppercase().as_str() {
            "NX" => ExpireCondition::Nx,
            "XX" => ExpireCondition::Xx,
            "GT" => ExpireCondition::Gt,
            "LT" => ExpireCondition::Lt,
            _ => {
                return Err(Error::msg(format!(
                    "ERR unsupported option for '{command_name}' command"
                )));
            }
        };
        if condition != ExpireCondition::Always {
            return Err(Error::msg(format!(
                "ERR NX, XX, GT, and LT options at the same time are not compatible for '{command_name}' command"
            )));
        }
        condition = next;
    }
    Ok(condition)
}
