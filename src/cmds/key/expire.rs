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
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for option in args.iter().skip(start_idx) {
        match option.to_ascii_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {
                return Err(Error::msg(format!(
                    "ERR unsupported option for '{command_name}' command"
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return Err(Error::msg(format!(
            "ERR NX and XX, GT or LT options at the same time are not compatible for '{command_name}' command"
        )));
    }
    if gt && lt {
        return Err(Error::msg(format!(
            "ERR GT and LT options at the same time are not compatible for '{command_name}' command"
        )));
    }
    Ok(match (nx, xx, gt, lt) {
        (true, false, false, false) => ExpireCondition::Nx,
        (false, true, true, false) => ExpireCondition::XxGt,
        (false, true, false, true) => ExpireCondition::XxLt,
        (false, true, false, false) => ExpireCondition::Xx,
        (false, false, true, false) => ExpireCondition::Gt,
        (false, false, false, true) => ExpireCondition::Lt,
        (false, false, false, false) => ExpireCondition::Always,
        _ => unreachable!("incompatible expiration options were rejected"),
    })
}
