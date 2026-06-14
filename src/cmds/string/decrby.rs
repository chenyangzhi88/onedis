use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Decrby {
    pub key: String,
    pub decrement: i64,
}

impl Decrby {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'decrby' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let decrement = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        Ok(Decrby { key, decrement })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.decrement.checked_neg() {
            Some(delta) => db.increment_integer_string(&self.key, delta),
            None => {
                db.update_integer_string(&self.key, |current| current.checked_sub(self.decrement))
            }
        };
        match result {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.decrement.checked_neg() {
            Some(delta) => db.increment_integer_string_async(&self.key, delta).await,
            None => {
                db.update_integer_string_async(&self.key, |current| {
                    current.checked_sub(self.decrement)
                })
                .await
            }
        };
        match result {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
