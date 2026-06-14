use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Incrby {
    pub key: String,
    pub increment: i64,
}

impl Incrby {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'incrby' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let increment = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        Ok(Incrby { key, increment })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.increment_integer_string(&self.key, self.increment) {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .increment_integer_string_async(&self.key, self.increment)
            .await
        {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
