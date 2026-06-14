use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Hincrby {
    key: String,
    field: String,
    increment: i64,
}

impl Hincrby {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hincrby' command",
            ));
        }

        let increment = args[3]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        Ok(Hincrby {
            key: args[1].to_string(),
            field: args[2].to_string(),
            increment,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_increment_by(&self.key, &self.field, self.increment) {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_increment_by_async(&self.key, &self.field, self.increment)
            .await
        {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
