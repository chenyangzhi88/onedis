use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zincrby {
    key: String,
    increment: f64,
    member: String,
}

impl Zincrby {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zincrby' command",
            ));
        }

        let increment = args[2]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR value is not a valid float"))?;
        if increment.is_nan() {
            return Err(Error::msg("ERR value is not a valid float"));
        }

        Ok(Zincrby {
            key: args[1].to_string(),
            increment,
            member: args[3].to_string(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_increment_by(&self.key, &self.member, self.increment) {
            Ok(score) => Ok(Frame::bulk_string(score.to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_increment_by_async(&self.key, &self.member, self.increment)
            .await
        {
            Ok(score) => Ok(Frame::bulk_string(score.to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
