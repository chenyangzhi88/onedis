use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Srandmember {
    key: String,
    count: Option<i64>,
}

impl Srandmember {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 && args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'srandmember' command",
            ));
        }
        let count = if args.len() == 3 {
            Some(
                args[2]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            )
        } else {
            None
        };
        Ok(Srandmember {
            key: args[1].to_string(),
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_random_members(&self.key, self.count) {
            Ok(Some(members)) if self.count.is_none() => Ok(Frame::bulk_string(
                members.into_iter().next().unwrap_or_default(),
            )),
            Ok(Some(members)) => Ok(Frame::Array(
                members.into_iter().map(Frame::bulk_string).collect(),
            )),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_random_members_async(&self.key, self.count).await {
            Ok(Some(members)) if self.count.is_none() => Ok(Frame::bulk_string(
                members.into_iter().next().unwrap_or_default(),
            )),
            Ok(Some(members)) => Ok(Frame::Array(
                members.into_iter().map(Frame::bulk_string).collect(),
            )),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
