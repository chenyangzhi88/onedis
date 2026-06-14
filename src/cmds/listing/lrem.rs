use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Lrem {
    key: String,
    count: i64,
    element: String,
}

impl Lrem {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lrem' command",
            ));
        }
        let count = args[2]
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        Ok(Self {
            key: args[1].clone(),
            count,
            element: args[3].clone(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_remove(&self.key, self.count, &self.element) {
            Ok(removed) => Ok(Frame::Integer(removed as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_remove_async(&self.key, self.count, &self.element)
            .await
        {
            Ok(removed) => Ok(Frame::Integer(removed as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
