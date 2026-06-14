use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Rpoplpush {
    source: String,
    destination: String,
}

impl Rpoplpush {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'rpoplpush' command",
            ));
        }

        Ok(Self {
            source: frame.get_arg(1).unwrap(),
            destination: frame.get_arg(2).unwrap(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_move(&self.source, &self.destination, false, true) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_move_async(&self.source, &self.destination, false, true)
            .await
        {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
