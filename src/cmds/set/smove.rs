use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Smove {
    source: String,
    destination: String,
    member: String,
}

impl Smove {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'smove' command",
            ));
        }
        Ok(Self {
            source: args[1].clone(),
            destination: args[2].clone(),
            member: args[3].clone(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_move(&self.source, &self.destination, &self.member) {
            Ok(moved) => Ok(Frame::Integer(if moved { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .set_move_async(&self.source, &self.destination, &self.member)
            .await
        {
            Ok(moved) => Ok(Frame::Integer(if moved { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
