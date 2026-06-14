use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Sdiffstore {
    destination: String,
    keys: Vec<String>,
}

impl Sdiffstore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiffstore' command",
            ));
        }

        Ok(Sdiffstore {
            destination: args[1].to_string(),
            keys: args[2..].iter().map(|arg| arg.to_string()).collect(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_diff_store(&self.destination, &self.keys) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_diff_store_async(&self.destination, &self.keys).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
