use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Sinter {
    keys: Vec<String>,
}

impl Sinter {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        }
        let keys: Vec<String> = args[1..].iter().map(|arg| arg.to_string()).collect();
        Ok(Sinter { keys })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_intersection(&self.keys) {
            Ok(intersection) => Ok(Frame::Array(
                intersection.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_intersection_async(&self.keys).await {
            Ok(intersection) => Ok(Frame::Array(
                intersection.into_iter().map(Frame::bulk_string).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
