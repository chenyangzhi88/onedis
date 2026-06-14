use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zcard {
    pub key: String,
}

impl Zcard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zcard' command",
            ));
        }
        let key = args[1].to_string(); // 键
        Ok(Zcard { key })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_card(&self.key) {
            Ok(card) => Ok(Frame::Integer(card as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_card_async(&self.key).await {
            Ok(card) => Ok(Frame::Integer(card as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
