use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zadd {
    pub key: String,
    pub members: Vec<(f64, String)>, // 成员及其分数
}

impl Zadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 || args.len() % 2 != 0 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zadd' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let mut members = Vec::new();

        for chunk in args[2..].chunks(2) {
            if chunk.len() != 2 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'zadd' command",
                ));
            }
            let score = chunk[0]
                .parse::<f64>()
                .map_err(|_| Error::msg("ERR score is not a valid float"))?;
            let member = chunk[1].to_string();
            members.push((score, member));
        }

        Ok(Zadd { key, members })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_add(&self.key, &self.members) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_add_async(&self.key, &self.members).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
