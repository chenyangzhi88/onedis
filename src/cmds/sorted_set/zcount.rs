use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zcount {
    key: String,
    min: f64,
    max: f64,
}

impl Zcount {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zcount' command",
            ));
        }
        let key = args[1].to_string(); // 键
        let min = args[2]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR min is not a valid float"))?;
        let max = args[3]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR max is not a valid float"))?;
        Ok(Zcount { key, min, max })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_count(&self.key, self.min, self.max) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_count_async(&self.key, self.min, self.max).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
