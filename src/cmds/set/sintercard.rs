use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Sintercard {
    keys: Vec<String>,
    limit: usize,
}

impl Sintercard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sintercard' command",
            ));
        }
        let numkeys = args[1]
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if args.len() < 2 + numkeys {
            return Err(Error::msg("ERR syntax error"));
        }
        let mut idx = 2 + numkeys;
        let mut limit = 0usize;
        while idx < args.len() {
            match args[idx].to_ascii_uppercase().as_str() {
                "LIMIT" => {
                    limit = args
                        .get(idx + 1)
                        .ok_or_else(|| Error::msg("ERR syntax error"))?
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            keys: args[2..2 + numkeys].to_vec(),
            limit,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_intersection_card(&self.keys, self.limit) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_intersection_async(&self.keys).await {
            Ok(intersection) => {
                let count = if self.limit > 0 {
                    intersection.len().min(self.limit)
                } else {
                    intersection.len()
                };
                Ok(Frame::Integer(count as i64))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
