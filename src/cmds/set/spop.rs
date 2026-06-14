use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Spop {
    pub key: String,
    pub count: Option<usize>,
}

impl Spop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() < 2 || args.len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'spop' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let count = if args.len() == 3 {
            match args[2].parse::<usize>() {
                Ok(c) => Some(c),
                Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
            }
        } else {
            None
        };

        Ok(Spop { key, count })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let pop_count = self.count.unwrap_or(1);
        match db.set_pop(&self.key, pop_count) {
            Ok(mut popped) => {
                if self.count.is_none() {
                    Ok(popped.pop().map(Frame::bulk_string).unwrap_or(Frame::Null))
                } else {
                    Ok(Frame::Array(
                        popped.into_iter().map(Frame::bulk_string).collect(),
                    ))
                }
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let pop_count = self.count.unwrap_or(1);
        match db.set_pop_async(&self.key, pop_count).await {
            Ok(mut popped) => {
                if self.count.is_none() {
                    Ok(popped.pop().map(Frame::bulk_string).unwrap_or(Frame::Null))
                } else {
                    Ok(Frame::Array(
                        popped.into_iter().map(Frame::bulk_string).collect(),
                    ))
                }
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
