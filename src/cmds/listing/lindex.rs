use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Lindex {
    key: String,
    index: i64,
}

impl Lindex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lindex' command",
            ));
        }
        let final_key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'lindex' command"))?;
        let final_index = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        Ok(Lindex {
            key: final_key,
            index: final_index,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_index(&self.key, self.index) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_index_async(&self.key, self.index).await {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
