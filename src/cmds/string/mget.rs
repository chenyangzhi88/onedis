use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Mget {
    keys: Vec<String>,
}

impl Mget {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'mget' command",
            ));
        }

        Ok(Mget { keys: args })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::new();
        for key in self.keys {
            match db.get_string_bytes(&key) {
                Ok(Some(value)) => result.push(Frame::bulk_string(value)),
                Ok(None) | Err(_) => result.push(Frame::Null),
            }
        }
        Ok(Frame::Array(result))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::new();
        for key in self.keys {
            match db.get_string_bytes_async(&key).await {
                Ok(Some(value)) => result.push(Frame::bulk_string(value)),
                Ok(None) | Err(_) => result.push(Frame::Null),
            }
        }
        Ok(Frame::Array(result))
    }
}
