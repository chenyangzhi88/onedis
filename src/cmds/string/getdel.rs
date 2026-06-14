use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct GetDel {
    key: String,
}

impl GetDel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getdel' command",
            ));
        }

        Ok(GetDel {
            key: args[1].to_string(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_bytes(&self.key)? {
            Some(value) => {
                db.delete_key(&self.key);
                Ok(Frame::bulk_string(value))
            }
            None => Ok(Frame::Null),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_bytes_async(&self.key).await? {
            Some(value) => {
                db.delete_key_async(&self.key).await;
                Ok(Frame::bulk_string(value))
            }
            None => Ok(Frame::Null),
        }
    }
}
