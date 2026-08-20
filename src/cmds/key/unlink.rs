use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Unlink {
    keys: Vec<String>,
}

impl Unlink {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = frame.get_args_from_index(1);
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'unlink' command",
            ));
        }
        Ok(Unlink { keys })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut deleted = 0i64;
        for key in self.keys {
            deleted += i64::from(db.delete_key(&key)?);
        }
        Ok(Frame::Integer(deleted))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            db.delete_keys_async(&self.keys).await? as i64,
        ))
    }
}
