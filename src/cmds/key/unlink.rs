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
        let deleted = self
            .keys
            .into_iter()
            .filter(|key| db.delete_key(key))
            .count() as i64;
        Ok(Frame::Integer(deleted))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut deleted = 0i64;
        for key in self.keys {
            if db.delete_key_async(&key).await {
                deleted += 1;
            }
        }
        Ok(Frame::Integer(deleted))
    }
}
