use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Touch {
    keys: Vec<String>,
}

impl Touch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = frame.get_args_from_index(1);
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'touch' command",
            ));
        }
        Ok(Touch { keys })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let count = self.keys.into_iter().filter(|key| db.touch(key)).count() as i64;
        Ok(Frame::Integer(count))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut count = 0i64;
        for key in self.keys {
            if db.touch_async(&key).await {
                count += 1;
            }
        }
        Ok(Frame::Integer(count))
    }
}
