use anyhow::Error;

use crate::{cmds::sorted_set::zrangebyscore::Zrangebyscore, frame::Frame, store::db::Db};

pub struct Zrevrangebyscore {
    inner: Zrangebyscore,
}

impl Zrevrangebyscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            inner: Zrangebyscore::parse_reverse(frame)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply_async(db).await
    }
}
