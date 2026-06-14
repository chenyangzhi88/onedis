use anyhow::Error;

use crate::{cmds::listing::blpop::parse_blocking_pop, frame::Frame, store::db::Db};

pub struct Brpop {
    pub(crate) inner: crate::cmds::listing::blpop::Blpop,
}

impl Brpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            inner: parse_blocking_pop(frame, false, "brpop")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply_async(db).await
    }
}
