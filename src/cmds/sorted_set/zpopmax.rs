use anyhow::Error;

use crate::{
    cmds::sorted_set::zpopmin::{Zpopmin, parse_zpop},
    frame::Frame,
    store::db::Db,
};

pub struct Zpopmax {
    inner: Zpopmin,
}

impl Zpopmax {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            inner: parse_zpop(frame, false, "zpopmax")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply_async(db).await
    }
}
