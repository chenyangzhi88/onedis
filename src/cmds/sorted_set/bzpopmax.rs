use anyhow::Error;

use crate::{
    cmds::sorted_set::bzpopmin::{Bzpopmin, parse_bzpop},
    frame::Frame,
    store::db::Db,
};

pub struct Bzpopmax {
    pub(crate) inner: Bzpopmin,
}

impl Bzpopmax {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            inner: parse_bzpop(frame, false, "bzpopmax")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply_async(db).await
    }
}
