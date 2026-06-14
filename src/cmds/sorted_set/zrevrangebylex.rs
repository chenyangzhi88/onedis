use anyhow::Error;

use crate::{
    cmds::sorted_set::zrangebylex::{Zrangebylex, parse_range_lex},
    frame::Frame,
    store::db::Db,
};

pub struct Zrevrangebylex {
    inner: Zrangebylex,
}

impl Zrevrangebylex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        Ok(Self {
            inner: parse_range_lex(frame, true, "zrevrangebylex")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply(db)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        self.inner.apply_async(db).await
    }
}
