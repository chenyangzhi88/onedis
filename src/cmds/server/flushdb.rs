use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Flushdb {}

impl Flushdb {
    pub fn new() -> Flushdb {
        Flushdb {}
    }

    pub fn parse_from_frame(_frame: Frame) -> Result<Self, Error> {
        Ok(Flushdb {})
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.clear();
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.clear_async().await;
        Ok(Frame::Ok)
    }
}
