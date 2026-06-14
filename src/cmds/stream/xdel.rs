use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xdel {
    key: String,
    ids: Vec<StreamId>,
}

impl Xdel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xdel' command",
            ));
        }
        let key = frame.get_arg(1).unwrap();
        let mut ids = Vec::with_capacity(frame.arg_len() - 2);
        for idx in 2..frame.arg_len() {
            ids.push(
                StreamId::parse(&frame.get_arg(idx).unwrap()).ok_or_else(|| {
                    Error::msg("ERR Invalid stream ID specified as stream command argument")
                })?,
            );
        }
        Ok(Self { key, ids })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_delete(&self.key, &self.ids) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_delete_async(&self.key, &self.ids).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
