use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xack {
    key: String,
    group: String,
    ids: Vec<StreamId>,
}

impl Xack {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xack' command",
            ));
        }
        let mut ids = Vec::with_capacity(frame.arg_len() - 3);
        for idx in 3..frame.arg_len() {
            ids.push(
                StreamId::parse(&frame.get_arg(idx).unwrap()).ok_or_else(|| {
                    Error::msg("ERR Invalid stream ID specified as stream command argument")
                })?,
            );
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            group: frame.get_arg(2).unwrap(),
            ids,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_ack(&self.key, &self.group, &self.ids) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_ack_async(&self.key, &self.group, &self.ids).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
