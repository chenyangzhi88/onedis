use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xsetid {
    key: String,
    id: StreamId,
}

impl Xsetid {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xsetid' command",
            ));
        }
        let id = StreamId::parse(&args[2]).ok_or_else(|| {
            Error::msg("ERR Invalid stream ID specified as stream command argument")
        })?;
        Ok(Self {
            key: args[1].clone(),
            id,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_set_id(&self.key, self.id) {
            Ok(()) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_set_id_async(&self.key, self.id).await {
            Ok(()) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
