use anyhow::Error;

use crate::{
    cmds::stream::{stream_entry_frame, xrange::parse_range_bound},
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xrevrange {
    key: String,
    end: Option<StreamId>,
    start: Option<StreamId>,
    count: Option<usize>,
}

impl Xrevrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xrevrange' command",
            ));
        }
        let key = frame.get_arg(1).unwrap().to_string();
        let end = parse_range_bound(frame.get_arg(2).unwrap().as_str(), false)?;
        let start = parse_range_bound(frame.get_arg(3).unwrap().as_str(), true)?;
        let mut count = None;
        let mut idx = 4;
        while idx < frame.arg_len() {
            match frame.get_arg(idx).unwrap().to_uppercase().as_str() {
                "COUNT" if idx + 1 < frame.arg_len() => {
                    count = Some(
                        frame
                            .get_arg(idx + 1)
                            .unwrap()
                            .parse::<usize>()
                            .map_err(|_| {
                                Error::msg("ERR value is not an integer or out of range")
                            })?,
                    );
                    idx += 2;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        Ok(Self {
            key,
            end,
            start,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_range(&self.key, self.start, self.end, self.count, true) {
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_range_async(&self.key, self.start, self.end, self.count, true)
            .await
        {
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
