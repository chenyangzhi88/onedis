use anyhow::Error;

use crate::{
    cmds::stream::{stream_entries_frame, text_arg, validate_count, xrange::parse_range_bound},
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
        let key = text_arg(&frame, 1)?;
        let end = parse_range_bound(&text_arg(&frame, 2)?, false)?;
        let start = parse_range_bound(&text_arg(&frame, 3)?, true)?;
        let mut count = None;
        let mut idx = 4;
        while idx < frame.arg_len() {
            match text_arg(&frame, idx)?.to_ascii_uppercase().as_str() {
                "COUNT" if idx + 1 < frame.arg_len() => {
                    let parsed = text_arg(&frame, idx + 1)?
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    validate_count(parsed)?;
                    count = Some(parsed);
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
            Ok(entries) => stream_entries_frame(entries),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_range_async(&self.key, self.start, self.end, self.count, true)
            .await
        {
            Ok(entries) => stream_entries_frame(entries),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
