use anyhow::Error;

use crate::{
    cmds::stream::{stream_entries_frame, text_arg, validate_count},
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xrange {
    pub(crate) key: String,
    pub(crate) start: Option<StreamId>,
    pub(crate) end: Option<StreamId>,
    pub(crate) count: Option<usize>,
}

impl Xrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_range_command(frame, "xrange")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_range(&self.key, self.start, self.end, self.count, false) {
            Ok(entries) => stream_entries_frame(entries),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_range_async(&self.key, self.start, self.end, self.count, false)
            .await
        {
            Ok(entries) => stream_entries_frame(entries),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_range_command(frame: Frame, command: &str) -> Result<Xrange, Error> {
    if frame.arg_len() < 4 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    let key = text_arg(&frame, 1)?;
    let start = parse_range_bound(&text_arg(&frame, 2)?, true)?;
    let end = parse_range_bound(&text_arg(&frame, 3)?, false)?;
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
            _ => {
                return Err(Error::msg("ERR syntax error"));
            }
        }
    }
    Ok(Xrange {
        key,
        start,
        end,
        count,
    })
}

pub(crate) fn parse_range_bound(text: &str, is_start: bool) -> Result<Option<StreamId>, Error> {
    if text == "-" && is_start {
        return Ok(None);
    }
    if text == "+" && !is_start {
        return Ok(None);
    }
    StreamId::parse(text)
        .map(Some)
        .ok_or_else(|| Error::msg("ERR Invalid stream ID specified as stream command argument"))
}
