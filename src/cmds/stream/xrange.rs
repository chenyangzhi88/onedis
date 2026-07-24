use anyhow::Error;

use crate::{
    cmds::stream::stream_entry_frame,
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
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_range_async(&self.key, self.start, self.end, self.count, false)
            .await
        {
            Ok(entries) => Ok(Frame::Array(
                entries.into_iter().map(stream_entry_frame).collect(),
            )),
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
    let key = frame.get_arg(1).unwrap().to_string();
    let start = parse_range_bound(&frame.get_arg(2).unwrap(), true)?;
    let end = parse_range_bound(&frame.get_arg(3).unwrap(), false)?;
    let mut count = None;
    let mut idx = 4;
    while idx < frame.arg_len() {
        match frame.get_arg(idx).unwrap().to_ascii_uppercase().as_str() {
            "COUNT" if idx + 1 < frame.arg_len() => {
                count = Some(
                    frame
                        .get_arg(idx + 1)
                        .unwrap()
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                );
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
