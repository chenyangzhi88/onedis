use anyhow::Error;

use crate::{
    cmds::stream::{stream_reads_frame, text_arg, validate_count},
    frame::Frame,
    store::db::{Db, StreamId, StreamReadGroupStart},
};

pub struct Xreadgroup {
    pub(crate) group: String,
    pub(crate) consumer: String,
    pub(crate) count: Option<usize>,
    pub(crate) block_ms: Option<u64>,
    pub(crate) noack: bool,
    pub(crate) streams: Vec<(String, StreamReadGroupStart)>,
}

impl Xreadgroup {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 7 || !text_arg(&frame, 1)?.eq_ignore_ascii_case("GROUP") {
            return Err(Error::msg("ERR syntax error"));
        }
        let group = text_arg(&frame, 2)?;
        let consumer = text_arg(&frame, 3)?;
        let mut count = None;
        let mut block_ms = None;
        let mut noack = false;
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
                "BLOCK" if idx + 1 < frame.arg_len() => {
                    block_ms =
                        Some(text_arg(&frame, idx + 1)?.parse::<u64>().map_err(|_| {
                            Error::msg("ERR value is not an integer or out of range")
                        })?);
                    idx += 2;
                }
                "NOACK" => {
                    noack = true;
                    idx += 1;
                }
                "STREAMS" => {
                    idx += 1;
                    break;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }
        let remaining = frame.arg_len().saturating_sub(idx);
        if remaining == 0 || remaining % 2 != 0 {
            return Err(Error::msg("ERR syntax error"));
        }
        let stream_count = remaining / 2;
        let mut streams = Vec::with_capacity(stream_count);
        for offset in 0..stream_count {
            let key = text_arg(&frame, idx + offset)?;
            let id_arg = text_arg(&frame, idx + stream_count + offset)?;
            let start = if id_arg == ">" {
                StreamReadGroupStart::New
            } else {
                StreamReadGroupStart::Id(StreamId::parse(&id_arg).ok_or_else(|| {
                    Error::msg("ERR Invalid stream ID specified as stream command argument")
                })?)
            };
            streams.push((key, start));
        }
        Ok(Self {
            group,
            consumer,
            count,
            block_ms,
            noack,
            streams,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_read_group(
            &self.group,
            &self.consumer,
            &self.streams,
            self.count,
            self.noack,
        ) {
            Ok(streams) if streams.is_empty() => Ok(Frame::Null),
            Ok(streams) => stream_reads_frame(streams),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .stream_read_group_async(
                &self.group,
                &self.consumer,
                &self.streams,
                self.count,
                self.noack,
            )
            .await
        {
            Ok(streams) if streams.is_empty() => Ok(Frame::Null),
            Ok(streams) => stream_reads_frame(streams),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
