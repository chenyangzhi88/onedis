use anyhow::Error;

use crate::{
    cmds::stream::{
        stream_pending_entries_frame, stream_pending_summary_frame, text_arg, validate_count,
    },
    frame::Frame,
    store::db::{Db, StreamId},
};

pub enum Xpending {
    Summary {
        key: String,
        group: String,
    },
    Range {
        key: String,
        group: String,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<String>,
    },
}

impl Xpending {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 && frame.arg_len() != 6 && frame.arg_len() != 7 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xpending' command",
            ));
        }
        let key = text_arg(&frame, 1)?;
        let group = text_arg(&frame, 2)?;
        if frame.arg_len() == 3 {
            return Ok(Self::Summary { key, group });
        }
        let start = parse_bound(&text_arg(&frame, 3)?, true)?;
        let end = parse_bound(&text_arg(&frame, 4)?, false)?;
        let count = text_arg(&frame, 5)?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        validate_count(count)?;
        let consumer = if frame.arg_len() == 7 {
            Some(text_arg(&frame, 6)?)
        } else {
            None
        };
        Ok(Self::Range {
            key,
            group,
            start,
            end,
            count,
            consumer,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Summary { key, group } => match db.stream_pending_summary(&key, &group) {
                Ok(summary) => stream_pending_summary_frame(summary),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Range {
                key,
                group,
                start,
                end,
                count,
                consumer,
            } => {
                match db.stream_pending_range(&key, &group, start, end, count, consumer.as_deref())
                {
                    Ok(entries) => stream_pending_entries_frame(entries),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Summary { key, group } => {
                match db.stream_pending_summary_async(&key, &group).await {
                    Ok(summary) => stream_pending_summary_frame(summary),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
            Self::Range {
                key,
                group,
                start,
                end,
                count,
                consumer,
            } => {
                match db
                    .stream_pending_range_async(
                        &key,
                        &group,
                        start,
                        end,
                        count,
                        consumer.as_deref(),
                    )
                    .await
                {
                    Ok(entries) => stream_pending_entries_frame(entries),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
        }
    }
}

fn parse_bound(text: &str, lower: bool) -> Result<StreamId, Error> {
    match text {
        "-" => Ok(StreamId { ms: 0, seq: 0 }),
        "+" => Ok(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        }),
        _ => StreamId::parse(text).ok_or_else(|| {
            let _ = lower;
            Error::msg("ERR Invalid stream ID specified as stream command argument")
        }),
    }
}
