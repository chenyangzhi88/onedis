use anyhow::Error;

use crate::{
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
        let key = frame.get_arg(1).unwrap();
        let group = frame.get_arg(2).unwrap();
        if frame.arg_len() == 3 {
            return Ok(Self::Summary { key, group });
        }
        let start = parse_bound(&frame.get_arg(3).unwrap(), true)?;
        let end = parse_bound(&frame.get_arg(4).unwrap(), false)?;
        let count = frame
            .get_arg(5)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let consumer = (frame.arg_len() == 7).then(|| frame.get_arg(6).unwrap());
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
                Ok(summary) => Ok(Frame::Array(vec![
                    Frame::Integer(summary.total as i64),
                    summary
                        .smallest_id
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                    summary
                        .greatest_id
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                    Frame::Array(
                        summary
                            .consumers
                            .into_iter()
                            .map(|(name, count)| {
                                Frame::Array(vec![
                                    Frame::bulk_string(name),
                                    Frame::Integer(count as i64),
                                ])
                            })
                            .collect(),
                    ),
                ])),
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
                    Ok(entries) => Ok(Frame::Array(
                        entries
                            .into_iter()
                            .map(|entry| {
                                Frame::Array(vec![
                                    Frame::bulk_string(entry.id),
                                    Frame::bulk_string(entry.consumer),
                                    Frame::Integer(entry.idle_ms as i64),
                                    Frame::Integer(entry.deliveries as i64),
                                ])
                            })
                            .collect(),
                    )),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Summary { key, group } => {
                match db.stream_pending_summary_async(&key, &group).await {
                    Ok(summary) => Ok(Frame::Array(vec![
                        Frame::Integer(summary.total as i64),
                        summary
                            .smallest_id
                            .map(Frame::bulk_string)
                            .unwrap_or(Frame::Null),
                        summary
                            .greatest_id
                            .map(Frame::bulk_string)
                            .unwrap_or(Frame::Null),
                        Frame::Array(
                            summary
                                .consumers
                                .into_iter()
                                .map(|(name, count)| {
                                    Frame::Array(vec![
                                        Frame::bulk_string(name),
                                        Frame::Integer(count as i64),
                                    ])
                                })
                                .collect(),
                        ),
                    ])),
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
                    Ok(entries) => Ok(Frame::Array(
                        entries
                            .into_iter()
                            .map(|entry| {
                                Frame::Array(vec![
                                    Frame::bulk_string(entry.id),
                                    Frame::bulk_string(entry.consumer),
                                    Frame::Integer(entry.idle_ms as i64),
                                    Frame::Integer(entry.deliveries as i64),
                                ])
                            })
                            .collect(),
                    )),
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
