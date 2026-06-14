use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub enum Xinfo {
    Groups { key: String },
    Consumers { key: String, group: String },
    Stream { key: String },
}

impl Xinfo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xinfo' command",
            ));
        }
        match frame.get_arg(1).unwrap().to_ascii_uppercase().as_str() {
            "GROUPS" => Ok(Self::Groups {
                key: frame.get_arg(2).unwrap(),
            }),
            "CONSUMERS" if frame.arg_len() >= 4 => Ok(Self::Consumers {
                key: frame.get_arg(2).unwrap(),
                group: frame.get_arg(3).unwrap(),
            }),
            "STREAM" => Ok(Self::Stream {
                key: frame.get_arg(2).unwrap(),
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Groups { key } => match db.stream_groups(&key) {
                Ok(groups) => Ok(Frame::Array(
                    groups
                        .into_iter()
                        .map(|group| {
                            Frame::Array(vec![
                                Frame::bulk_string("name"),
                                Frame::bulk_string(group.name),
                                Frame::bulk_string("consumers"),
                                Frame::Integer(group.consumers as i64),
                                Frame::bulk_string("pending"),
                                Frame::Integer(group.pending as i64),
                                Frame::bulk_string("last-delivered-id"),
                                Frame::bulk_string(group.last_delivered_id),
                                Frame::bulk_string("entries-read"),
                                Frame::Integer(group.entries_read as i64),
                            ])
                        })
                        .collect(),
                )),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Consumers { key, group } => match db.stream_consumers(&key, &group) {
                Ok(consumers) => Ok(Frame::Array(
                    consumers
                        .into_iter()
                        .map(|consumer| {
                            Frame::Array(vec![
                                Frame::bulk_string("name"),
                                Frame::bulk_string(consumer.name),
                                Frame::bulk_string("pending"),
                                Frame::Integer(consumer.pending as i64),
                                Frame::bulk_string("idle"),
                                Frame::Integer(consumer.idle_ms as i64),
                            ])
                        })
                        .collect(),
                )),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Stream { key } => match db.stream_len(&key) {
                Ok(len) => Ok(Frame::Array(vec![
                    Frame::bulk_string("length"),
                    Frame::Integer(len as i64),
                    Frame::bulk_string("groups"),
                    Frame::Integer(
                        db.stream_groups(&key)
                            .map(|groups| groups.len())
                            .unwrap_or(0) as i64,
                    ),
                ])),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Groups { key } => match db.stream_groups_async(&key).await {
                Ok(groups) => Ok(Frame::Array(
                    groups
                        .into_iter()
                        .map(|group| {
                            Frame::Array(vec![
                                Frame::bulk_string("name"),
                                Frame::bulk_string(group.name),
                                Frame::bulk_string("consumers"),
                                Frame::Integer(group.consumers as i64),
                                Frame::bulk_string("pending"),
                                Frame::Integer(group.pending as i64),
                                Frame::bulk_string("last-delivered-id"),
                                Frame::bulk_string(group.last_delivered_id),
                                Frame::bulk_string("entries-read"),
                                Frame::Integer(group.entries_read as i64),
                            ])
                        })
                        .collect(),
                )),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Consumers { key, group } => match db.stream_consumers_async(&key, &group).await {
                Ok(consumers) => Ok(Frame::Array(
                    consumers
                        .into_iter()
                        .map(|consumer| {
                            Frame::Array(vec![
                                Frame::bulk_string("name"),
                                Frame::bulk_string(consumer.name),
                                Frame::bulk_string("pending"),
                                Frame::Integer(consumer.pending as i64),
                                Frame::bulk_string("idle"),
                                Frame::Integer(consumer.idle_ms as i64),
                            ])
                        })
                        .collect(),
                )),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Stream { key } => match db.stream_len_async(&key).await {
                Ok(len) => Ok(Frame::Array(vec![
                    Frame::bulk_string("length"),
                    Frame::Integer(len as i64),
                    Frame::bulk_string("groups"),
                    Frame::Integer(
                        db.stream_groups_async(&key)
                            .await
                            .map(|groups| groups.len())
                            .unwrap_or(0) as i64,
                    ),
                ])),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
        }
    }
}
