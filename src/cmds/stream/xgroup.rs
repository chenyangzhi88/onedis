use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub enum Xgroup {
    Create {
        key: String,
        group: String,
        id: StreamId,
        mkstream: bool,
    },
    SetId {
        key: String,
        group: String,
        id: StreamId,
    },
    Destroy {
        key: String,
        group: String,
    },
    CreateConsumer {
        key: String,
        group: String,
        consumer: String,
    },
    DelConsumer {
        key: String,
        group: String,
        consumer: String,
    },
}

impl Xgroup {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xgroup' command",
            ));
        }
        match frame.get_arg(1).unwrap().to_ascii_uppercase().as_str() {
            "CREATE" => {
                if frame.arg_len() < 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'xgroup create' command",
                    ));
                }
                let id = parse_id_or_latest(&frame.get_arg(4).unwrap())?;
                let mut mkstream = false;
                for idx in 5..frame.arg_len() {
                    if frame.get_arg(idx).unwrap().eq_ignore_ascii_case("MKSTREAM") {
                        mkstream = true;
                    }
                }
                Ok(Self::Create {
                    key: frame.get_arg(2).unwrap(),
                    group: frame.get_arg(3).unwrap(),
                    id,
                    mkstream,
                })
            }
            "SETID" => {
                if frame.arg_len() < 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'xgroup setid' command",
                    ));
                }
                Ok(Self::SetId {
                    key: frame.get_arg(2).unwrap(),
                    group: frame.get_arg(3).unwrap(),
                    id: parse_id_or_latest(&frame.get_arg(4).unwrap())?,
                })
            }
            "DESTROY" => {
                if frame.arg_len() != 4 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'xgroup destroy' command",
                    ));
                }
                Ok(Self::Destroy {
                    key: frame.get_arg(2).unwrap(),
                    group: frame.get_arg(3).unwrap(),
                })
            }
            "CREATECONSUMER" => {
                if frame.arg_len() != 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'xgroup createconsumer' command",
                    ));
                }
                Ok(Self::CreateConsumer {
                    key: frame.get_arg(2).unwrap(),
                    group: frame.get_arg(3).unwrap(),
                    consumer: frame.get_arg(4).unwrap(),
                })
            }
            "DELCONSUMER" => {
                if frame.arg_len() != 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'xgroup delconsumer' command",
                    ));
                }
                Ok(Self::DelConsumer {
                    key: frame.get_arg(2).unwrap(),
                    group: frame.get_arg(3).unwrap(),
                    consumer: frame.get_arg(4).unwrap(),
                })
            }
            _ => Err(Error::msg("ERR unknown subcommand")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Create {
                key,
                group,
                id,
                mkstream,
            } => match db.stream_group_create(&key, &group, id, mkstream) {
                Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::SetId { key, group, id } => match db.stream_group_set_id(&key, &group, id) {
                Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Destroy { key, group } => match db.stream_group_destroy(&key, &group) {
                Ok(count) => Ok(Frame::Integer(count as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::CreateConsumer {
                key,
                group,
                consumer,
            } => match db.stream_group_create_consumer(&key, &group, &consumer) {
                Ok(count) => Ok(Frame::Integer(count as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::DelConsumer {
                key,
                group,
                consumer,
            } => match db.stream_group_delete_consumer(&key, &group, &consumer) {
                Ok(count) => Ok(Frame::Integer(count as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Create {
                key,
                group,
                id,
                mkstream,
            } => match db
                .stream_group_create_async(&key, &group, id, mkstream)
                .await
            {
                Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::SetId { key, group, id } => {
                match db.stream_group_set_id_async(&key, &group, id).await {
                    Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
            Self::Destroy { key, group } => {
                match db.stream_group_destroy_async(&key, &group).await {
                    Ok(count) => Ok(Frame::Integer(count as i64)),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
            Self::CreateConsumer {
                key,
                group,
                consumer,
            } => {
                match db
                    .stream_group_create_consumer_async(&key, &group, &consumer)
                    .await
                {
                    Ok(count) => Ok(Frame::Integer(count as i64)),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
            Self::DelConsumer {
                key,
                group,
                consumer,
            } => {
                match db
                    .stream_group_delete_consumer_async(&key, &group, &consumer)
                    .await
                {
                    Ok(count) => Ok(Frame::Integer(count as i64)),
                    Err(err) => Ok(Frame::Error(err.to_string())),
                }
            }
        }
    }
}

fn parse_id_or_latest(text: &str) -> Result<StreamId, Error> {
    if text == "$" {
        Ok(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        })
    } else {
        StreamId::parse(text)
            .ok_or_else(|| Error::msg("ERR Invalid stream ID specified as stream command argument"))
    }
}
