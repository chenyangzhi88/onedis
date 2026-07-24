use anyhow::Error;

use crate::{
    cmds::stream::{stream_consumers_frame, stream_groups_frame, text_arg},
    frame::Frame,
    store::db::Db,
};

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
        match text_arg(&frame, 1)?.to_ascii_uppercase().as_str() {
            "GROUPS" if frame.arg_len() == 3 => Ok(Self::Groups {
                key: text_arg(&frame, 2)?,
            }),
            "CONSUMERS" if frame.arg_len() == 4 => Ok(Self::Consumers {
                key: text_arg(&frame, 2)?,
                group: text_arg(&frame, 3)?,
            }),
            "STREAM" if frame.arg_len() == 3 => Ok(Self::Stream {
                key: text_arg(&frame, 2)?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Groups { key } => match db.stream_groups(&key) {
                Ok(groups) => stream_groups_frame(groups),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Consumers { key, group } => match db.stream_consumers(&key, &group) {
                Ok(consumers) => stream_consumers_frame(consumers),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Stream { key } => match db.stream_len(&key) {
                Ok(len) => {
                    let groups = db.stream_groups(&key)?;
                    Ok(Frame::Array(vec![
                        Frame::bulk_string("length"),
                        Frame::Integer(len as i64),
                        Frame::bulk_string("groups"),
                        Frame::Integer(groups.len() as i64),
                    ]))
                }
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match self {
            Self::Groups { key } => match db.stream_groups_async(&key).await {
                Ok(groups) => stream_groups_frame(groups),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Consumers { key, group } => match db.stream_consumers_async(&key, &group).await {
                Ok(consumers) => stream_consumers_frame(consumers),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Self::Stream { key } => match db.stream_len_async(&key).await {
                Ok(len) => {
                    let groups = db.stream_groups_async(&key).await?;
                    Ok(Frame::Array(vec![
                        Frame::bulk_string("length"),
                        Frame::Integer(len as i64),
                        Frame::bulk_string("groups"),
                        Frame::Integer(groups.len() as i64),
                    ]))
                }
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
        }
    }
}
