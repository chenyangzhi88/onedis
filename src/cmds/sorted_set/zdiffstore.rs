use anyhow::Error;

use crate::{cmds::sorted_set::common::parse_numkeys_command, frame::Frame, store::db::Db};

pub struct Zdiffstore {
    destination: String,
    keys: Vec<String>,
}

impl Zdiffstore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zdiffstore' command",
            ));
        }
        let shifted = Frame::Array(
            std::iter::once(Frame::bulk_string("zdiff"))
                .chain(
                    (2..frame.arg_len()).map(|idx| Frame::bulk_string(frame.get_arg(idx).unwrap())),
                )
                .collect(),
        );
        Ok(Self {
            destination: frame.get_arg(1).unwrap(),
            keys: parse_numkeys_command(&shifted, "zdiffstore")?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_diff(&self.keys)
            .and_then(|entries| db.zset_store_entries(&self.destination, entries))
        {
            Ok(len) => Ok(Frame::Integer(len as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_diff(&self.keys) {
            Ok(entries) => match db
                .zset_store_entries_async(&self.destination, entries)
                .await
            {
                Ok(len) => Ok(Frame::Integer(len as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
