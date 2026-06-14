use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, StreamId},
};

pub struct Xadd {
    key: String,
    id: Option<StreamId>,
    fields: Vec<(String, String)>,
}

impl Xadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'xadd' command"))?
            .to_string();
        let id_arg = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'xadd' command"))?;
        let id = if id_arg == "*" {
            None
        } else {
            Some(StreamId::parse(&id_arg).ok_or_else(|| {
                Error::msg("ERR Invalid stream ID specified as stream command argument")
            })?)
        };

        let arg_len = frame.arg_len();
        if arg_len < 5 || (arg_len - 3) % 2 != 0 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }

        let mut fields = Vec::with_capacity((arg_len - 3) / 2);
        let mut idx = 3;
        while idx < arg_len {
            let field = frame.get_arg(idx).unwrap().to_string();
            let value = frame.get_arg(idx + 1).unwrap().to_string();
            fields.push((field, value));
            idx += 2;
        }

        Ok(Self { key, id, fields })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_add(&self.key, self.id, &self.fields) {
            Ok(id) => Ok(Frame::bulk_string(id.to_redis_id())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_add_async(&self.key, self.id, &self.fields).await {
            Ok(id) => Ok(Frame::bulk_string(id.to_redis_id())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
