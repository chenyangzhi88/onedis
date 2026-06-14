use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Setnx {
    key: String,
    value: Vec<u8>,
}

impl Setnx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setnx' command",
            ));
        }

        Ok(Setnx {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            value: frame
                .get_arg_bytes(2)
                .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'setnx' command"))?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if db.exists(&self.key) {
            return Ok(Frame::Integer(0));
        }

        db.insert_string_bytes(self.key, self.value, None);
        Ok(Frame::Integer(1))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if db.exists(&self.key) {
            return Ok(Frame::Integer(0));
        }

        db.insert_string_bytes_async(self.key, self.value, None)
            .await;
        Ok(Frame::Integer(1))
    }
}
