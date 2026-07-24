use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Msetnx {
    key_vals: Vec<(String, Vec<u8>)>,
}

impl Msetnx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let arg_count = frame.arg_len().saturating_sub(1);
        if arg_count == 0 || !arg_count.is_multiple_of(2) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'msetnx' command",
            ));
        }

        let mut key_vals = Vec::with_capacity(arg_count / 2);
        for idx in (1..frame.arg_len()).step_by(2) {
            let key = frame
                .get_arg(idx)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
            let value = frame
                .get_arg_bytes(idx + 1)
                .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'msetnx' command"))?;
            key_vals.push((key, value));
        }

        Ok(Msetnx { key_vals })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if db.insert_string_bytes_many_nx(self.key_vals) {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if db.insert_string_bytes_many_nx_async(self.key_vals).await {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        }
    }
}
