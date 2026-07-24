use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Xtrim {
    key: String,
    max_len: usize,
}

impl Xtrim {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xtrim' command",
            ));
        }
        if !frame.get_arg(2).unwrap().eq_ignore_ascii_case("MAXLEN") {
            return Err(Error::msg("ERR syntax error"));
        }
        let threshold_idx = if frame.get_arg(3).unwrap() == "=" || frame.get_arg(3).unwrap() == "~"
        {
            4
        } else {
            3
        };
        if threshold_idx >= frame.arg_len() {
            return Err(Error::msg("ERR syntax error"));
        }
        if frame.arg_len() != threshold_idx + 1 {
            return Err(Error::msg("ERR syntax error"));
        }
        let max_len = frame
            .get_arg(threshold_idx)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            max_len,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_trim_maxlen(&self.key, self.max_len) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.stream_trim_maxlen_async(&self.key, self.max_len).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
