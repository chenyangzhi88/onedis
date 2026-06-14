use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Lmpop {
    pub(crate) keys: Vec<String>,
    pub(crate) left: bool,
    pub(crate) count: usize,
}

impl Lmpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lmpop' command",
            ));
        }

        let num_keys = frame
            .get_arg(1)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if num_keys == 0 {
            return Err(Error::msg("ERR numkeys should be greater than 0"));
        }

        let direction_idx = 2 + num_keys;
        if frame.arg_len() != direction_idx + 1 && frame.arg_len() != direction_idx + 3 {
            return Err(Error::msg("ERR syntax error"));
        }
        if direction_idx >= frame.arg_len() {
            return Err(Error::msg("ERR syntax error"));
        }

        let keys = (0..num_keys)
            .map(|idx| frame.get_arg(2 + idx).unwrap())
            .collect();
        let left = match frame
            .get_arg(direction_idx)
            .unwrap()
            .to_ascii_uppercase()
            .as_str()
        {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(Error::msg("ERR syntax error")),
        };

        let mut count = 1;
        if frame.arg_len() == direction_idx + 3 {
            if frame
                .get_arg(direction_idx + 1)
                .unwrap()
                .to_ascii_uppercase()
                != "COUNT"
            {
                return Err(Error::msg("ERR syntax error"));
            }
            count = frame
                .get_arg(direction_idx + 2)
                .unwrap()
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
            if count == 0 {
                return Err(Error::msg("ERR count should be greater than 0"));
            }
        }

        Ok(Self { keys, left, count })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_multi_pop(&self.keys, self.left, self.count) {
            Ok(Some((key, values))) => Ok(Frame::Array(vec![
                Frame::bulk_string(key),
                Frame::Array(values.into_iter().map(Frame::bulk_string).collect()),
            ])),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_multi_pop_async(&self.keys, self.left, self.count)
            .await
        {
            Ok(Some((key, values))) => Ok(Frame::Array(vec![
                Frame::bulk_string(key),
                Frame::Array(values.into_iter().map(Frame::bulk_string).collect()),
            ])),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
