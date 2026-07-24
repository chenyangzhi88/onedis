use anyhow::Error;

use crate::{
    cmds::sorted_set::common::{text_arg, validate_entry_count},
    frame::Frame,
    store::db::Db,
};

pub struct Zmpop {
    pub(crate) keys: Vec<String>,
    pub(crate) min: bool,
    pub(crate) count: usize,
}

impl Zmpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zmpop' command",
            ));
        }
        let num_keys = text_arg(&frame, 1)?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if num_keys == 0 {
            return Err(Error::msg("ERR numkeys should be greater than 0"));
        }
        let side_idx = 2usize
            .checked_add(num_keys)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        if side_idx >= frame.arg_len() {
            return Err(Error::msg("ERR syntax error"));
        }
        let keys = (0..num_keys)
            .map(|idx| text_arg(&frame, 2 + idx))
            .collect::<Result<Vec<_>, _>>()?;
        let min = match text_arg(&frame, side_idx)?.to_ascii_uppercase().as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(Error::msg("ERR syntax error")),
        };
        let mut count = 1usize;
        if frame.arg_len() == side_idx + 3 {
            if !text_arg(&frame, side_idx + 1)?.eq_ignore_ascii_case("COUNT") {
                return Err(Error::msg("ERR syntax error"));
            }
            count = text_arg(&frame, side_idx + 2)?
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
            if count == 0 {
                return Err(Error::msg("ERR count should be greater than 0"));
            }
        } else if frame.arg_len() != side_idx + 1 {
            return Err(Error::msg("ERR syntax error"));
        }
        validate_entry_count(count as u64, false)?;
        Ok(Self { keys, min, count })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_multi_pop(&self.keys, self.min, self.count) {
            Ok(Some((key, entries))) => Ok(zmpop_frame(key, entries)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_multi_pop_async(&self.keys, self.min, self.count)
            .await
        {
            Ok(Some((key, entries))) => Ok(zmpop_frame(key, entries)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn zmpop_frame(key: String, entries: Vec<(String, f64)>) -> Frame {
    Frame::Array(vec![
        Frame::bulk_string(key),
        Frame::Array(
            entries
                .into_iter()
                .map(|(member, score)| {
                    Frame::Array(vec![
                        Frame::bulk_string(member),
                        Frame::bulk_string(score.to_string()),
                    ])
                })
                .collect(),
        ),
    ])
}
