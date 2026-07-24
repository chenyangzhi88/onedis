use anyhow::Error;

use crate::{
    cmds::listing::{list_array, text_arg, validate_response_count},
    frame::Frame,
    store::db::Db,
};

pub struct Blmpop {
    pub(crate) timeout_secs: f64,
    pub(crate) keys: Vec<String>,
    pub(crate) left: bool,
    pub(crate) count: usize,
}

impl Blmpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'blmpop' command",
            ));
        }
        let timeout_secs = text_arg(&frame, 1)?
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
        if !timeout_secs.is_finite() {
            return Err(Error::msg("ERR timeout is not a float or out of range"));
        }
        if timeout_secs < 0.0 {
            return Err(Error::msg("ERR timeout is negative"));
        }
        let num_keys = text_arg(&frame, 2)?
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if num_keys == 0 {
            return Err(Error::msg("ERR numkeys should be greater than 0"));
        }
        let direction_idx = 3usize
            .checked_add(num_keys)
            .ok_or_else(|| Error::msg("ERR value is not an integer or out of range"))?;
        if frame.arg_len() != direction_idx + 1 && frame.arg_len() != direction_idx + 3 {
            return Err(Error::msg("ERR syntax error"));
        }
        let keys = (0..num_keys)
            .map(|idx| text_arg(&frame, 3 + idx))
            .collect::<Result<Vec<_>, _>>()?;
        let left = match text_arg(&frame, direction_idx)?
            .to_ascii_uppercase()
            .as_str()
        {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(Error::msg("ERR syntax error")),
        };
        let mut count = 1;
        if frame.arg_len() == direction_idx + 3 {
            if !text_arg(&frame, direction_idx + 1)?.eq_ignore_ascii_case("COUNT") {
                return Err(Error::msg("ERR syntax error"));
            }
            count = text_arg(&frame, direction_idx + 2)?
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
            if count == 0 {
                return Err(Error::msg("ERR count should be greater than 0"));
            }
        }
        validate_response_count(count)?;
        Ok(Self {
            timeout_secs,
            keys,
            left,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_multi_pop(&self.keys, self.left, self.count) {
            Ok(Some((key, values))) => Ok(Frame::Array(vec![
                Frame::bulk_string(key),
                list_array(values)?,
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
                list_array(values)?,
            ])),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
