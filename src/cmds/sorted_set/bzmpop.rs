use anyhow::Error;

use crate::{cmds::sorted_set::zmpop::zmpop_frame, frame::Frame, store::db::Db};

pub struct Bzmpop {
    pub(crate) timeout_secs: f64,
    pub(crate) keys: Vec<String>,
    pub(crate) min: bool,
    pub(crate) count: usize,
}

impl Bzmpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bzmpop' command",
            ));
        }
        let timeout_secs = frame
            .get_arg(1)
            .unwrap()
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
        if !timeout_secs.is_finite() {
            return Err(Error::msg("ERR timeout is not a float or out of range"));
        }
        if timeout_secs < 0.0 {
            return Err(Error::msg("ERR timeout is negative"));
        }
        let num_keys = frame
            .get_arg(2)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let side_idx = 3 + num_keys;
        if num_keys == 0 || side_idx >= frame.arg_len() {
            return Err(Error::msg("ERR syntax error"));
        }
        let keys = (0..num_keys)
            .map(|idx| frame.get_arg(3 + idx).unwrap())
            .collect();
        let min = match frame
            .get_arg(side_idx)
            .unwrap()
            .to_ascii_uppercase()
            .as_str()
        {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(Error::msg("ERR syntax error")),
        };
        let mut count = 1usize;
        if frame.arg_len() == side_idx + 3 {
            if !frame
                .get_arg(side_idx + 1)
                .unwrap()
                .eq_ignore_ascii_case("COUNT")
            {
                return Err(Error::msg("ERR syntax error"));
            }
            count = frame
                .get_arg(side_idx + 2)
                .unwrap()
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
            if count == 0 {
                return Err(Error::msg("ERR count should be greater than 0"));
            }
        } else if frame.arg_len() != side_idx + 1 {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self {
            timeout_secs,
            keys,
            min,
            count,
        })
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
