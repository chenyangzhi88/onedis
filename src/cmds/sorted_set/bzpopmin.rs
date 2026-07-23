use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Bzpopmin {
    pub(crate) keys: Vec<String>,
    pub(crate) timeout_secs: f64,
    pub(crate) min: bool,
}

impl Bzpopmin {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_bzpop(frame, true, "bzpopmin")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_multi_pop(&self.keys, self.min, 1) {
            Ok(Some((key, mut entries))) => {
                let Some((member, score)) = entries.pop() else {
                    return Ok(Frame::Null);
                };
                Ok(Frame::Array(vec![
                    Frame::bulk_string(key),
                    Frame::bulk_string(member),
                    Frame::bulk_string(score.to_string()),
                ]))
            }
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_multi_pop_async(&self.keys, self.min, 1).await {
            Ok(Some((key, mut entries))) => {
                let Some((member, score)) = entries.pop() else {
                    return Ok(Frame::Null);
                };
                Ok(Frame::Array(vec![
                    Frame::bulk_string(key),
                    Frame::bulk_string(member),
                    Frame::bulk_string(score.to_string()),
                ]))
            }
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_bzpop(frame: Frame, min: bool, command: &str) -> Result<Bzpopmin, Error> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    }
    let timeout_secs = frame
        .get_arg(frame.arg_len() - 1)
        .unwrap()
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
    if !timeout_secs.is_finite() {
        return Err(Error::msg("ERR timeout is not a float or out of range"));
    }
    if timeout_secs < 0.0 {
        return Err(Error::msg("ERR timeout is negative"));
    }
    let keys = (1..frame.arg_len() - 1)
        .map(|idx| frame.get_arg(idx).unwrap())
        .collect();
    Ok(Bzpopmin {
        keys,
        timeout_secs,
        min,
    })
}
