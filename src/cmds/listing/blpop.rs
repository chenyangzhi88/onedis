use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Blpop {
    pub(crate) keys: Vec<String>,
    pub(crate) timeout_secs: f64,
    pub(crate) left: bool,
}

impl Blpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_blocking_pop(frame, true, "blpop")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_blocking_pop_once(&self.keys, self.left) {
            Ok(Some((key, value))) => Ok(Frame::Array(vec![
                Frame::bulk_string(key),
                Frame::bulk_string(value),
            ])),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.list_multi_pop_async(&self.keys, self.left, 1).await {
            Ok(Some((key, mut values))) => match values.pop() {
                Some(value) => Ok(Frame::Array(vec![
                    Frame::bulk_string(key),
                    Frame::bulk_string(value),
                ])),
                None => Ok(Frame::Null),
            },
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_blocking_pop(frame: Frame, left: bool, command: &str) -> Result<Blpop, Error> {
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
    Ok(Blpop {
        keys,
        timeout_secs,
        left,
    })
}
