use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Brpoplpush {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) timeout_secs: f64,
}

impl Brpoplpush {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'brpoplpush' command",
            ));
        }
        let timeout_secs = frame
            .get_arg(3)
            .unwrap()
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
        if !timeout_secs.is_finite() {
            return Err(Error::msg("ERR timeout is not a float or out of range"));
        }
        if timeout_secs < 0.0 {
            return Err(Error::msg("ERR timeout is negative"));
        }
        Ok(Self {
            source: frame.get_arg(1).unwrap(),
            destination: frame.get_arg(2).unwrap(),
            timeout_secs,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_move(&self.source, &self.destination, false, true) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_move_async(&self.source, &self.destination, false, true)
            .await
        {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
