use anyhow::Error;

use crate::{cmds::listing::lmove::ListSide, frame::Frame, store::db::Db};

pub struct Blmove {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) source_side: ListSide,
    pub(crate) destination_side: ListSide,
    pub(crate) timeout_secs: f64,
}

impl Blmove {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 6 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'blmove' command",
            ));
        }
        let timeout_secs = frame
            .get_arg(5)
            .unwrap()
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
        if timeout_secs < 0.0 {
            return Err(Error::msg("ERR timeout is negative"));
        }
        Ok(Self {
            source: frame.get_arg(1).unwrap(),
            destination: frame.get_arg(2).unwrap(),
            source_side: ListSide::parse(&frame.get_arg(3).unwrap())?,
            destination_side: ListSide::parse(&frame.get_arg(4).unwrap())?,
            timeout_secs,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_move(
            &self.source,
            &self.destination,
            self.source_side.is_left(),
            self.destination_side.is_left(),
        ) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_move_async(
                &self.source,
                &self.destination,
                self.source_side.is_left(),
                self.destination_side.is_left(),
            )
            .await
        {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
