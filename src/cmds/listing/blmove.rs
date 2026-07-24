use anyhow::Error;

use crate::{
    cmds::listing::{lmove::ListSide, text_arg},
    frame::Frame,
    store::db::Db,
};

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
        let timeout_secs = text_arg(&frame, 5)?
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR timeout is not a float or out of range"))?;
        if !timeout_secs.is_finite() {
            return Err(Error::msg("ERR timeout is not a float or out of range"));
        }
        if timeout_secs < 0.0 {
            return Err(Error::msg("ERR timeout is negative"));
        }
        Ok(Self {
            source: text_arg(&frame, 1)?,
            destination: text_arg(&frame, 2)?,
            source_side: ListSide::parse(&text_arg(&frame, 3)?)?,
            destination_side: ListSide::parse(&text_arg(&frame, 4)?)?,
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
