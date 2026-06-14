use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

#[derive(Clone, Copy)]
pub enum ListSide {
    Left,
    Right,
}

impl ListSide {
    pub fn parse(text: &str) -> Result<Self, Error> {
        match text.to_ascii_uppercase().as_str() {
            "LEFT" => Ok(Self::Left),
            "RIGHT" => Ok(Self::Right),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }
}

pub struct Lmove {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) source_side: ListSide,
    pub(crate) destination_side: ListSide,
}

impl Lmove {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lmove' command",
            ));
        }

        Ok(Self {
            source: frame.get_arg(1).unwrap(),
            destination: frame.get_arg(2).unwrap(),
            source_side: ListSide::parse(&frame.get_arg(3).unwrap())?,
            destination_side: ListSide::parse(&frame.get_arg(4).unwrap())?,
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

impl ListSide {
    pub fn is_left(self) -> bool {
        matches!(self, Self::Left)
    }
}
