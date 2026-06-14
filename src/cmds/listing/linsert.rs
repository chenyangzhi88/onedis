use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Linsert {
    key: String,
    before: bool,
    pivot: String,
    element: String,
}

impl Linsert {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'linsert' command",
            ));
        }

        let position = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR syntax error"))?
            .to_ascii_uppercase();
        let before = match position.as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return Err(Error::msg("ERR syntax error")),
        };

        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            before,
            pivot: frame.get_arg(3).unwrap(),
            element: frame.get_arg(4).unwrap(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_insert(&self.key, self.before, &self.pivot, &self.element) {
            Ok(len) => Ok(Frame::Integer(len)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_insert_async(&self.key, self.before, &self.pivot, &self.element)
            .await
        {
            Ok(len) => Ok(Frame::Integer(len)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
