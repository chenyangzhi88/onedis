use crate::{
    cmds::hll::{binary_arg, text_arg},
    frame::Frame,
    store::db::Db,
};
use anyhow::Error;
pub struct Pfadd {
    key: String,
    elements: Vec<Vec<u8>>,
}
impl Pfadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfadd' command",
            ));
        }
        Ok(Self {
            key: text_arg(&frame, 1)?,
            elements: (2..frame.arg_len())
                .map(|i| binary_arg(&frame, i))
                .collect::<Result<_, _>>()?,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_add(&self.key, &self.elements) {
            Ok(changed) => Ok(Frame::Integer(i64::from(changed))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_add_async(&self.key, &self.elements).await {
            Ok(changed) => Ok(Frame::Integer(i64::from(changed))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
