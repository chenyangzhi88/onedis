use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Pfadd {
    key: String,
    elements: Vec<String>,
}
impl Pfadd {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfadd' command",
            ));
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            elements: (2..frame.arg_len())
                .map(|i| frame.get_arg(i).unwrap())
                .collect(),
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.set_add(&self.key, &self.elements) {
            Ok(added) => Ok(Frame::Integer((added > 0) as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_add_async(&self.key, &self.elements).await {
            Ok(added) => Ok(Frame::Integer((added > 0) as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
