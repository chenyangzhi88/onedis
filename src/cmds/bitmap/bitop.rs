use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Bitop {
    op: String,
    dest: String,
    keys: Vec<String>,
}
impl Bitop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bitop' command",
            ));
        }
        Ok(Self {
            op: frame.get_arg(1).unwrap(),
            dest: frame.get_arg(2).unwrap(),
            keys: (3..frame.arg_len())
                .map(|i| frame.get_arg(i).unwrap())
                .collect(),
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitop(&self.op, &self.dest, &self.keys)
            .map(|len| Frame::Integer(len as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitop_async(&self.op, &self.dest, &self.keys)
            .await
            .map(|len| Frame::Integer(len as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
