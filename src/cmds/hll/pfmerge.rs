use crate::{cmds::hll::text_arg, frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Pfmerge {
    dest: String,
    keys: Vec<String>,
}
impl Pfmerge {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfmerge' command",
            ));
        }
        Ok(Self {
            dest: text_arg(&frame, 1)?,
            keys: (2..frame.arg_len())
                .map(|i| text_arg(&frame, i))
                .collect::<Result<_, _>>()?,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_merge(&self.dest, &self.keys) {
            Ok(()) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_merge_async(&self.dest, &self.keys).await {
            Ok(()) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
