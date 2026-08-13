use crate::{cmds::hll::text_arg, frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Pfcount {
    keys: Vec<String>,
}
impl Pfcount {
    pub(crate) fn is_single_key(&self) -> bool {
        self.keys.len() == 1
    }

    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfcount' command",
            ));
        }
        Ok(Self {
            keys: (1..frame.arg_len())
                .map(|i| text_arg(&frame, i))
                .collect::<Result<_, _>>()?,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_count(&self.keys) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hll_count_async(&self.keys).await {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
