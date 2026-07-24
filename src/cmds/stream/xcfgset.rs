use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Xcfgset {
    _key: String,
}

impl Xcfgset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 || !args.len().is_multiple_of(2) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'xcfgset' command",
            ));
        }
        Ok(Self {
            _key: args[1].clone(),
        })
    }

    pub fn apply(self, _db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Error("ERR XCFGSET is not supported".to_string()))
    }

    pub async fn apply_async(self, _db: &Db) -> Result<Frame, Error> {
        Ok(Frame::Error("ERR XCFGSET is not supported".to_string()))
    }
}
