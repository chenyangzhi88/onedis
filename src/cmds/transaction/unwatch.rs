use anyhow::Error;

use crate::{frame::Frame, server::Handler};

pub struct Unwatch;

impl Unwatch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'unwatch' command",
            ));
        }
        Ok(Self)
    }

    pub fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
        handler.clear_watches();
        Ok(Frame::Ok)
    }
}
