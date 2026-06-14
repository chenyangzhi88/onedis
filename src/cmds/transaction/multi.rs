use crate::frame::Frame;
use anyhow::Error;

pub struct Multi;

impl Multi {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'multi' command",
            ));
        }
        Ok(Multi)
    }

    pub fn apply(&self, handler: &mut crate::server::Handler) -> Result<Frame, Error> {
        handler.start_transaction()?;
        Ok(Frame::Ok)
    }
}
