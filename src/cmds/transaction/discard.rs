use crate::frame::Frame;
use anyhow::Error;

pub struct Discard;

impl Discard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 1 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'discard' command",
            ));
        }
        Ok(Discard)
    }

    pub fn apply(&self, handler: &mut crate::server::Handler) -> Result<Frame, Error> {
        if !handler.is_in_transaction() {
            return Ok(Frame::Error("ERR DISCARD without MULTI".to_string()));
        }
        handler.clear_transaction();
        Ok(Frame::Ok)
    }
}
