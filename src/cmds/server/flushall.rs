use crate::frame::Frame;
use anyhow::Error;

pub struct Flushall {}

impl Flushall {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() > 2
            || (frame.arg_len() == 2
                && !frame.get_arg(1).is_some_and(|arg| {
                    arg.eq_ignore_ascii_case("ASYNC") || arg.eq_ignore_ascii_case("SYNC")
                }))
        {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Flushall {})
    }
}
