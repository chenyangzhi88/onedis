use anyhow::Error;

use crate::frame::Frame;

pub struct Ping {
    message: Option<Vec<u8>>,
}

impl Ping {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() > 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ping' command",
            ));
        }

        Ok(Ping {
            message: frame.get_arg_bytes(1),
        })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        match self.message {
            Some(message) => Ok(Frame::bulk_string(message)),
            None => Ok(Frame::SimpleString("PONG".to_string())),
        }
    }
}
