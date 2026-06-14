use anyhow::Error;

use crate::frame::Frame;

pub struct Ping {
    message: Option<String>,
}

impl Ping {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() > 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'ping' command",
            ));
        }

        Ok(Ping {
            message: args.get(1).cloned(),
        })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        match self.message {
            Some(message) => Ok(Frame::bulk_string(message)),
            None => Ok(Frame::SimpleString("PONG".to_string())),
        }
    }
}
