use anyhow::Error;

use crate::{frame::Frame, server::Handler};

pub struct Watch {
    keys: Vec<String>,
}

impl Watch {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'watch' command",
            ));
        }

        let keys = frame.get_args().into_iter().skip(1).collect();
        Ok(Self { keys })
    }

    pub fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
        handler.watch_keys(self.keys)?;
        Ok(Frame::Ok)
    }
}
