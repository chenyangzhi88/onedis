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

        let mut keys = Vec::with_capacity(frame.arg_len() - 1);
        for index in 1..frame.arg_len() {
            keys.push(
                frame
                    .get_arg(index)
                    .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            );
        }
        Ok(Self { keys })
    }

    pub fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
        handler.watch_keys(self.keys)?;
        Ok(Frame::Ok)
    }
}
