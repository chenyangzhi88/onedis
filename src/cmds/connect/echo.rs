use anyhow::Error;

use crate::frame::Frame;

pub struct Echo {
    value: Vec<u8>,
}

impl Echo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'echo' command",
            ));
        }

        let value = match frame.get_arg_bytes(1) {
            Some(value) => value,
            None => {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'echo' command",
                ));
            }
        };
        Ok(Echo { value })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(Frame::bulk_string(self.value))
    }
}
