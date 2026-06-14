use anyhow::Error;

use crate::frame::Frame;

pub struct Echo {
    str: String,
}

impl Echo {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'echo' command",
            ));
        }

        let str = match frame.get_arg(1) {
            Some(name) => name.to_string(),
            None => {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'echo' command",
                ));
            }
        };
        Ok(Echo { str })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(Frame::bulk_string(self.str))
    }
}
