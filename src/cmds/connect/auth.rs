use anyhow::Error;

use crate::{frame::Frame, server::Handler};

pub struct Auth {
    username: Option<String>,
    requirepass: String,
}

impl Auth {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let arg_len = frame.arg_len();
        if arg_len != 2 && arg_len != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'auth' command",
            ));
        }

        let username = if arg_len == 3 {
            Some(frame.get_arg(1).ok_or_else(|| {
                Error::msg("ERR username and password must be valid UTF-8 strings")
            })?)
        } else {
            None
        };
        let requirepass = frame
            .get_arg(arg_len - 1)
            .ok_or_else(|| Error::msg("ERR username and password must be valid UTF-8 strings"))?;
        Ok(Auth {
            username,
            requirepass,
        })
    }

    pub fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
        match handler.login(self.username.as_deref(), &self.requirepass) {
            Ok(_) => Ok(Frame::Ok),
            Err(e) => Ok(Frame::Error(e.to_string())),
        }
    }
}
