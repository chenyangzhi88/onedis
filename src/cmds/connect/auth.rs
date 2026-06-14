use anyhow::Error;

use crate::{frame::Frame, server::Handler};

pub struct Auth {
    username: Option<String>,
    requirepass: String,
}

impl Auth {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 2 && args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'auth' command",
            ));
        }

        let username = (args.len() == 3).then(|| args[1].to_string());
        let requirepass = args
            .last()
            .expect("AUTH argument length was validated")
            .to_string();
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
