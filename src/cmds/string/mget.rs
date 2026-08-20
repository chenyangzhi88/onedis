use crate::{cmds::string::checked_string_values, frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Mget {
    keys: Vec<String>,
}

impl Mget {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'mget' command",
            ));
        }

        Ok(Mget { keys: args })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::with_capacity(self.keys.len());
        for key in self.keys {
            match db.get_string_bytes(&key) {
                Ok(value) => result.push(value),
                // Redis treats keys holding non-string values as missing in MGET.
                Err(_) => result.push(None),
            }
        }
        checked_string_values(result)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        checked_string_values(db.get_string_bytes_many_async(&self.keys).await?)
    }
}
