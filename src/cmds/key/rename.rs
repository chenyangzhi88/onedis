use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Rename {
    pub old_key: String,
    pub new_key: String,
}

impl Rename {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'rename' command",
            ));
        }
        let old_key_str = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'rename' command"))?;
        let new_key_str = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'rename' command"))?;

        Ok(Rename {
            old_key: old_key_str,
            new_key: new_key_str,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.rename_key(&self.old_key, &self.new_key, true)?;
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.rename_key_async(&self.old_key, &self.new_key, true)
            .await?;
        Ok(Frame::Ok)
    }
}
