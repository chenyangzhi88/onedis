use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Renamenx {
    pub old_key: String,
    pub new_key: String,
}

impl Renamenx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'renamenx' command",
            ));
        }
        let old_key_str = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'renamenx' command"))?;
        let new_key_str = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR wrong number of arguments for 'renamenx' command"))?;

        Ok(Renamenx {
            old_key: old_key_str,
            new_key: new_key_str,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let renamed = db.rename_key(&self.old_key, &self.new_key, false)?;
        Ok(Frame::Integer(if renamed { 1 } else { 0 }))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let renamed = db
            .rename_key_async(&self.old_key, &self.new_key, false)
            .await?;
        Ok(Frame::Integer(if renamed { 1 } else { 0 }))
    }
}
