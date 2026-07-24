use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Renamenx {
    pub old_key: String,
    pub new_key: String,
}

impl Renamenx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let old_key = frame.get_arg(1);
        let new_key = frame.get_arg(2);

        if frame.arg_len() != 3 || old_key.is_none() || new_key.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'renamenx' command",
            ));
        }

        let old_key_str = old_key.unwrap().to_string();
        let new_key_str = new_key.unwrap().to_string();

        Ok(Renamenx {
            old_key: old_key_str,
            new_key: new_key_str,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if !db.exists(&self.old_key) {
            return Err(Error::msg("ERR no such key"));
        }

        if db.exists(&self.new_key) {
            return Ok(Frame::Integer(0));
        }

        if let Some(value) = db.remove(&self.old_key) {
            db.insert(self.new_key.clone(), value);
        }

        Ok(Frame::Integer(1))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let renamed = db
            .rename_key_async(&self.old_key, &self.new_key, false)
            .await?;
        Ok(Frame::Integer(if renamed { 1 } else { 0 }))
    }
}
