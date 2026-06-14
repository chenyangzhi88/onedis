use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Mset {
    key_vals: Vec<(String, Vec<u8>)>,
}

impl Mset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let arg_count = frame.arg_len().saturating_sub(1);
        if arg_count == 0 || arg_count % 2 != 0 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'mset' command",
            ));
        }

        let mut key_vals = Vec::new();

        for i in (1..frame.arg_len()).step_by(2) {
            let key = frame
                .get_arg(i)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
            let val = frame
                .get_arg_bytes(i + 1)
                .ok_or_else(|| Error::msg("ERR invalid bulk string argument"))?;
            key_vals.push((key, val));
        }

        Ok(Mset { key_vals })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.insert_string_bytes_many(self.key_vals);
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.insert_string_bytes_many_async(self.key_vals).await;
        Ok(Frame::Ok)
    }
}
