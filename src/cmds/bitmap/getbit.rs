use crate::{
    cmds::bitmap::text_arg,
    frame::{Frame, MAX_BULK_STRING_BYTES},
    store::db::Db,
};
use anyhow::Error;
pub struct Getbit {
    key: String,
    offset: usize,
}
impl Getbit {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getbit' command",
            ));
        }
        let offset = text_arg(&frame, 2)?
            .parse()
            .map_err(|_| Error::msg("ERR bit offset is not an integer or out of range"))?;
        if offset >= MAX_BULK_STRING_BYTES.saturating_mul(8) {
            return Err(Error::msg(
                "ERR bit offset is not an integer or out of range",
            ));
        }
        Ok(Self {
            key: text_arg(&frame, 1)?,
            offset,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_get_bit(&self.key, self.offset)
            .map(|bit| Frame::Integer(bit as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_get_bit_async(&self.key, self.offset)
            .await
            .map(|bit| Frame::Integer(bit as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
