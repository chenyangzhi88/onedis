use crate::{
    cmds::bitmap::text_arg,
    frame::{Frame, MAX_BULK_STRING_BYTES},
    store::db::Db,
};
use anyhow::Error;
pub struct Setbit {
    key: String,
    offset: usize,
    bit: u8,
}
impl Setbit {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setbit' command",
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
            bit: text_arg(&frame, 3)?
                .parse()
                .map_err(|_| Error::msg("ERR bit is not an integer or out of range"))?,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_set_bit(&self.key, self.offset, self.bit)
            .map(|bit| Frame::Integer(bit as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_set_bit_async(&self.key, self.offset, self.bit)
            .await
            .map(|bit| Frame::Integer(bit as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
