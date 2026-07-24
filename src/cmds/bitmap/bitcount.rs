use crate::{cmds::bitmap::text_arg, frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Bitcount {
    key: String,
    start: Option<i64>,
    end: Option<i64>,
    bit_unit: bool,
}
impl Bitcount {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 2 && frame.arg_len() != 4 && frame.arg_len() != 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bitcount' command",
            ));
        }
        let bit_unit = if frame.arg_len() == 5 {
            match text_arg(&frame, 4)?.to_ascii_uppercase().as_str() {
                "BYTE" => false,
                "BIT" => true,
                _ => return Err(Error::msg("ERR syntax error")),
            }
        } else {
            false
        };
        Ok(Self {
            key: text_arg(&frame, 1)?,
            start: if frame.arg_len() >= 4 {
                Some(
                    text_arg(&frame, 2)?
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                )
            } else {
                None
            },
            end: if frame.arg_len() >= 4 {
                Some(
                    text_arg(&frame, 3)?
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                )
            } else {
                None
            },
            bit_unit,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitcount_with_unit(&self.key, self.start, self.end, self.bit_unit)
            .map(|count| Frame::Integer(count as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitcount_with_unit_async(&self.key, self.start, self.end, self.bit_unit)
            .await
            .map(|count| Frame::Integer(count as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
