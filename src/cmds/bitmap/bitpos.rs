use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Bitpos {
    key: String,
    bit: u8,
    start: Option<i64>,
    end: Option<i64>,
}
impl Bitpos {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 || frame.arg_len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bitpos' command",
            ));
        }
        Ok(Self {
            key: frame.get_arg(1).unwrap(),
            bit: frame
                .get_arg(2)
                .unwrap()
                .parse()
                .map_err(|_| Error::msg("ERR bit is not an integer or out of range"))?,
            start: if frame.arg_len() >= 4 {
                Some(
                    frame
                        .get_arg(3)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                )
            } else {
                None
            },
            end: if frame.arg_len() >= 5 {
                Some(
                    frame
                        .get_arg(4)
                        .unwrap()
                        .parse()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                )
            } else {
                None
            },
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitpos(&self.key, self.bit, self.start, self.end)
            .map(Frame::Integer)
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitpos_async(&self.key, self.bit, self.start, self.end)
            .await
            .map(Frame::Integer)
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
