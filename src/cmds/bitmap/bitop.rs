use crate::{cmds::bitmap::text_arg, frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Bitop {
    op: String,
    dest: String,
    keys: Vec<String>,
}
impl Bitop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'bitop' command",
            ));
        }
        let op = text_arg(&frame, 1)?.to_ascii_uppercase();
        if !matches!(op.as_str(), "AND" | "OR" | "XOR" | "NOT") {
            return Err(Error::msg("ERR syntax error"));
        }
        if op == "NOT" && frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR BITOP NOT must be called with a single source key",
            ));
        }
        let keys = (3..frame.arg_len())
            .map(|idx| text_arg(&frame, idx))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            op,
            dest: text_arg(&frame, 2)?,
            keys,
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitop(&self.op, &self.dest, &self.keys)
            .map(|len| Frame::Integer(len as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        db.string_bitop_async(&self.op, &self.dest, &self.keys)
            .await
            .map(|len| Frame::Integer(len as i64))
            .or_else(|e| Ok(Frame::Error(e.to_string())))
    }
}
