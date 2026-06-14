use anyhow::Error;

use crate::{
    cmds::sorted_set::common::{entries_with_scores, parse_numkeys_command},
    frame::Frame,
    store::db::Db,
};

pub struct Zdiff {
    keys: Vec<String>,
    withscores: bool,
}

impl Zdiff {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = parse_numkeys_command(&frame, "zdiff")?;
        let withscores_idx = 2 + keys.len();
        let withscores = match frame.arg_len() {
            len if len == withscores_idx => false,
            len if len == withscores_idx + 1 => frame
                .get_arg(withscores_idx)
                .unwrap()
                .eq_ignore_ascii_case("WITHSCORES"),
            _ => false,
        };
        if frame.arg_len() != withscores_idx && !withscores {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(Self { keys, withscores })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_diff(&self.keys) {
            Ok(entries) if self.withscores => Ok(Frame::Array(entries_with_scores(entries))),
            Ok(entries) => Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|(member, _)| Frame::bulk_string(member))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_diff_async(&self.keys).await {
            Ok(entries) if self.withscores => Ok(Frame::Array(entries_with_scores(entries))),
            Ok(entries) => Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|(member, _)| Frame::bulk_string(member))
                    .collect(),
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
