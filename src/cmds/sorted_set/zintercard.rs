use anyhow::Error;

use crate::{
    cmds::sorted_set::common::{parse_numkeys_command, text_arg},
    frame::Frame,
    store::db::Db,
};

pub struct Zintercard {
    keys: Vec<String>,
    limit: Option<usize>,
}

impl Zintercard {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = parse_numkeys_command(&frame, "zintercard")?;
        let mut limit = None;
        let mut idx = 2 + keys.len();
        while idx < frame.arg_len() {
            if text_arg(&frame, idx)?.eq_ignore_ascii_case("LIMIT") && idx + 1 < frame.arg_len() {
                if limit.is_some() {
                    return Err(Error::msg("ERR syntax error"));
                }
                limit = Some(
                    text_arg(&frame, idx + 1)?
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                );
                idx += 2;
            } else {
                return Err(Error::msg("ERR syntax error"));
            }
        }
        Ok(Self { keys, limit })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_intersection_card(&self.keys, self.limit.unwrap_or(0)) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_intersection_card_async(&self.keys, self.limit.unwrap_or(0))
            .await
        {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
