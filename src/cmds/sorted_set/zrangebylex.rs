use anyhow::Error;

use crate::{
    cmds::sorted_set::{
        common::{text_arg, validate_entry_count},
        zrange::{flatten_entries, parse_lex_bound},
    },
    frame::Frame,
    store::db::Db,
};

pub struct Zrangebylex {
    key: String,
    min: crate::cmds::sorted_set::zrange::LexBound,
    max: crate::cmds::sorted_set::zrange::LexBound,
    limit: Option<(i64, i64)>,
    reverse: bool,
}

impl Zrangebylex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_range_lex(frame, false, "zrangebylex")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_range_by_lex_window(&self.key, &self.min, &self.max, self.reverse, self.limit)
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, false)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_range_by_lex_window_async(
                &self.key,
                &self.min,
                &self.max,
                self.reverse,
                self.limit,
            )
            .await
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, false)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_range_lex(
    frame: Frame,
    reverse: bool,
    command: &str,
) -> Result<Zrangebylex, Error> {
    if frame.arg_len() != 4 && frame.arg_len() != 7 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    }
    let mut limit = None;
    if frame.arg_len() == 7 {
        if !text_arg(&frame, 4)?.eq_ignore_ascii_case("LIMIT") {
            return Err(Error::msg("ERR syntax error"));
        }
        let offset = text_arg(&frame, 5)?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let count = text_arg(&frame, 6)?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        if count >= 0 {
            validate_entry_count(count as u64, false)?;
        }
        limit = Some((offset, count));
    }
    Ok(Zrangebylex {
        key: text_arg(&frame, 1)?,
        min: parse_lex_bound(&text_arg(&frame, if reverse { 3 } else { 2 })?)?,
        max: parse_lex_bound(&text_arg(&frame, if reverse { 2 } else { 3 })?)?,
        limit,
        reverse,
    })
}
