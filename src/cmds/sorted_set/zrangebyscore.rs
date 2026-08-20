use anyhow::Error;

use crate::{
    cmds::sorted_set::{
        common::{text_arg, validate_entry_count},
        zrange::{ScoreBound, flatten_entries, parse_score_bound},
    },
    frame::Frame,
    store::db::{Db, ZsetScoreWindow},
};

pub struct Zrangebyscore {
    key: String,
    min: ScoreBound,
    max: ScoreBound,
    withscores: bool,
    limit: Option<(i64, i64)>,
    reverse: bool,
}

impl Zrangebyscore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_range_by_score(frame, false, "zrangebyscore")
    }

    pub(crate) fn parse_reverse(frame: Frame) -> Result<Self, Error> {
        parse_range_by_score(frame, true, "zrevrangebyscore")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_range_by_score_window(ZsetScoreWindow {
            key: &self.key,
            min: self.min.value,
            min_inclusive: self.min.inclusive,
            max: self.max.value,
            max_inclusive: self.max.inclusive,
            reverse: self.reverse,
            limit: self.limit,
        }) {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_range_by_score_window_async(ZsetScoreWindow {
                key: &self.key,
                min: self.min.value,
                min_inclusive: self.min.inclusive,
                max: self.max.value,
                max_inclusive: self.max.inclusive,
                reverse: self.reverse,
                limit: self.limit,
            })
            .await
        {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn parse_range_by_score(
    frame: Frame,
    reverse: bool,
    command: &str,
) -> Result<Zrangebyscore, Error> {
    if frame.arg_len() < 4 {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    let first = parse_score_bound(&text_arg(&frame, 2)?)?;
    let second = parse_score_bound(&text_arg(&frame, 3)?)?;
    let (min, max) = if reverse {
        (second, first)
    } else {
        (first, second)
    };
    let mut withscores = false;
    let mut limit = None;
    let mut index = 4;
    while index < frame.arg_len() {
        match text_arg(&frame, index)?.to_ascii_uppercase().as_str() {
            "WITHSCORES" if !withscores => {
                withscores = true;
                index += 1;
            }
            "LIMIT" if limit.is_none() && index + 2 < frame.arg_len() => {
                let offset = text_arg(&frame, index + 1)?
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                let count = text_arg(&frame, index + 2)?
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                if count >= 0 {
                    validate_entry_count(count as u64, withscores)?;
                }
                limit = Some((offset, count));
                index += 3;
            }
            _ => return Err(Error::msg("ERR syntax error")),
        }
    }
    if let Some((_, count)) = limit
        && count >= 0
    {
        validate_entry_count(count as u64, withscores)?;
    }
    Ok(Zrangebyscore {
        key: text_arg(&frame, 1)?,
        min,
        max,
        withscores,
        limit,
        reverse,
    })
}
