use anyhow::Error;

use crate::{
    cmds::sorted_set::common::validate_entry_count,
    frame::{Frame, MAX_FRAME_BYTES},
    store::db::{Db, ZsetScoreWindow},
};

pub struct Zrange {
    key: String,
    range: ZrangeBounds,
    reverse: bool,
    limit: Option<(i64, i64)>,
    withscores: bool,
}

enum ZrangeBounds {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

#[derive(Clone, Copy)]
pub(crate) struct ScoreBound {
    pub(crate) value: f64,
    pub(crate) inclusive: bool,
}

#[derive(Clone)]
pub(crate) enum LexBound {
    NegInfinity,
    PosInfinity,
    Value { value: String, inclusive: bool },
}

impl Zrange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrange' command",
            ));
        }

        let key = args[1].to_string();
        let mut by_score = false;
        let mut by_lex = false;
        let mut reverse = false;
        let mut withscores = false;
        let mut limit = None;
        let mut idx = 4;
        while idx < args.len() {
            match args[idx].to_ascii_uppercase().as_str() {
                "BYSCORE" => {
                    if by_score || by_lex {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    by_score = true;
                    idx += 1;
                }
                "BYLEX" => {
                    if by_score || by_lex {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    by_lex = true;
                    idx += 1;
                }
                "REV" => {
                    if reverse {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    reverse = true;
                    idx += 1;
                }
                "WITHSCORES" => {
                    if withscores {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    withscores = true;
                    idx += 1;
                }
                "LIMIT" => {
                    if limit.is_some() || idx + 2 >= args.len() {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    let offset = args[idx + 1]
                        .parse::<i64>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    let count = args[idx + 2]
                        .parse::<i64>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    limit = Some((offset, count));
                    idx += 3;
                }
                _ => return Err(Error::msg("ERR syntax error")),
            }
        }

        if limit.is_some() && !(by_score || by_lex) {
            return Err(Error::msg("ERR syntax error"));
        }
        if let Some((_, count)) = limit
            && count >= 0
        {
            validate_entry_count(count as u64, withscores)?;
        }

        let range = if by_score {
            let first = parse_score_bound(&args[2])?;
            let second = parse_score_bound(&args[3])?;
            if reverse {
                ZrangeBounds::Score(second, first)
            } else {
                ZrangeBounds::Score(first, second)
            }
        } else if by_lex {
            let first = parse_lex_bound(&args[2])?;
            let second = parse_lex_bound(&args[3])?;
            if reverse {
                ZrangeBounds::Lex(second, first)
            } else {
                ZrangeBounds::Lex(first, second)
            }
        } else {
            ZrangeBounds::Rank(
                args[2]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                args[3]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            )
        };

        Ok(Self {
            key,
            range,
            reverse,
            limit,
            withscores,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.range {
            ZrangeBounds::Rank(start, stop) => db.zset_range(&self.key, start, stop, self.reverse),
            ZrangeBounds::Score(min, max) => db.zset_range_by_score_window(ZsetScoreWindow {
                key: &self.key,
                min: min.value,
                min_inclusive: min.inclusive,
                max: max.value,
                max_inclusive: max.inclusive,
                reverse: self.reverse,
                limit: self.limit,
            }),
            ZrangeBounds::Lex(min, max) => {
                db.zset_range_by_lex_window(&self.key, &min, &max, self.reverse, self.limit)
            }
        };
        match result {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.range {
            ZrangeBounds::Rank(start, stop) => {
                db.zset_range_async(&self.key, start, stop, self.reverse)
                    .await
            }
            ZrangeBounds::Score(min, max) => {
                db.zset_range_by_score_window_async(ZsetScoreWindow {
                    key: &self.key,
                    min: min.value,
                    min_inclusive: min.inclusive,
                    max: max.value,
                    max_inclusive: max.inclusive,
                    reverse: self.reverse,
                    limit: self.limit,
                })
                .await
            }
            ZrangeBounds::Lex(min, max) => {
                db.zset_range_by_lex_window_async(&self.key, &min, &max, self.reverse, self.limit)
                    .await
            }
        };
        match result {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores)?)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

pub(crate) fn parse_score_bound(input: &str) -> Result<ScoreBound, Error> {
    let (value, inclusive) = if let Some(value) = input.strip_prefix('(') {
        (value, false)
    } else {
        (input, true)
    };
    let value = value
        .parse::<f64>()
        .map_err(|_| Error::msg("ERR min or max is not a float"))?;
    if value.is_nan() {
        return Err(Error::msg("ERR min or max is not a float"));
    }
    Ok(ScoreBound { value, inclusive })
}

pub(crate) fn flatten_entries(
    entries: Vec<(String, f64)>,
    withscores: bool,
) -> Result<Vec<Frame>, Error> {
    validate_entry_count(entries.len() as u64, withscores)?;
    let mut frames =
        Vec::with_capacity(entries.len().saturating_mul(if withscores { 2 } else { 1 }));
    let mut bytes = 32usize;
    for (member, score) in entries {
        let score = score.to_string();
        bytes = bytes
            .checked_add(
                member
                    .len()
                    .saturating_add(if withscores { score.len() } else { 0 })
                    .saturating_add(if withscores { 64 } else { 32 }),
            )
            .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
            .ok_or_else(|| Error::msg("ERR response exceeds configured limit"))?;
        frames.push(Frame::bulk_string(member));
        if withscores {
            frames.push(Frame::bulk_string(score));
        }
    }
    Ok(frames)
}

pub(crate) fn parse_lex_bound(input: &str) -> Result<LexBound, Error> {
    match input {
        "-" => Ok(LexBound::NegInfinity),
        "+" => Ok(LexBound::PosInfinity),
        _ => {
            let mut chars = input.chars();
            let Some(prefix) = chars.next() else {
                return Err(Error::msg("ERR min or max not valid string range item"));
            };
            let inclusive = match prefix {
                '[' => true,
                '(' => false,
                _ => return Err(Error::msg("ERR min or max not valid string range item")),
            };
            Ok(LexBound::Value {
                value: chars.collect(),
                inclusive,
            })
        }
    }
}
