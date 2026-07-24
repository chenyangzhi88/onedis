use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Zrange {
    key: String,
    range: ZrangeBounds,
    reverse: bool,
    limit: Option<(usize, usize)>,
    withscores: bool,
}

enum ZrangeBounds {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

#[derive(Clone, Copy)]
struct ScoreBound {
    value: f64,
    inclusive: bool,
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
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                    let count = args[idx + 2]
                        .parse::<usize>()
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
            ZrangeBounds::Score(min, max) => db
                .zset_range_by_score(&self.key, min.value, max.value)
                .map(|mut entries| {
                    entries.retain(|(_, score)| score_in_range(*score, min, max));
                    if self.reverse {
                        entries.reverse();
                    }
                    if let Some((offset, count)) = self.limit {
                        entries = entries.into_iter().skip(offset).take(count).collect();
                    }
                    entries
                }),
            ZrangeBounds::Lex(min, max) => {
                db.zset_range_by_lex(&self.key, &min, &max)
                    .map(|mut entries| {
                        if self.reverse {
                            entries.reverse();
                        }
                        if let Some((offset, count)) = self.limit {
                            entries = entries.into_iter().skip(offset).take(count).collect();
                        }
                        entries
                    })
            }
        };
        match result {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.range {
            ZrangeBounds::Rank(start, stop) => {
                db.zset_range_async(&self.key, start, stop, self.reverse)
                    .await
            }
            ZrangeBounds::Score(min, max) => db
                .zset_range_by_score_async(&self.key, min.value, max.value)
                .await
                .map(|mut entries| {
                    entries.retain(|(_, score)| score_in_range(*score, min, max));
                    if self.reverse {
                        entries.reverse();
                    }
                    if let Some((offset, count)) = self.limit {
                        entries = entries.into_iter().skip(offset).take(count).collect();
                    }
                    entries
                }),
            ZrangeBounds::Lex(min, max) => db
                .zset_range_by_lex_async(&self.key, &min, &max)
                .await
                .map(|mut entries| {
                    if self.reverse {
                        entries.reverse();
                    }
                    if let Some((offset, count)) = self.limit {
                        entries = entries.into_iter().skip(offset).take(count).collect();
                    }
                    entries
                }),
        };
        match result {
            Ok(entries) => Ok(Frame::Array(flatten_entries(entries, self.withscores))),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

fn parse_score_bound(input: &str) -> Result<ScoreBound, Error> {
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

fn score_in_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    (score > min.value || (min.inclusive && score == min.value))
        && (score < max.value || (max.inclusive && score == max.value))
}

pub(crate) fn flatten_entries(entries: Vec<(String, f64)>, withscores: bool) -> Vec<Frame> {
    let mut frames = Vec::new();
    for (member, score) in entries {
        frames.push(Frame::bulk_string(member));
        if withscores {
            frames.push(Frame::bulk_string(score.to_string()));
        }
    }
    frames
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

pub(crate) fn lex_member_in_range(member: &str, min: &LexBound, max: &LexBound) -> bool {
    lex_member_above_min(member, min) && lex_member_below_max(member, max)
}

fn lex_member_above_min(member: &str, min: &LexBound) -> bool {
    match min {
        LexBound::NegInfinity => true,
        LexBound::PosInfinity => false,
        LexBound::Value { value, inclusive } => {
            if *inclusive {
                member >= value.as_str()
            } else {
                member > value.as_str()
            }
        }
    }
}

fn lex_member_below_max(member: &str, max: &LexBound) -> bool {
    match max {
        LexBound::PosInfinity => true,
        LexBound::NegInfinity => false,
        LexBound::Value { value, inclusive } => {
            if *inclusive {
                member <= value.as_str()
            } else {
                member < value.as_str()
            }
        }
    }
}
