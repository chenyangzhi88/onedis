use anyhow::Error;

use crate::{
    cmds::sorted_set::zrange::{LexBound, parse_lex_bound},
    frame::Frame,
    store::db::Db,
};

pub struct Zrangestore {
    destination: String,
    source: String,
    range: ZrangestoreBounds,
    reverse: bool,
    limit: Option<(usize, usize)>,
}

enum ZrangestoreBounds {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

#[derive(Clone, Copy)]
struct ScoreBound {
    value: f64,
    inclusive: bool,
}

impl Zrangestore {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'zrangestore' command",
            ));
        }

        let mut by_score = false;
        let mut by_lex = false;
        let mut reverse = false;
        let mut limit = None;
        let mut idx = 5;
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
                "WITHSCORES" => return Err(Error::msg("ERR syntax error")),
                "REV" => {
                    if reverse {
                        return Err(Error::msg("ERR syntax error"));
                    }
                    reverse = true;
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
            let first = parse_score_bound(&args[3])?;
            let second = parse_score_bound(&args[4])?;
            if reverse {
                ZrangestoreBounds::Score(second, first)
            } else {
                ZrangestoreBounds::Score(first, second)
            }
        } else if by_lex {
            let first = parse_lex_bound(&args[3])?;
            let second = parse_lex_bound(&args[4])?;
            if reverse {
                ZrangestoreBounds::Lex(second, first)
            } else {
                ZrangestoreBounds::Lex(first, second)
            }
        } else {
            ZrangestoreBounds::Rank(
                args[3]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
                args[4]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            )
        };

        Ok(Zrangestore {
            destination: args[1].to_string(),
            source: args[2].to_string(),
            range,
            reverse,
            limit,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.range {
            ZrangestoreBounds::Rank(start, stop) => {
                db.zset_range(&self.source, start, stop, self.reverse)
            }
            ZrangestoreBounds::Score(min, max) => db
                .zset_range_by_score(&self.source, min.value, max.value)
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
            ZrangestoreBounds::Lex(min, max) => {
                db.zset_range_by_lex(&self.source, &min, &max)
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

        match result.and_then(|entries| db.zset_store_entries(&self.destination, entries)) {
            Ok(count) => Ok(Frame::Integer(count as i64)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let result = match self.range {
            ZrangestoreBounds::Rank(start, stop) => {
                db.zset_range_async(&self.source, start, stop, self.reverse)
                    .await
            }
            ZrangestoreBounds::Score(min, max) => db
                .zset_range_by_score_async(&self.source, min.value, max.value)
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
            ZrangestoreBounds::Lex(min, max) => db
                .zset_range_by_lex_async(&self.source, &min, &max)
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
            Ok(entries) => match db
                .zset_store_entries_async(&self.destination, entries)
                .await
            {
                Ok(count) => Ok(Frame::Integer(count as i64)),
                Err(err) => Ok(Frame::Error(err.to_string())),
            },
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
