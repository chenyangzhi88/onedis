use anyhow::Error;

use crate::{cmds::sorted_set::zrange::parse_lex_bound, frame::Frame, store::db::Db};

pub struct Zrangebylex {
    key: String,
    min: crate::cmds::sorted_set::zrange::LexBound,
    max: crate::cmds::sorted_set::zrange::LexBound,
    limit: Option<(usize, usize)>,
    reverse: bool,
}

impl Zrangebylex {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        parse_range_lex(frame, false, "zrangebylex")
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.zset_range_by_lex(&self.key, &self.min, &self.max) {
            Ok(mut entries) => {
                if self.reverse {
                    entries.reverse();
                }
                if let Some((offset, count)) = self.limit {
                    entries = entries.into_iter().skip(offset).take(count).collect();
                }
                Ok(Frame::Array(
                    entries
                        .into_iter()
                        .map(|(m, _)| Frame::bulk_string(m))
                        .collect(),
                ))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_range_by_lex_async(&self.key, &self.min, &self.max)
            .await
        {
            Ok(mut entries) => {
                if self.reverse {
                    entries.reverse();
                }
                if let Some((offset, count)) = self.limit {
                    entries = entries.into_iter().skip(offset).take(count).collect();
                }
                Ok(Frame::Array(
                    entries
                        .into_iter()
                        .map(|(m, _)| Frame::bulk_string(m))
                        .collect(),
                ))
            }
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
        if !frame.get_arg(4).unwrap().eq_ignore_ascii_case("LIMIT") {
            return Err(Error::msg("ERR syntax error"));
        }
        limit = Some((
            frame
                .get_arg(5)
                .unwrap()
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            frame
                .get_arg(6)
                .unwrap()
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
        ));
    }
    Ok(Zrangebylex {
        key: frame.get_arg(1).unwrap(),
        min: parse_lex_bound(&frame.get_arg(if reverse { 3 } else { 2 }).unwrap())?,
        max: parse_lex_bound(&frame.get_arg(if reverse { 2 } else { 3 }).unwrap())?,
        limit,
        reverse,
    })
}
