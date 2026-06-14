use anyhow::Error;

use crate::{cmds::sorted_set::common::parse_numkeys_command, frame::Frame, store::db::Db};

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
            if frame.get_arg(idx).unwrap().eq_ignore_ascii_case("LIMIT")
                && idx + 1 < frame.arg_len()
            {
                limit = Some(
                    frame
                        .get_arg(idx + 1)
                        .unwrap()
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
        match db.zset_union_or_inter(
            &self.keys,
            &vec![1.0; self.keys.len()],
            crate::store::db::ZsetAggregate::Sum,
            true,
        ) {
            Ok(entries) => Ok(Frame::Integer(
                self.limit
                    .map_or(entries.len(), |limit| entries.len().min(limit)) as i64,
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .zset_union_or_inter_async(
                &self.keys,
                &vec![1.0; self.keys.len()],
                crate::store::db::ZsetAggregate::Sum,
                true,
            )
            .await
        {
            Ok(entries) => Ok(Frame::Integer(
                self.limit
                    .map_or(entries.len(), |limit| entries.len().min(limit)) as i64,
            )),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
