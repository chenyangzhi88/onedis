use anyhow::Error;

use crate::{
    cmds::hash::common::checked_random_entries,
    frame::{Frame, MAX_ARRAY_ELEMENTS},
    store::db::Db,
};

pub struct Hrandfield {
    key: String,
    count: Option<i64>,
    with_values: bool,
}

impl Hrandfield {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 2 || args.len() > 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hrandfield' command",
            ));
        }
        let mut count = None;
        let mut with_values = false;
        if args.len() >= 3 {
            if args[2].eq_ignore_ascii_case("WITHVALUES") {
                return Err(Error::msg("ERR syntax error"));
            }
            count = Some(
                args[2]
                    .parse::<i64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?,
            );
        }
        if args.len() == 4 {
            if !args[3].eq_ignore_ascii_case("WITHVALUES") {
                return Err(Error::msg("ERR syntax error"));
            }
            with_values = true;
        }
        if let Some(count) = count {
            let max_count = if with_values {
                MAX_ARRAY_ELEMENTS / 2
            } else {
                MAX_ARRAY_ELEMENTS
            };
            if count.unsigned_abs() > max_count as u64 {
                return Err(Error::msg("ERR count exceeds configured response limit"));
            }
        }

        Ok(Hrandfield {
            key: args[1].to_string(),
            count,
            with_values,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_random_fields_bytes(&self.key, self.count, self.with_values) {
            Ok(Some(entries)) if self.count.is_none() => {
                let Some((field, _)) = entries.into_iter().next() else {
                    return Ok(Frame::Null);
                };
                Ok(Frame::bulk_string(field))
            }
            Ok(Some(entries)) => checked_random_entries(entries),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_random_fields_bytes_async(&self.key, self.count, self.with_values)
            .await
        {
            Ok(Some(entries)) if self.count.is_none() => {
                let Some((field, _)) = entries.into_iter().next() else {
                    return Ok(Frame::Null);
                };
                Ok(Frame::bulk_string(field))
            }
            Ok(Some(entries)) => checked_random_entries(entries),
            Ok(None) if self.count.is_none() => Ok(Frame::Null),
            Ok(None) => Ok(Frame::Array(Vec::new())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
