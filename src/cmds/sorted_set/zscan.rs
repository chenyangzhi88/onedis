use anyhow::Error;

use crate::{cmds::sorted_set::zrange::flatten_entries, frame::Frame, store::db::Db};

pub struct Zscan {
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<u64>,
}

impl Zscan {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.len() < 2 {
            return Err(Error::msg("ZSCAN command requires at least two arguments"));
        }

        let key = args[0].clone();
        let cursor = args[1]
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        let mut pattern = None;
        let mut count = None;
        let mut i = 2;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case("MATCH") {
                if i + 1 >= args.len() {
                    return Err(Error::msg("MATCH option requires an argument"));
                }
                pattern = Some(args[i + 1].clone());
                i += 2;
            } else if args[i].eq_ignore_ascii_case("COUNT") {
                if i + 1 >= args.len() {
                    return Err(Error::msg("COUNT option requires an argument"));
                }
                let parsed = args[i + 1]
                    .parse::<u64>()
                    .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
                if parsed == 0 {
                    return Err(Error::msg("ERR syntax error"));
                }
                count = Some(parsed);
                i += 2;
            } else {
                return Err(Error::msg(format!("Unknown option: {}", args[i])));
            }
        }

        Ok(Self {
            key,
            cursor,
            pattern,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        match db.zset_scan(&self.key, self.cursor, &pattern, count) {
            Ok((next_cursor, entries)) => Ok(Frame::Array(vec![
                Frame::bulk_string(next_cursor.to_string()),
                Frame::Array(flatten_entries(entries, true)),
            ])),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        match db
            .zset_scan_async(&self.key, self.cursor, &pattern, count)
            .await
        {
            Ok((next_cursor, entries)) => Ok(Frame::Array(vec![
                Frame::bulk_string(next_cursor.to_string()),
                Frame::Array(flatten_entries(entries, true)),
            ])),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
