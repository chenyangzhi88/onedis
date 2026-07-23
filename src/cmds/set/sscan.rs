use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Sscan {
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<u64>,
}

impl Sscan {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.len() < 2 {
            return Err(Error::msg("SSCAN command requires at least two arguments"));
        }

        let key = args[0].clone();
        let cursor = args[1].parse::<u64>()?;

        let mut pattern = None;
        let mut count = None;

        let mut i = 2;
        while i < args.len() {
            let arg = &args[i].to_uppercase();
            if arg == "MATCH" {
                if i + 1 >= args.len() {
                    return Err(Error::msg("MATCH option requires an argument"));
                }
                pattern = Some(args[i + 1].clone());
                i += 2;
            } else if arg == "COUNT" {
                if i + 1 >= args.len() {
                    return Err(Error::msg("COUNT option requires an argument"));
                }
                count = Some(args[i + 1].parse::<u64>()?);
                i += 2;
            } else {
                return Err(Error::msg(format!("Unknown option: {}", args[i])));
            }
        }

        Ok(Sscan {
            key,
            cursor,
            pattern,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = self.count.unwrap_or(10) as usize;

        match db.set_scan(&self.key, self.cursor, &pattern, count) {
            Ok((next_cursor, members)) => Ok(Frame::Array(vec![
                Frame::bulk_string(next_cursor.to_string()),
                Frame::Array(members.into_iter().map(Frame::bulk_string).collect()),
            ])),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = self.count.unwrap_or(10) as usize;

        match db
            .set_scan_async(&self.key, self.cursor, &pattern, count)
            .await
        {
            Ok((next_cursor, members)) => Ok(Frame::Array(vec![
                Frame::bulk_string(next_cursor.to_string()),
                Frame::Array(members.into_iter().map(Frame::bulk_string).collect()),
            ])),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
