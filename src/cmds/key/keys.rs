use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Keys {
    pattern: String,
}

impl Keys {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.len() != 1 {
            return Err(Error::msg("KEYS command requires exactly one argument"));
        }
        Ok(Keys {
            pattern: args[0].clone(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let limit = crate::resource_limits::resource_limits()?.keys_items;
        let (cursor, keys) = db.scan_keys_page(0, &self.pattern, limit, None)?;
        if cursor != 0 {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        let results: Vec<Frame> = keys.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(results))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let (cursor, keys) = db
            .scan_keys_page_async(
                0,
                &self.pattern,
                crate::resource_limits::resource_limits()?.keys_items,
                None,
            )
            .await?;
        if cursor != 0 {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        let results: Vec<Frame> = keys.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(results))
    }
}
