use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS},
    store::db::Db,
};
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
        let (cursor, keys) = db.scan_keys_page(0, &self.pattern, MAX_ARRAY_ELEMENTS, None)?;
        if cursor != 0 {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        let results: Vec<Frame> = keys.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(results))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let (cursor, keys) = db
            .scan_keys_page_async(0, &self.pattern, MAX_ARRAY_ELEMENTS, None)
            .await?;
        if cursor != 0 {
            return Err(Error::msg("ERR response exceeds configured limit"));
        }
        let results: Vec<Frame> = keys.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(results))
    }
}
