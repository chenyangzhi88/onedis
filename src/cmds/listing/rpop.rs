use anyhow::Error;

use crate::{
    cmds::listing::{list_array, validate_response_count},
    frame::Frame,
    store::db::Db,
};

pub struct Rpop {
    pub key: String,
    count: Option<usize>,
}

impl Rpop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() != 2 && args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'rpop' command",
            ));
        }

        let key = args[1].to_string(); // 键

        let count = args
            .get(2)
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| Error::msg("ERR value is out of range, must be positive"))
            })
            .transpose()?;
        if let Some(count) = count {
            validate_response_count(count)?;
        }

        Ok(Rpop { key, count })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if let Some(count) = self.count {
            return match db.list_multi_pop(std::slice::from_ref(&self.key), false, count) {
                Ok(Some((_, values))) => list_array(values),
                Ok(None) => Ok(Frame::Array(Vec::new())),
                Err(err) => Ok(Frame::Error(err.to_string())),
            };
        }
        match db.list_pop_right(&self.key) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if let Some(count) = self.count {
            return match db
                .list_multi_pop_async(std::slice::from_ref(&self.key), false, count)
                .await
            {
                Ok(Some((_, values))) => list_array(values),
                Ok(None) => Ok(Frame::Array(Vec::new())),
                Err(err) => Ok(Frame::Error(err.to_string())),
            };
        }
        match db.list_pop_right_async(&self.key).await {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
