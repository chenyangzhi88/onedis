use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct HincrbyFloat {
    key: String,
    field: String,
    increment: f64,
}

impl HincrbyFloat {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hincrbyfloat' command",
            ));
        }

        let increment = args[3]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR value is not a valid float"))?;
        if !increment.is_finite() {
            return Err(Error::msg("ERR value is not a valid float"));
        }

        Ok(HincrbyFloat {
            key: args[1].to_string(),
            field: args[2].to_string(),
            increment,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_increment_by_float(&self.key, &self.field, self.increment) {
            Ok(value) => Ok(Frame::bulk_string(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_increment_by_float_async(&self.key, &self.field, self.increment)
            .await
        {
            Ok(value) => Ok(Frame::bulk_string(value)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
