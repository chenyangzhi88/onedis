use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hsetnx {
    key: String,
    field: String,
    value: String,
}

impl Hsetnx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let field = frame.get_arg(2);
        let value = frame.get_arg(3);

        if key.is_none() || field.is_none() || value.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hsetnx' command",
            ));
        }

        let final_key = key.unwrap().to_string();
        let final_field = field.unwrap().to_string();
        let final_value = value.unwrap().to_string();

        Ok(Hsetnx {
            key: final_key,
            field: final_field,
            value: final_value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_nx(&self.key, &self.field, &self.value) {
            Ok(inserted) => Ok(Frame::Integer(if inserted { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_set_nx_async(&self.key, &self.field, &self.value)
            .await
        {
            Ok(inserted) => Ok(Frame::Integer(if inserted { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
