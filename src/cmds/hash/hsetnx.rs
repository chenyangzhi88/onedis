use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hsetnx {
    key: String,
    field: String,
    value: Vec<u8>,
}

impl Hsetnx {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let field = frame.get_arg(2);
        let value = frame.get_arg_bytes(3);

        if frame.arg_len() != 4 || key.is_none() || field.is_none() || value.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hsetnx' command",
            ));
        }

        let final_key = key.unwrap().to_string();
        let final_field = field.unwrap().to_string();
        let final_value = value.unwrap();

        Ok(Hsetnx {
            key: final_key,
            field: final_field,
            value: final_value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_set_nx_bytes(&self.key, &self.field, &self.value) {
            Ok(inserted) => Ok(Frame::Integer(if inserted { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .hash_set_nx_bytes_async(&self.key, &self.field, &self.value)
            .await
        {
            Ok(inserted) => Ok(Frame::Integer(if inserted { 1 } else { 0 })),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
