use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration, Structure},
};
use anyhow::Error;

pub struct Append {
    pub key: String,
    pub val: String,
}

impl Append {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let val = frame.get_arg(2);

        if key.is_none() || val.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'append' command",
            ));
        }

        let key_str = key.unwrap().to_string(); // 键
        let val_str = val.unwrap().to_string(); // 值

        Ok(Append {
            key: key_str,
            val: val_str,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let existing_value = match db.get(&self.key) {
            Some(Structure::String(s)) => s,
            Some(_) => return Err(Error::msg("ERR wrong type for 'append' command")),
            None => String::new(),
        };
        let new_value = format!("{}{}", existing_value, self.val);
        let len = new_value.len();
        db.insert(self.key, Structure::String(new_value));
        Ok(Frame::Integer(len as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut value = db
            .get_string_bytes_async(&self.key)
            .await?
            .unwrap_or_default();
        value.extend_from_slice(self.val.as_bytes());
        let len = value.len();
        db.set_string_bytes_async(
            self.key,
            value,
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )
        .await?;
        Ok(Frame::Integer(len as i64))
    }
}
