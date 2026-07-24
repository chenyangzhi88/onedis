use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Hstrlen {
    key: String,
    field: String,
}

impl Hstrlen {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let field = frame.get_arg(2);

        if frame.arg_len() != 3 || key.is_none() || field.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'hstrlen' command",
            ));
        }

        let final_key = key.unwrap().to_string(); // 键
        let final_field = field.unwrap().to_string(); // 字段

        Ok(Hstrlen {
            key: final_key,
            field: final_field,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.get(&self.key) {
            Some(structure) => match structure {
                crate::store::db::Structure::Hash(hash) => match hash.get(&self.field) {
                    Some(value) => Ok(Frame::Integer(value.len() as i64)),
                    None => Ok(Frame::Integer(0)),
                },
                _ => {
                    let f = "ERR Operation against a key holding the wrong kind of value";
                    Ok(Frame::Error(f.to_string()))
                }
            },
            None => Ok(Frame::Integer(0)),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.hash_get_async(&self.key, &self.field).await {
            Ok(Some(value)) => Ok(Frame::Integer(value.len() as i64)),
            Ok(None) => Ok(Frame::Integer(0)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
