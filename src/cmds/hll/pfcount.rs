use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Pfcount {
    keys: Vec<String>,
}
impl Pfcount {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfcount' command",
            ));
        }
        Ok(Self {
            keys: (1..frame.arg_len())
                .map(|i| frame.get_arg(i).unwrap())
                .collect(),
        })
    }
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut set = std::collections::HashSet::new();
        for key in self.keys {
            match db.set_members(&key) {
                Ok(members) => set.extend(members),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Integer(set.len() as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut set = std::collections::HashSet::new();
        for key in self.keys {
            match db.set_members_async(&key).await {
                Ok(members) => set.extend(members),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Integer(set.len() as i64))
    }
}
