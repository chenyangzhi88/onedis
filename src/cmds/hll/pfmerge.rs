use crate::{frame::Frame, store::db::Db};
use anyhow::Error;
pub struct Pfmerge {
    dest: String,
    keys: Vec<String>,
}
impl Pfmerge {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'pfmerge' command",
            ));
        }
        Ok(Self {
            dest: frame.get_arg(1).unwrap(),
            keys: (2..frame.arg_len())
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
        let members = set.into_iter().collect::<Vec<_>>();
        db.delete_key(&self.dest);
        match db.set_add(&self.dest, &members) {
            Ok(_) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut set = std::collections::HashSet::new();
        for key in self.keys {
            match db.set_members_async(&key).await {
                Ok(members) => set.extend(members),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        let members = set.into_iter().collect::<Vec<_>>();
        db.delete_key_async(&self.dest).await;
        match db.set_add_async(&self.dest, &members).await {
            Ok(_) => Ok(Frame::Ok),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
