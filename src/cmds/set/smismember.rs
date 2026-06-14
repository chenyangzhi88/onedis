use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Smismember {
    key: String,
    members: Vec<String>,
}

impl Smismember {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'smismember' command",
            ));
        }
        Ok(Smismember {
            key: args[1].to_string(),
            members: args[2..].iter().map(|arg| arg.to_string()).collect(),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut result = Vec::with_capacity(self.members.len());
        for member in self.members {
            match db.set_contains(&self.key, &member) {
                Ok(exists) => result.push(Frame::Integer(exists as i64)),
                Err(err) => return Ok(Frame::Error(err.to_string())),
            }
        }
        Ok(Frame::Array(result))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let set = match db.set_members_async(&self.key).await {
            Ok(members) => members
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        Ok(Frame::Array(
            self.members
                .into_iter()
                .map(|member| Frame::Integer(set.contains(&member) as i64))
                .collect(),
        ))
    }
}
