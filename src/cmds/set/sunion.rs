use crate::{
    cmds::set::set_array,
    frame::Frame,
    store::db::{Db, Structure},
};
use anyhow::Error;

pub struct Sunion {
    keys: Vec<String>,
}

impl Sunion {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        // 至少需要两个键（一个命令名，一个或多个集合键）
        if args.len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sunion' command",
            ));
        }

        // 提取所有键
        let keys = args[1..].iter().map(|arg| arg.to_string()).collect();

        Ok(Sunion { keys })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let mut result_set = std::collections::HashSet::new();
        for key in self.keys {
            if let Some(structure) = db.get(&key)? {
                match structure {
                    Structure::Set(set) => {
                        for member in set.iter() {
                            result_set.insert(member.clone());
                        }
                    }
                    _ => {
                        let f = "ERR Operation against a key holding the wrong kind of value";
                        return Ok(Frame::Error(f.to_string()));
                    }
                }
            }
        }

        set_array(result_set)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.set_union_async(&self.keys).await {
            Ok(result_set) => set_array(result_set),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
