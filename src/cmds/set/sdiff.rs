use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Sdiff {
    keys: Vec<String>,
}

impl Sdiff {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        // 至少需要两个键（一个命令名，一个或多个集合键）
        if args.len() < 2 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        }

        // 提取所有键
        let keys = args[1..].iter().map(|arg| arg.to_string()).collect();

        Ok(Sdiff { keys })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        if self.keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        }

        let difference = match db.set_diff(&self.keys) {
            Ok(difference) => difference,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };

        // 将结果转换为 Frame::Array
        let members: Vec<Frame> = difference.into_iter().map(Frame::bulk_string).collect();

        Ok(Frame::Array(members))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        if self.keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        }

        let difference = match db.set_diff_async(&self.keys).await {
            Ok(difference) => difference,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };

        let members: Vec<Frame> = difference.into_iter().map(Frame::bulk_string).collect();

        Ok(Frame::Array(members))
    }
}
