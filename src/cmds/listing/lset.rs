use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Lset {
    pub key: String,
    pub index: isize,  // 索引，支持负数索引
    pub value: String, // 要设置的值
}

impl Lset {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();

        if args.len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'lset' command",
            ));
        }

        let key = args[1].to_string(); // 键
        let index = args[2]
            .parse::<isize>()
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?; // 索引
        let value = args[3].to_string(); // 要设置的值

        Ok(Lset { key, index, value })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.list_set(&self.key, self.index as i64, &self.value) {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .list_set_async(&self.key, self.index as i64, &self.value)
            .await
        {
            Ok(()) => Ok(Frame::SimpleString("OK".to_string())),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
