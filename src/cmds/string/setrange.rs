use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, Structure},
};

pub struct SetRange {
    pub key: String,
    pub offset: i64,
    pub value: Vec<u8>,
}

impl SetRange {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let key = frame.get_arg(1);
        let offset = frame.get_arg(2);
        if frame.arg_len() != 4 || key.is_none() || offset.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'setrange' command",
            ));
        }

        let final_key = key.unwrap().to_string();
        let final_offset = offset.unwrap().to_string();
        let final_value = frame
            .get_arg_bytes(3)
            .ok_or_else(|| Error::msg("ERR missing value"))?;

        let offset_int = match final_offset.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        if offset_int < 0 {
            return Err(Error::msg("ERR offset is out of range, must be positive"));
        }

        Ok(SetRange {
            key: final_key,
            offset: offset_int,
            value: final_value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        // 获取当前值，如果不存在则创建一个空字符串
        let current_value = match db.get(&self.key) {
            Some(Structure::String(s)) => s,
            Some(_) => {
                return Err(Error::msg(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => String::new(),
        };

        // 将字符串转换为字节向量以便操作
        let mut bytes = current_value.into_bytes();
        let offset = self.offset as usize;
        let value_bytes = self.value;

        // 确保字节数组足够长以容纳新数据
        if bytes.len() < offset + value_bytes.len() {
            bytes.resize(offset + value_bytes.len(), 0);
        }

        // 在指定偏移处写入新值
        for (i, byte) in value_bytes.iter().enumerate() {
            bytes[offset + i] = *byte;
        }

        // The legacy synchronous API stores strings as UTF-8.
        let new_value = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                return Err(Error::msg(
                    "ERR invalid UTF-8 sequence produced by SETRANGE operation",
                ));
            }
        };

        // 保存到数据库
        db.insert(self.key.clone(), Structure::String(new_value));

        // 返回修改后的字符串长度
        let length = db.get(&self.key).map_or(0, |s| {
            if let Structure::String(str) = s {
                str.len()
            } else {
                0
            }
        });

        Ok(Frame::Integer(length as i64))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let offset = self.offset as usize;
        let value = self.value;
        let length = db
            .mutate_string_bytes_async(&self.key, |bytes, _| {
                if value.is_empty() {
                    return Ok(bytes.len());
                }
                let required_len = offset
                    .checked_add(value.len())
                    .ok_or_else(|| Error::msg("ERR string exceeds maximum allowed size"))?;
                if required_len > bytes.len() {
                    bytes
                        .try_reserve_exact(required_len - bytes.len())
                        .map_err(|_| Error::msg("ERR string exceeds maximum allowed size"))?;
                    bytes.resize(required_len, 0);
                }
                bytes[offset..required_len].copy_from_slice(&value);
                Ok(bytes.len())
            })
            .await?;

        Ok(Frame::Integer(length as i64))
    }
}
