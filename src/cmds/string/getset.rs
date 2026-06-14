use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct GetSet {
    pub key: String,
    pub value: String,
}

impl GetSet {
    /// 从 Frame 解析出 GetSet 命令
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        // 确保参数数量正确（key + value）
        if frame.get_args().len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getset' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or(Error::msg("ERR missing key"))?
            .to_string();
        let value = frame
            .get_arg(2)
            .ok_or(Error::msg("ERR missing value"))?
            .to_string();

        Ok(GetSet { key, value })
    }

    /// 应用 GetSet 命令到数据库
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        // 获取旧值（同时检查类型）
        let old_value = db.get_string(&self.key).unwrap_or(None);

        // 插入新值（覆盖旧值）
        db.insert_string(self.key.clone(), self.value.clone(), None);

        // TODO 是否移除过期时间

        // 返回结果：旧值或 nil
        match old_value {
            Some(val) => Ok(Frame::bulk_string(val)),
            None => Ok(Frame::Null),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let old_value = db.get_string_async(&self.key).await?;
        db.insert_string_async(self.key, self.value, None).await;
        match old_value {
            Some(val) => Ok(Frame::bulk_string(val)),
            None => Ok(Frame::Null),
        }
    }
}
