use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration, SetOutcome},
};
use anyhow::Error;

pub struct GetSet {
    pub key: String,
    pub value: String,
}

impl GetSet {
    /// 从 Frame 解析出 GetSet 命令
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        // 确保参数数量正确（key + value）
        if frame.arg_len() != 3 {
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
        match db.set_string_bytes(
            self.key,
            self.value.into_bytes(),
            SetExpiration::Clear,
            SetCondition::Always,
            true,
        )? {
            SetOutcome::Set {
                old_value: Some(value),
            } => Ok(Frame::BulkString(value)),
            SetOutcome::Set { old_value: None } => Ok(Frame::Null),
            SetOutcome::NotSet => unreachable!("unconditional GETSET must write"),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .set_string_bytes_async(
                self.key,
                self.value.into_bytes(),
                SetExpiration::Clear,
                SetCondition::Always,
                true,
            )
            .await?
        {
            SetOutcome::Set {
                old_value: Some(value),
            } => Ok(Frame::BulkString(value)),
            SetOutcome::Set { old_value: None } => Ok(Frame::Null),
            SetOutcome::NotSet => unreachable!("unconditional GETSET must write"),
        }
    }
}
