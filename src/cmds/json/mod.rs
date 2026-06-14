use anyhow::Error;

use crate::{
    frame::Frame,
    store::db::{Db, SetCondition},
};

pub struct JsonSet {
    pub key: String,
    pub path: String,
    pub value: String,
    pub condition: SetCondition,
}

pub struct JsonGet {
    pub key: String,
    pub path: String,
}

pub struct JsonDel {
    pub key: String,
    pub path: String,
}

pub struct JsonType {
    pub key: String,
    pub path: String,
}

impl JsonSet {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || frame.arg_len() > 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.set' command",
            ));
        }

        let key = frame
            .get_arg(1)
            .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?;
        let path = frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR invalid JSON path"))?;
        let value = String::from_utf8(
            frame
                .get_arg_bytes(3)
                .ok_or_else(|| Error::msg("ERR invalid JSON value"))?,
        )
        .map_err(|_| Error::msg("ERR invalid JSON value"))?;

        let condition = match frame.arg_len() {
            4 => SetCondition::Always,
            5 => match frame
                .get_arg(4)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .to_ascii_uppercase()
                .as_str()
            {
                "NX" => SetCondition::Nx,
                "XX" => SetCondition::Xx,
                _ => return Err(Error::msg("ERR syntax error")),
            },
            _ => unreachable!(),
        };

        Ok(JsonSet {
            key,
            path,
            value,
            condition,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.json_set(&self.key, &self.path, &self.value, self.condition) {
            Ok(true) => Ok(Frame::Ok),
            Ok(false) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db
            .json_set_async(&self.key, &self.path, &self.value, self.condition)
            .await
        {
            Ok(true) => Ok(Frame::Ok),
            Ok(false) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

impl JsonGet {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.get' command",
            ));
        }
        Ok(JsonGet {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            path: frame.get_arg(2).unwrap_or_else(|| "$".to_string()),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.json_get(&self.key, &self.path) {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.json_get_async(&self.key, &self.path).await {
            Ok(Some(value)) => Ok(Frame::bulk_string(value)),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

impl JsonDel {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.del' command",
            ));
        }
        Ok(JsonDel {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            path: frame.get_arg(2).unwrap_or_else(|| "$".to_string()),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.json_del(&self.key, &self.path) {
            Ok(deleted) => Ok(Frame::Integer(deleted)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.json_del_async(&self.key, &self.path).await {
            Ok(deleted) => Ok(Frame::Integer(deleted)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

impl JsonType {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 2 || frame.arg_len() > 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.type' command",
            ));
        }
        Ok(JsonType {
            key: frame
                .get_arg(1)
                .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
            path: frame.get_arg(2).unwrap_or_else(|| "$".to_string()),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        match db.json_type(&self.key, &self.path) {
            Ok(Some(kind)) => Ok(Frame::SimpleString(kind.to_string())),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.json_type_async(&self.key, &self.path).await {
            Ok(Some(kind)) => Ok(Frame::SimpleString(kind.to_string())),
            Ok(None) => Ok(Frame::Null),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
