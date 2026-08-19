use crate::{
    frame::Frame,
    store::db::{Db, SetCondition},
};
use anyhow::Error;

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

pub struct JsonMGet {
    pub keys: Vec<String>,
    pub path: String,
}

pub struct JsonMSet {
    pub entries: Vec<(String, String, String)>,
}

pub struct JsonNumIncrBy {
    pub key: String,
    pub path: String,
    pub increment: f64,
}

pub struct JsonStrAppend {
    pub key: String,
    pub path: String,
    pub value: String,
}

pub struct JsonArrAppend {
    pub key: String,
    pub path: String,
    pub values: Vec<serde_json::Value>,
}

pub struct JsonArrInsert {
    pub key: String,
    pub path: String,
    pub index: i64,
    pub values: Vec<serde_json::Value>,
}

pub struct JsonArrPop {
    pub key: String,
    pub path: String,
    pub index: i64,
}

pub struct JsonObjKeys {
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

        let condition = if frame.arg_len() == 4 {
            SetCondition::Always
        } else {
            match frame
                .get_arg(4)
                .ok_or_else(|| Error::msg("ERR syntax error"))?
                .to_ascii_uppercase()
                .as_str()
            {
                "NX" => SetCondition::Nx,
                "XX" => SetCondition::Xx,
                _ => return Err(Error::msg("ERR syntax error")),
            }
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
            path: optional_path(&frame)?,
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
            path: optional_path(&frame)?,
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
            path: optional_path(&frame)?,
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

fn optional_path(frame: &Frame) -> Result<String, Error> {
    if frame.arg_len() == 2 {
        Ok("$".to_string())
    } else {
        frame
            .get_arg(2)
            .ok_or_else(|| Error::msg("ERR invalid JSON path"))
    }
}

impl JsonMGet {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.mget' command",
            ));
        }
        let mut args = frame.get_args_from_index(1);
        let path = args
            .pop()
            .ok_or_else(|| Error::msg("ERR invalid JSON path"))?;
        if args.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.mget' command",
            ));
        }
        Ok(Self { keys: args, path })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let values = self
            .keys
            .iter()
            .map(|key| db.json_get(key, &self.path))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json_optional_values_frame(values))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let commands = self
            .keys
            .iter()
            .map(|key| (key.as_str(), self.path.as_str()))
            .collect::<Vec<_>>();
        let values = db.json_get_batch_async(&commands).await;
        let mut frames = Vec::with_capacity(values.len());
        for value in values {
            match value? {
                Some(value) => frames.push(Frame::bulk_string(value)),
                None => frames.push(Frame::Null),
            }
        }
        Ok(Frame::Array(frames))
    }
}

impl JsonMSet {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 || !(frame.arg_len() - 1).is_multiple_of(3) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.mset' command",
            ));
        }
        let mut entries = Vec::with_capacity((frame.arg_len() - 1) / 3);
        let mut index = 1;
        while index < frame.arg_len() {
            entries.push((
                frame
                    .get_arg(index)
                    .ok_or_else(|| Error::msg("ERR invalid UTF-8 key"))?,
                frame
                    .get_arg(index + 1)
                    .ok_or_else(|| Error::msg("ERR invalid JSON path"))?,
                String::from_utf8(
                    frame
                        .get_arg_bytes(index + 2)
                        .ok_or_else(|| Error::msg("ERR invalid JSON value"))?,
                )
                .map_err(|_| Error::msg("ERR invalid JSON value"))?,
            ));
            index += 3;
        }
        // Parse all values before executing so malformed input never causes a
        // partial multi-key mutation.
        for (_, _, value) in &entries {
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|_| Error::msg("ERR invalid JSON value"))?;
        }
        Ok(Self { entries })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        for (key, path, value) in self.entries {
            if !db.json_set(&key, &path, &value, SetCondition::Always)? {
                return Ok(Frame::Error("ERR path does not exist".to_string()));
            }
        }
        Ok(Frame::Ok)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let entries = self
            .entries
            .iter()
            .map(|(key, path, value)| (key.as_str(), path.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        db.json_mset_atomic_async(&entries).await?;
        Ok(Frame::Ok)
    }
}

impl JsonNumIncrBy {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        require_arity(&frame, 4, "json.numincrby")?;
        let increment = frame
            .get_arg(3)
            .ok_or_else(|| Error::msg("ERR value is not a valid number"))?
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR value is not a valid number"))?;
        if !increment.is_finite() {
            return Err(Error::msg("ERR value is not a valid number"));
        }
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path: required_arg(&frame, 2, "ERR invalid JSON path")?,
            increment,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let Some(raw) = db.json_get(&self.key, &self.path)? else {
            return Ok(Frame::Null);
        };
        let mut value: serde_json::Value = serde_json::from_str(&raw)?;
        let result = increment_json_number(&mut value, self.increment)?;
        db.json_set(
            &self.key,
            &self.path,
            &serde_json::to_string(&value)?,
            SetCondition::Always,
        )?;
        Ok(Frame::bulk_string(format_json_number(result)))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let increment = self.increment;
        Ok(
            match db
                .json_update_value_async(&self.key, &self.path, move |value| {
                    increment_json_number(value, increment)
                })
                .await?
            {
                Some(value) => Frame::bulk_string(format_json_number(value)),
                None => Frame::Null,
            },
        )
    }
}

impl JsonStrAppend {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        require_arity(&frame, 4, "json.strappend")?;
        let raw = required_arg(&frame, 3, "ERR invalid JSON string")?;
        let value: String =
            serde_json::from_str(&raw).map_err(|_| Error::msg("ERR expected a JSON string"))?;
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path: required_arg(&frame, 2, "ERR invalid JSON path")?,
            value,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        json_sync_update(db, &self.key, &self.path, move |value| {
            let text = value
                .as_str()
                .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not a string"))?;
            let combined = format!("{text}{}", self.value);
            let len = combined.len();
            *value = serde_json::Value::String(combined);
            Ok(Frame::Integer(len as i64))
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let append = self.value;
        Ok(db
            .json_update_value_async(&self.key, &self.path, move |value| {
                let text = value
                    .as_str()
                    .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not a string"))?;
                let combined = format!("{text}{append}");
                let len = combined.len();
                *value = serde_json::Value::String(combined);
                Ok(Frame::Integer(len as i64))
            })
            .await?
            .unwrap_or(Frame::Null))
    }
}

impl JsonArrAppend {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.arrappend' command",
            ));
        }
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path: required_arg(&frame, 2, "ERR invalid JSON path")?,
            values: parse_json_values(&frame, 3)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        json_sync_update(db, &self.key, &self.path, move |value| {
            let array = value
                .as_array_mut()
                .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not an array"))?;
            array.extend(self.values);
            Ok(Frame::Integer(array.len() as i64))
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let values = self.values;
        Ok(db
            .json_update_value_async(&self.key, &self.path, move |value| {
                let array = value
                    .as_array_mut()
                    .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not an array"))?;
                array.extend(values);
                Ok(Frame::Integer(array.len() as i64))
            })
            .await?
            .unwrap_or(Frame::Null))
    }
}

impl JsonArrInsert {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() < 5 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.arrinsert' command",
            ));
        }
        let index = required_arg(&frame, 3, "ERR index is not an integer")?
            .parse::<i64>()
            .map_err(|_| Error::msg("ERR index is not an integer"))?;
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path: required_arg(&frame, 2, "ERR invalid JSON path")?,
            index,
            values: parse_json_values(&frame, 4)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        json_sync_update(db, &self.key, &self.path, move |value| {
            Ok(Frame::Integer(
                insert_json_array(value, self.index, self.values)? as i64,
            ))
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let index = self.index;
        let values = self.values;
        Ok(db
            .json_update_value_async(&self.key, &self.path, move |value| {
                Ok(Frame::Integer(
                    insert_json_array(value, index, values)? as i64
                ))
            })
            .await?
            .unwrap_or(Frame::Null))
    }
}

impl JsonArrPop {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if !(2..=4).contains(&frame.arg_len()) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.arrpop' command",
            ));
        }
        let path = if frame.arg_len() >= 3 {
            required_arg(&frame, 2, "ERR invalid JSON path")?
        } else {
            "$".to_string()
        };
        let index = if frame.arg_len() == 4 {
            required_arg(&frame, 3, "ERR index is not an integer")?
                .parse::<i64>()
                .map_err(|_| Error::msg("ERR index is not an integer"))?
        } else {
            -1
        };
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path,
            index,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        json_sync_update(db, &self.key, &self.path, move |value| {
            pop_json_array(value, self.index)
        })
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let index = self.index;
        Ok(db
            .json_update_value_async(&self.key, &self.path, move |value| {
                pop_json_array(value, index)
            })
            .await?
            .unwrap_or(Frame::Null))
    }
}

impl JsonObjKeys {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if !(2..=3).contains(&frame.arg_len()) {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'json.objkeys' command",
            ));
        }
        Ok(Self {
            key: required_arg(&frame, 1, "ERR invalid UTF-8 key")?,
            path: optional_path(&frame)?,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        json_objkeys_frame(db.json_get(&self.key, &self.path)?)
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        json_objkeys_frame(db.json_get_async(&self.key, &self.path).await?)
    }
}

fn require_arity(frame: &Frame, arity: usize, command: &str) -> Result<(), Error> {
    if frame.arg_len() != arity {
        return Err(Error::msg(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    Ok(())
}

fn required_arg(frame: &Frame, index: usize, message: &'static str) -> Result<String, Error> {
    frame.get_arg(index).ok_or_else(|| Error::msg(message))
}

fn parse_json_values(frame: &Frame, start: usize) -> Result<Vec<serde_json::Value>, Error> {
    (start..frame.arg_len())
        .map(|index| {
            let raw = frame
                .get_arg_bytes(index)
                .ok_or_else(|| Error::msg("ERR invalid JSON value"))?;
            serde_json::from_slice(&raw).map_err(|_| Error::msg("ERR invalid JSON value"))
        })
        .collect()
}

fn json_optional_values_frame(values: Vec<Option<String>>) -> Frame {
    Frame::Array(
        values
            .into_iter()
            .map(|value| value.map_or(Frame::Null, Frame::bulk_string))
            .collect(),
    )
}

fn increment_json_number(value: &mut serde_json::Value, increment: f64) -> Result<f64, Error> {
    let current = value
        .as_f64()
        .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not a number"))?;
    let next = current + increment;
    if !next.is_finite() {
        return Err(Error::msg("ERR result is not a number"));
    }
    *value = serde_json::Number::from_f64(next)
        .map(serde_json::Value::Number)
        .ok_or_else(|| Error::msg("ERR result is not a number"))?;
    Ok(next)
}

fn format_json_number(value: f64) -> String {
    serde_json::Number::from_f64(value)
        .map_or_else(|| value.to_string(), |number| number.to_string())
}

fn json_sync_update<F>(db: &Db, key: &str, path: &str, update: F) -> Result<Frame, Error>
where
    F: FnOnce(&mut serde_json::Value) -> Result<Frame, Error>,
{
    let Some(raw) = db.json_get(key, path)? else {
        return Ok(Frame::Null);
    };
    let mut value: serde_json::Value = serde_json::from_str(&raw)?;
    let result = update(&mut value)?;
    db.json_set(
        key,
        path,
        &serde_json::to_string(&value)?,
        SetCondition::Always,
    )?;
    Ok(result)
}

fn normalize_array_index(index: i64, len: usize, allow_end: bool) -> Result<usize, Error> {
    let len_i64 = i64::try_from(len).map_err(|_| Error::msg("ERR index out of range"))?;
    let normalized = if index < 0 { len_i64 + index } else { index };
    let max = if allow_end {
        len_i64
    } else {
        len_i64.saturating_sub(1)
    };
    if normalized < 0 || normalized > max || (!allow_end && len == 0) {
        return Err(Error::msg("ERR index out of range"));
    }
    usize::try_from(normalized).map_err(|_| Error::msg("ERR index out of range"))
}

fn insert_json_array(
    value: &mut serde_json::Value,
    index: i64,
    values: Vec<serde_json::Value>,
) -> Result<usize, Error> {
    let array = value
        .as_array_mut()
        .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not an array"))?;
    let index = normalize_array_index(index, array.len(), true)?;
    array.splice(index..index, values);
    Ok(array.len())
}

fn pop_json_array(value: &mut serde_json::Value, index: i64) -> Result<Frame, Error> {
    let array = value
        .as_array_mut()
        .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not an array"))?;
    if array.is_empty() {
        return Ok(Frame::Null);
    }
    let index = normalize_array_index(index, array.len(), false)?;
    let value = array.remove(index);
    Ok(Frame::bulk_string(serde_json::to_string(&value)?))
}

fn json_objkeys_frame(raw: Option<String>) -> Result<Frame, Error> {
    let Some(raw) = raw else {
        return Ok(Frame::Null);
    };
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::msg("WRONGTYPE JSON element is not an object"))?;
    Ok(Frame::Array(
        object.keys().cloned().map(Frame::bulk_string).collect(),
    ))
}
