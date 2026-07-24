use crate::{
    frame::Frame,
    store::db::{Db, SetCondition, SetExpiration},
};
use anyhow::Error;

pub struct IncrbyFloat {
    pub key: String,
    pub increment: f64,
}

impl IncrbyFloat {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args();
        if args.len() != 3 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'incrbyfloat' command",
            ));
        }
        let key = args[1].to_string();
        let increment = args[2]
            .parse::<f64>()
            .map_err(|_| Error::msg("ERR value is not a valid float"))?;
        if !increment.is_finite() {
            return Err(Error::msg("ERR value is not a valid float"));
        }
        Ok(IncrbyFloat { key, increment })
    }

    // 改进的浮点数格式化函数
    pub fn format_float(value: f64) -> String {
        // 处理特殊值
        if value.is_nan() {
            return "nan".to_string();
        }
        if value.is_infinite() {
            return if value.is_sign_positive() {
                "inf".to_string()
            } else {
                "-inf".to_string()
            };
        }

        // 四舍五入到小数点后10位以避免精度问题
        let rounded = (value * 1e10).round() / 1e10;

        // 处理整数情况
        if rounded.fract().abs() < f64::EPSILON {
            return rounded.trunc().to_string();
        }

        // 格式化为字符串并去除尾部多余的零
        let mut s = format!("{:.10}", rounded);
        while s.contains('.') && (s.ends_with('0') || s.ends_with('.')) {
            let len = s.len();
            if s.ends_with('.') {
                s.truncate(len - 1);
                break;
            } else {
                s.truncate(len - 1);
            }
        }
        s
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let current = match db.get_string_bytes(&self.key) {
            Ok(Some(value)) => std::str::from_utf8(&value)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite())
                .ok_or_else(|| Error::msg("ERR value is not a valid float")),
            Ok(None) => Ok(0.0),
            Err(err) => Err(err),
        };
        let current = match current {
            Ok(current) => current,
            Err(err) => return Ok(Frame::Error(err.to_string())),
        };
        let next = current + self.increment;
        if !next.is_finite() {
            return Ok(Frame::Error(
                "ERR increment would produce NaN or Infinity".to_string(),
            ));
        }
        let formatted = Self::format_float(next);
        db.set_string_bytes(
            self.key,
            formatted.as_bytes().to_vec(),
            SetExpiration::KeepTtl,
            SetCondition::Always,
            false,
        )?;
        Ok(Frame::bulk_string(formatted))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let increment = self.increment;
        match db
            .mutate_string_bytes_async(&self.key, |bytes, exists| {
                let current = if !exists {
                    0.0
                } else {
                    std::str::from_utf8(bytes)
                        .ok()
                        .and_then(|value| value.parse::<f64>().ok())
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| Error::msg("ERR value is not a valid float"))?
                };
                let next = current + increment;
                if !next.is_finite() {
                    return Err(Error::msg("ERR increment would produce NaN or Infinity"));
                }
                let formatted = Self::format_float(next);
                bytes.clear();
                bytes.extend_from_slice(formatted.as_bytes());
                Ok(formatted)
            })
            .await
        {
            Ok(formatted) => Ok(Frame::bulk_string(formatted)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}
