use crate::{
    frame::Frame,
    store::db::{Db, Structure},
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
        match db.get(&self.key) {
            Some(structure) => match structure {
                Structure::String(str_value) => match str_value.parse::<f64>() {
                    Ok(current) => {
                        let new_value = current + self.increment;
                        let formatted = Self::format_float(new_value);
                        db.update(self.key, Structure::String(formatted.clone()));
                        Ok(Frame::bulk_string(formatted))
                    }
                    Err(_) => {
                        let e = "ERR value is not a valid float";
                        Ok(Frame::Error(e.to_string()))
                    }
                },
                _ => {
                    let e = "WRONGTYPE Operation against a key holding the wrong kind of value";
                    Ok(Frame::Error(e.to_string()))
                }
            },
            None => {
                let formatted = Self::format_float(self.increment);
                db.insert(self.key.clone(), Structure::String(formatted.clone()));
                Ok(Frame::bulk_string(formatted))
            }
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        match db.get_string_async(&self.key).await? {
            Some(str_value) => match str_value.parse::<f64>() {
                Ok(current) => {
                    let new_value = current + self.increment;
                    let formatted = Self::format_float(new_value);
                    db.set_string_bytes_async(
                        self.key,
                        formatted.clone().into_bytes(),
                        crate::store::db::SetExpiration::KeepTtl,
                        crate::store::db::SetCondition::Always,
                        false,
                    )
                    .await?;
                    Ok(Frame::bulk_string(formatted))
                }
                Err(_) => Ok(Frame::Error("ERR value is not a valid float".to_string())),
            },
            None => {
                let formatted = Self::format_float(self.increment);
                db.insert_string_async(self.key, formatted.clone(), None)
                    .await;
                Ok(Frame::bulk_string(formatted))
            }
        }
    }
}
