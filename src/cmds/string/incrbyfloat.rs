use crate::{frame::Frame, store::db::Db};
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

    pub fn format_float(value: f64) -> String {
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

        // Rust's shortest-roundtrip formatting preserves the full f64 value. Artificially
        // rounding here loses valid Redis increments such as 1e-12.
        let mut formatted = value.to_string();
        if formatted == "-0" {
            formatted = "0".to_string();
        }
        formatted
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let increment = self.increment;
        match db.mutate_string_bytes(&self.key, |bytes, exists| {
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
        }) {
            Ok(formatted) => Ok(Frame::bulk_string(formatted)),
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
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
