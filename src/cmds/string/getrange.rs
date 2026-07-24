use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct GetRange {
    key: String,
    start: i64,
    end: i64,
    substr_alias: bool,
}

impl GetRange {
    pub(crate) fn command_name(&self) -> &'static str {
        if self.substr_alias {
            "SUBSTR"
        } else {
            "GETRANGE"
        }
    }

    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        if frame.arg_len() != 4 {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getrange' command",
            ));
        }
        let key = frame.get_arg(1);
        let start = frame.get_arg(2);
        let end = frame.get_arg(3);

        if key.is_none() || start.is_none() || end.is_none() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'getrange' command",
            ));
        }

        let final_key = key.unwrap().to_string();
        let final_start = start.unwrap().to_string();
        let final_end = end.unwrap().to_string();

        let start_int = match final_start.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        let end_int = match final_end.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
        };

        Ok(GetRange {
            key: final_key,
            start: start_int,
            end: end_int,
            substr_alias: frame
                .get_arg(0)
                .is_some_and(|name| name.eq_ignore_ascii_case("SUBSTR")),
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let value = db.get_string_bytes(&self.key)?.unwrap_or_default();
        Ok(Frame::bulk_string(byte_range(value, self.start, self.end)))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let value = db
            .get_string_bytes_async(&self.key)
            .await?
            .unwrap_or_default();
        Ok(Frame::bulk_string(byte_range(value, self.start, self.end)))
    }
}

fn byte_range(value: Vec<u8>, start: i64, end: i64) -> Vec<u8> {
    let len = value.len() as i64;
    if len == 0 {
        return Vec::new();
    }
    let normalize = |index: i64| {
        if index < 0 {
            (len + index).max(0)
        } else {
            index
        }
    };
    let start = normalize(start);
    let end = normalize(end).min(len - 1);
    if start >= len || start > end {
        return Vec::new();
    }
    value[start as usize..=end as usize].to_vec()
}

#[cfg(test)]
mod tests {
    use super::byte_range;

    #[test]
    fn ranges_are_inclusive_and_use_bytes_not_utf8_characters() {
        assert_eq!(byte_range(b"abcdef".to_vec(), 1, 3), b"bcd");
        assert_eq!(byte_range(b"abcdef".to_vec(), -3, -1), b"def");
        assert_eq!(byte_range(b"abcdef".to_vec(), 9, 12), b"");
        assert_eq!(byte_range("你a".as_bytes().to_vec(), 0, 0), vec![0xe4]);
    }
}
