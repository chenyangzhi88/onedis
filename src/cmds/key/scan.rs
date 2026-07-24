use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Scan {
    cursor: u64,
    pattern: Option<String>,
    count: Option<u64>,
    type_filter: Option<String>,
}

impl Scan {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.is_empty() {
            return Err(Error::msg("SCAN command requires at least one argument"));
        }

        let cursor = args[0].parse::<u64>()?;

        let mut pattern = None;
        let mut count = None;
        let mut type_filter = None;

        let mut i = 1;
        while i < args.len() {
            let arg = &args[i].to_ascii_uppercase();
            if arg == "MATCH" {
                if i + 1 >= args.len() {
                    return Err(Error::msg("MATCH option requires an argument"));
                }
                pattern = Some(args[i + 1].clone());
                i += 2;
            } else if arg == "COUNT" {
                if i + 1 >= args.len() {
                    return Err(Error::msg("COUNT option requires an argument"));
                }
                let parsed = args[i + 1].parse::<u64>()?;
                if parsed == 0 {
                    return Err(Error::msg("ERR syntax error"));
                }
                count = Some(parsed);
                i += 2;
            } else if arg == "TYPE" {
                if type_filter.is_some() || i + 1 >= args.len() {
                    return Err(Error::msg("ERR syntax error"));
                }
                type_filter = Some(args[i + 1].to_ascii_lowercase());
                i += 2;
            } else {
                return Err(Error::msg(format!("Unknown option: {}", args[i])));
            }
        }

        Ok(Scan {
            cursor,
            pattern,
            count,
            type_filter,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        // 默认匹配模式为 "*"
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        // 默认返回数量为 10
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        // 获取所有匹配的键
        let mut matched_keys: Vec<String> = db.keys(&pattern);
        if let Some(type_filter) = self.type_filter {
            matched_keys.retain(|key| db.type_name_readonly(key) == type_filter);
        }

        // 根据游标确定返回的键
        let start_index =
            usize::try_from(self.cursor).map_err(|_| Error::msg("ERR invalid cursor"))?;
        let end_index = start_index.saturating_add(count).min(matched_keys.len());

        // 获取要返回的键
        let keys_to_return = if start_index < matched_keys.len() {
            matched_keys[start_index..end_index].to_vec()
        } else {
            vec![]
        };

        // 计算下一个游标
        let next_cursor = if end_index >= matched_keys.len() {
            0 // 如果已经遍历完所有键，返回0表示结束
        } else {
            end_index as u64 // 否则返回下一个位置作为游标
        };

        // 构造返回结果：第一个元素是游标，第二个元素是键数组
        let keys_frames: Vec<Frame> = keys_to_return.into_iter().map(Frame::bulk_string).collect();
        let result_array = vec![
            Frame::bulk_string(next_cursor.to_string()),
            Frame::Array(keys_frames),
        ];

        Ok(Frame::Array(result_array))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;
        let mut matched_keys: Vec<String> = db.keys_async(&pattern).await;
        if let Some(type_filter) = self.type_filter {
            let mut filtered = Vec::with_capacity(matched_keys.len());
            for key in matched_keys {
                if db.type_name_readonly_async(&key).await == type_filter {
                    filtered.push(key);
                }
            }
            matched_keys = filtered;
        }
        let start_index =
            usize::try_from(self.cursor).map_err(|_| Error::msg("ERR invalid cursor"))?;
        let end_index = start_index.saturating_add(count).min(matched_keys.len());
        let keys_to_return = if start_index < matched_keys.len() {
            matched_keys[start_index..end_index].to_vec()
        } else {
            vec![]
        };
        let next_cursor = if end_index >= matched_keys.len() {
            0
        } else {
            end_index as u64
        };
        let keys_frames: Vec<Frame> = keys_to_return.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(vec![
            Frame::bulk_string(next_cursor.to_string()),
            Frame::Array(keys_frames),
        ]))
    }
}
