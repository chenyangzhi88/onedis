use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS},
    store::db::Db,
};
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
                if parsed > MAX_ARRAY_ELEMENTS as u64 {
                    return Err(Error::msg("ERR COUNT exceeds configured response limit"));
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

        let (next_cursor, keys_to_return) =
            db.scan_keys_page(self.cursor, &pattern, count, self.type_filter.as_deref())?;

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
        let (next_cursor, keys_to_return) = db
            .scan_keys_page_async(self.cursor, &pattern, count, self.type_filter.as_deref())
            .await?;
        let keys_frames: Vec<Frame> = keys_to_return.into_iter().map(Frame::bulk_string).collect();
        Ok(Frame::Array(vec![
            Frame::bulk_string(next_cursor.to_string()),
            Frame::Array(keys_frames),
        ]))
    }
}
