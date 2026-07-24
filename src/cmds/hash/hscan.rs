use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Hscan {
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<u64>,
}

impl Hscan {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let args = frame.get_args_from_index(1);
        if args.len() < 2 {
            return Err(Error::msg("HSCAN command requires at least two arguments"));
        }

        let key = args[0].clone();
        let cursor = args[1].parse::<u64>()?;

        let mut pattern = None;
        let mut count = None;
        let mut i = 2;
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
            } else {
                return Err(Error::msg(format!("Unknown option: {}", args[i])));
            }
        }

        Ok(Hscan {
            key,
            cursor,
            pattern,
            count,
        })
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        match db.hash_scan(&self.key, self.cursor, &pattern, count) {
            Ok((next_cursor, entries)) => {
                let mut items = Vec::with_capacity(entries.len() * 2);
                for (field, value) in entries {
                    items.push(Frame::bulk_string(field));
                    items.push(Frame::bulk_string(value));
                }
                Ok(Frame::Array(vec![
                    Frame::bulk_string(next_cursor.to_string()),
                    Frame::Array(items),
                ]))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let pattern = self.pattern.unwrap_or_else(|| "*".to_string());
        let count = usize::try_from(self.count.unwrap_or(10))
            .map_err(|_| Error::msg("ERR value is not an integer or out of range"))?;

        match db
            .hash_scan_async(&self.key, self.cursor, &pattern, count)
            .await
        {
            Ok((next_cursor, entries)) => {
                let mut items = Vec::with_capacity(entries.len() * 2);
                for (field, value) in entries {
                    items.push(Frame::bulk_string(field));
                    items.push(Frame::bulk_string(value));
                }
                Ok(Frame::Array(vec![
                    Frame::bulk_string(next_cursor.to_string()),
                    Frame::Array(items),
                ]))
            }
            Err(err) => Ok(Frame::Error(err.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cmds::hash::hscan::Hscan,
        frame::Frame,
        store::{
            db::Db,
            kv_store::KvStore,
            ttl::{TtlConfig, TtlManager, VersionCounter},
        },
    };
    use std::path::PathBuf;
    use std::sync::Arc;

    fn test_root(unique: String) -> PathBuf {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"));
        base.join(unique)
    }

    fn test_db() -> Db {
        let unique = format!(
            "onedis-hscan-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = test_root(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let vc = Arc::new(VersionCounter::new());
        let ttl = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, vc, ttl)
    }

    #[test]
    fn hscan_returns_cursor_and_flat_field_value_array() {
        let db = test_db();
        db.hash_set("user:1", "name", "alice").unwrap();
        db.hash_set("user:1", "city", "paris").unwrap();

        let frame = Hscan {
            key: "user:1".to_string(),
            cursor: 0,
            pattern: Some("*".to_string()),
            count: Some(10),
        }
        .apply(&db)
        .unwrap();

        match frame {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Frame::BulkString(cursor) if cursor == b"0"));
                match &items[1] {
                    Frame::Array(entries) => assert_eq!(entries.len(), 4),
                    other => panic!("expected entry array, got {}", other),
                }
            }
            other => panic!("expected array frame, got {}", other),
        }
    }
}
