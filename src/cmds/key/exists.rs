use crate::{frame::Frame, store::db::Db};
use anyhow::Error;

pub struct Exists {
    pub keys: Vec<String>,
}

impl Exists {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = frame.get_args_from_index(1);
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'exists' command",
            ));
        }
        Ok(Exists { keys })
    }

    pub fn new(keys: Vec<String>) -> Self {
        Exists { keys }
    }

    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let count = self
            .keys
            .into_iter()
            .filter(|key| db.exists_readonly(key))
            .count() as i64;
        Ok(Frame::Integer(count))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut count = 0i64;
        for key in self.keys {
            if db.exists_readonly_async(&key).await {
                count += 1;
            }
        }
        Ok(Frame::Integer(count))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cmds::key::exists::Exists,
        frame::Frame,
        store::{
            db::{Db, Structure},
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
            "onedis-exists-test-{}",
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
    fn exists_counts_all_existing_keys() {
        let db = test_db();
        db.insert("k1".to_string(), Structure::String("v1".to_string()));
        db.insert("k2".to_string(), Structure::String("v2".to_string()));

        let frame = Exists::new(vec![
            "k1".to_string(),
            "missing".to_string(),
            "k2".to_string(),
        ])
        .apply(&db)
        .unwrap();

        assert!(matches!(frame, Frame::Integer(2)));
    }

    #[test]
    fn exists_parse_accepts_multiple_keys() {
        let frame = Frame::Array(vec![
            Frame::bulk_string("EXISTS"),
            Frame::bulk_string("k1"),
            Frame::bulk_string("k2"),
        ]);

        let cmd = Exists::parse_from_frame(frame).unwrap();
        assert_eq!(cmd.keys, vec!["k1".to_string(), "k2".to_string()]);
    }
}
