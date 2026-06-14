use anyhow::Error;

use crate::{frame::Frame, store::db::Db};

pub struct Del {
    pub keys: Vec<String>,
}

impl Del {
    /**
     * 获取键的集合
     *
     * @param frame 命令帧
     */
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let keys = frame.get_args_from_index(1);
        if keys.is_empty() {
            return Err(Error::msg(
                "ERR wrong number of arguments for 'del' command",
            ));
        }
        Ok(Del { keys: keys })
    }

    pub fn new(keys: Vec<String>) -> Self {
        Del { keys }
    }

    /**
     * 执行命令逻辑
     *
     * @param db 数据库
     */
    pub fn apply(self, db: &Db) -> Result<Frame, Error> {
        let deleted = self
            .keys
            .into_iter()
            .filter(|key| db.delete_key(key))
            .count() as i64;

        Ok(Frame::Integer(deleted))
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame, Error> {
        let mut deleted = 0i64;
        for key in self.keys {
            if db.delete_key_async(&key).await {
                deleted += 1;
            }
        }

        Ok(Frame::Integer(deleted))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cmds::key::del::Del,
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
            "onedis-del-test-{}",
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
    fn del_returns_deleted_key_count() {
        let db = test_db();
        db.insert("k1".to_string(), Structure::String("v1".to_string()));
        db.insert("k2".to_string(), Structure::String("v2".to_string()));

        let frame = Del::new(vec![
            "k1".to_string(),
            "missing".to_string(),
            "k2".to_string(),
        ])
        .apply(&db)
        .unwrap();

        assert!(matches!(frame, Frame::Integer(2)));
        assert!(db.get("k1").is_none());
        assert!(db.get("k2").is_none());
    }

    #[test]
    fn del_parse_requires_at_least_one_key() {
        let frame = Frame::Array(vec![Frame::bulk_string("DEL")]);
        assert!(Del::parse_from_frame(frame).is_err());
    }
}
