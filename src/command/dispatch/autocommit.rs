use super::*;

pub fn handle_command_autocommit(db: &Db, command: Command) -> Result<Frame, Error> {
    let txn_db = db.transactional_view()?;
    let frame = match handle_command(&txn_db, command) {
        Ok(frame) => frame,
        Err(error) => {
            txn_db.discard_transaction();
            return Err(error);
        }
    };
    if matches!(frame, Frame::Error(_)) {
        txn_db.discard_transaction();
        return Ok(frame);
    }
    txn_db.commit_transaction()?;
    Ok(frame)
}

pub async fn handle_command_autocommit_async(db: &Db, command: Command) -> Result<Frame, Error> {
    let txn_db = db.transactional_view()?;
    let frame = match handle_command_async(&txn_db, command).await {
        Ok(frame) => frame,
        Err(error) => {
            txn_db.discard_transaction();
            return Err(error);
        }
    };
    if matches!(frame, Frame::Error(_)) {
        txn_db.discard_transaction();
        return Ok(frame);
    }
    txn_db.commit_transaction_async().await?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    };
    use std::{path::PathBuf, sync::Arc, time::SystemTime};

    fn frame(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn test_db(prefix: &str) -> Db {
        let unique = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"))
            .join(format!("{prefix}-{unique}"));
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    fn write_then_error_command() -> Command {
        Command::parse_from_frame(frame(&[
            "EVAL",
            "redis.call('SET', 'autocommit:key', 'value'); return redis.error_reply('ERR forced')",
            "0",
        ]))
        .unwrap()
    }

    fn assert_key_was_rolled_back(db: &Db) {
        let get = Command::parse_from_frame(frame(&["GET", "autocommit:key"])).unwrap();
        assert!(matches!(handle_command(db, get).unwrap(), Frame::Null));
    }

    #[test]
    fn sync_autocommit_discards_writes_when_command_returns_error_frame() {
        let _lua_guard = crate::lua::LUA_TEST_LOCK.lock().unwrap();
        let db = test_db("onedis-command-autocommit-sync");

        assert!(matches!(
            handle_command_autocommit(&db, write_then_error_command()).unwrap(),
            Frame::Error(message) if message == "ERR forced"
        ));
        assert_key_was_rolled_back(&db);
    }

    #[test]
    fn async_autocommit_discards_writes_when_command_returns_error_frame() {
        let _lua_guard = crate::lua::LUA_TEST_LOCK.lock().unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = test_db("onedis-command-autocommit-async");

            assert!(matches!(
                handle_command_autocommit_async(&db, write_then_error_command())
                    .await
                    .unwrap(),
                Frame::Error(message) if message == "ERR forced"
            ));
            assert_key_was_rolled_back(&db);
        });
    }
}
