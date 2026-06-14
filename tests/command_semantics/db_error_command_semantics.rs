use onedis_server::{
    command::Command,
    frame::Frame,
    store::{
        db::Db,
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    },
};
use std::sync::Arc;
use tempfile::TempDir;

fn make_command(args: &[&str]) -> Command {
    let frame = Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    );
    Command::parse_from_frame(frame).expect("failed to parse command")
}

fn make_store() -> (TempDir, KvStore) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = dir.path().join("kv_engine").join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    (dir, KvStore::new(root, wal_dir, 1))
}

#[test]
fn db_command_errors_are_returned_to_caller() {
    let (_dir, store) = make_store();
    let vc = Arc::new(VersionCounter::new());
    let ttl = TtlManager::new(store.clone(), TtlConfig::default());
    let db = Db::new(0, store, vc, ttl);
    let command = make_command(&["RENAME", "missing-key", "new-key"]);

    let result = db.handle_command(command);
    match result {
        Err(e) => assert_eq!(e.to_string(), "ERR no such key"),
        Ok(Frame::Error(message)) => assert_eq!(message, "ERR no such key"),
        Ok(other) => panic!("expected error, got {}", other.to_string()),
    }
}
