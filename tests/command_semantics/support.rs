#![allow(unused)]

pub(crate) use std::path::PathBuf;
pub(crate) use std::sync::Arc;
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) use onedis_server::args::ResolvedArgs;
pub(crate) use onedis_server::cmds::key::r#move::Move;
pub(crate) use onedis_server::cmds::key::persist::Persist;
pub(crate) use onedis_server::cmds::key::rename::Rename;
pub(crate) use onedis_server::cmds::key::ttl::Ttl;
pub(crate) use onedis_server::cmds::server::bgsave::Bgsave;
pub(crate) use onedis_server::cmds::server::save::Save;
pub(crate) use onedis_server::cmds::string::append::Append;
pub(crate) use onedis_server::cmds::string::getset::GetSet;
pub(crate) use onedis_server::cmds::string::incrby::Incrby;
pub(crate) use onedis_server::cmds::string::setrange::SetRange;
pub(crate) use onedis_server::cmds::wasm::WasmCommand;
pub(crate) use onedis_server::command::Command;
pub(crate) use onedis_server::command_executor::CommandExecutor;
pub(crate) use onedis_server::frame::Frame;
pub(crate) use onedis_server::network::connection::Connection;
pub(crate) use onedis_server::network::session::Session;
pub(crate) use onedis_server::network::session_manager::SessionManager;
pub(crate) use onedis_server::server::Handler;
pub(crate) use onedis_server::store::db::{Db, Structure};
pub(crate) use onedis_server::store::db_manager::DatabaseManager;
pub(crate) use onedis_server::store::kv_store::KvStore;
pub(crate) use onedis_server::store::ttl::{TtlConfig, TtlManager, VersionCounter};
pub(crate) use onedis_server::wasm::WasmRegistry;
pub(crate) use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(crate) use tokio::net::{TcpListener, TcpStream};

pub(crate) fn test_root(unique: String) -> PathBuf {
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"));
    base.join(unique)
}

pub(crate) fn test_db() -> Db {
    let unique = format!(
        "onedis-command-smoke-{}",
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
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

pub(crate) fn frame_args(args: &[&str]) -> Frame {
    Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    )
}

pub(crate) fn apply_command(db: &Db, args: &[&str]) -> Frame {
    let frame = frame_args(args);
    let command = Command::parse_from_frame(frame).unwrap();
    db.handle_command(command).unwrap()
}

pub(crate) async fn apply_command_async(db: &Db, args: &[&str]) -> Frame {
    let frame = frame_args(args);
    let command = Command::parse_from_frame(frame).unwrap();
    db.handle_command_async(command).await.unwrap()
}

pub(crate) fn apply_frame(db: &Db, frame: Frame) -> Frame {
    let command = Command::parse_from_frame(frame).unwrap();
    db.handle_command(command).unwrap()
}

pub(crate) fn array_contains_bulk(frame: &Frame, expected: &str) -> bool {
    match frame {
        Frame::Array(values) => values
            .iter()
            .any(|value| matches!(value, Frame::BulkString(text) if text.as_slice() == expected.as_bytes())),
        _ => false,
    }
}

pub(crate) fn test_args_with_databases(databases: usize) -> Arc<ResolvedArgs> {
    let unique = format!(
        "onedis-handler-smoke-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = test_root(unique);
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(ResolvedArgs {
        config: "config/onedis.toml".to_string(),
        requirepass: None,
        bind: "127.0.0.1".to_string(),
        databases,
        hz: 10.0,
        port: 0,
        loglevel: "info".to_string(),
        maxclients: 0,
    })
}

pub(crate) fn test_args_with_config(
    databases: usize,
    requirepass: Option<&str>,
) -> Arc<ResolvedArgs> {
    let unique = format!(
        "onedis-config-smoke-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = test_root(unique);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    let config = root.join("onedis.toml");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        &config,
        format!(
            r#"
[db]
path = "{}"

[wal]
dir = "{}"

[onedis_server]
port = 0
bind = "127.0.0.1"
databases = {}
hz = 10.0
loglevel = "info"
maxclients = 0
"#,
            db_path.display(),
            wal_dir.display(),
            databases
        ),
    )
    .unwrap();

    let mut args = (*test_args_with_databases(databases)).clone();
    args.config = config.to_string_lossy().into_owned();
    args.requirepass = requirepass.map(ToString::to_string);
    Arc::new(args)
}

pub(crate) fn test_args_with_requirepass(requirepass: &str) -> Arc<ResolvedArgs> {
    test_args_with_config(1, Some(requirepass))
}

pub(crate) fn test_store(engine_id: u32) -> KvStore {
    let unique = format!(
        "onedis-store-smoke-{}",
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
    KvStore::new(db_path, wal_dir, engine_id)
}

pub(crate) async fn connected_streams() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = listener.accept();
    let connect = TcpStream::connect(addr);
    let (accepted, connected) = tokio::join!(accept, connect);
    let (server_stream, _) = accepted.unwrap();
    (server_stream, connected.unwrap())
}
