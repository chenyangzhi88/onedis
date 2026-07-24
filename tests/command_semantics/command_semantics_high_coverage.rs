use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use onedis_server::args::ResolvedArgs;
use onedis_server::cmds::connect::client::Client;
use onedis_server::cmds::server::config::Config;
use onedis_server::command::Command;
use onedis_server::frame::Frame;
use onedis_server::store::db::{Db, Structure};
use onedis_server::store::kv_store::KvStore;
use onedis_server::store::ttl::{TtlConfig, TtlManager, VersionCounter};

fn test_root(prefix: &str) -> PathBuf {
    let unique = format!(
        "{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/onedis-test-data"));
    base.join(unique)
}

fn test_db(prefix: &str) -> Db {
    let root = test_root(prefix);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn frame_args(args: &[&str]) -> Frame {
    Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    )
}

#[track_caller]
fn parse(args: &[&str]) -> Command {
    Command::parse_from_frame(frame_args(args)).unwrap()
}

#[track_caller]
fn apply(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command(db, parse(args)).unwrap()
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command_async(db, parse(args))
        .await
        .unwrap()
}

#[track_caller]
fn parse_err(args: &[&str]) -> String {
    match Command::parse_from_frame(frame_args(args)) {
        Ok(command) => panic!("expected parse error for {}", command.name()),
        Err(err) => err.to_string(),
    }
}

#[track_caller]
fn bulk(frame: Frame) -> String {
    match frame {
        Frame::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
        other => panic!("expected bulk string, got {}", other),
    }
}

fn array(frame: Frame) -> Vec<Frame> {
    match frame {
        Frame::Array(values) => values,
        other => panic!("expected array, got {}", other),
    }
}

fn contains_bulk(frame: &Frame, expected: &str) -> bool {
    matches!(frame, Frame::BulkString(bytes) if bytes.as_slice() == expected.as_bytes())
        || matches!(frame, Frame::Array(values) if values.iter().any(|value| contains_bulk(value, expected)))
}

fn first_integer(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::Integer(value) => Some(*value),
        Frame::Array(values) => values.first().and_then(first_integer),
        _ => None,
    }
}

#[track_caller]
fn client_apply(args: &[&str]) -> Frame {
    Client::parse_from_frame(frame_args(args))
        .unwrap()
        .apply()
        .unwrap()
}

#[track_caller]
fn config_apply(args: &[&str]) -> Frame {
    let resolved = ResolvedArgs {
        config: "config/onedis.toml".to_string(),
        requirepass: Some("secret".to_string()),
        bind: "127.0.0.1".to_string(),
        databases: 16,
        hz: 10.5,
        port: 6380,
        loglevel: "debug".to_string(),
        maxclients: 1024,
        observability_enabled: false,
        metrics_bind: "127.0.0.1".to_string(),
        metrics_port: 0,
        slow_command_threshold_ms: 10,
    };
    Config::parse_from_frame(frame_args(args))
        .unwrap()
        .apply(&resolved)
        .unwrap()
}

async fn wait_async_total(db: &Db, args: &[&str], expected: i64) -> Frame {
    let mut last = Frame::Null;
    for _ in 0..50 {
        last = apply_async(db, args).await;
        if first_integer(&last) == Some(expected) {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("expected total {expected}, got {}", last);
}

async fn expect_async_ok(db: &Db, args: &[&str]) -> Frame {
    let frame = apply_async(db, args).await;
    assert!(
        !matches!(frame, Frame::Error(_)),
        "{args:?} returned {}",
        frame
    );
    frame
}

mod string_hash_list {
    use super::*;
    include!("high_coverage/string_hash_list.rs");
}

mod stream {
    use super::*;
    include!("high_coverage/stream.rs");
}

mod dispatch {
    use super::*;
    include!("high_coverage/dispatch.rs");
}

mod full_text_vector_extension {
    use super::*;
    include!("high_coverage/full_text_vector_extension.rs");
}

mod concurrency {
    use super::*;
    include!("high_coverage/concurrency.rs");
}

mod low_coverage_core {
    use super::*;
    include!("high_coverage/low_coverage_core.rs");
}

mod low_coverage_options {
    use super::*;
    include!("high_coverage/low_coverage_options.rs");
}
