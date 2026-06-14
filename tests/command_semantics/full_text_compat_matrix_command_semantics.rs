use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use onedis_server::{
    command::Command,
    frame::Frame,
    store::{
        db::Db,
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    },
};
use tempfile::TempDir;

const MATRIX: &str = include_str!("../../../docs/onedis-server/full_text_redis_compat_matrix.md");

const REDIS_SEARCH_COMMANDS: &[&str] = &[
    "FT._LIST",
    "FT.AGGREGATE",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.ALTER",
    "FT.CONFIG GET",
    "FT.CONFIG SET",
    "FT.CREATE",
    "FT.CURSOR DEL",
    "FT.CURSOR READ",
    "FT.DICTADD",
    "FT.DICTDEL",
    "FT.DICTDUMP",
    "FT.DROPINDEX",
    "FT.EXPLAIN",
    "FT.EXPLAINCLI",
    "FT.HYBRID",
    "FT.INFO",
    "FT.PROFILE",
    "FT.SEARCH",
    "FT.SPELLCHECK",
    "FT.SUGADD",
    "FT.SUGDEL",
    "FT.SUGGET",
    "FT.SUGLEN",
    "FT.SYNDUMP",
    "FT.SYNUPDATE",
    "FT.TAGVALS",
];

const UNSUPPORTED_SEARCH_COMMANDS: &[&[&str]] = &[];

fn make_db() -> (TempDir, Db) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    (dir, Db::new(0, store, version_counter, ttl_manager))
}

fn command(args: &[&str]) -> Command {
    Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    ))
    .expect("failed to parse command")
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    db.handle_command(command(args)).expect("command failed")
}

fn wait_for_search_bytes(db: &Db, args: &[&str], expected_prefix: &[u8]) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = apply(db, args).as_bytes();
        if last.starts_with(expected_prefix) {
            return last;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        last.starts_with(expected_prefix),
        "last response: {}",
        String::from_utf8_lossy(&last)
    );
    last
}

fn matrix_rows() -> BTreeMap<String, String> {
    MATRIX
        .lines()
        .filter(|line| line.starts_with("| `FT."))
        .map(|line| {
            let cells = line.split('|').map(str::trim).collect::<Vec<_>>();
            let command = cells[1].trim_matches('`').to_string();
            let status = cells[2].to_string();
            (command, status)
        })
        .collect()
}

#[test]
fn full_text_compat_matrix_covers_redis_search_commands() {
    let rows = matrix_rows();
    for command in REDIS_SEARCH_COMMANDS {
        assert!(
            rows.contains_key(*command),
            "missing matrix row for {command}"
        );
    }
    for (command, status) in rows {
        assert!(
            matches!(
                status.as_str(),
                "partial" | "unsupported" | "not-applicable"
            ),
            "invalid status for {command}: {status}"
        );
    }
}

#[test]
fn unsupported_full_text_commands_return_explicit_errors() {
    let (_dir, db) = make_db();
    for args in UNSUPPORTED_SEARCH_COMMANDS {
        let frame = apply(&db, args);
        let Frame::Error(message) = frame else {
            panic!("expected error for {args:?}");
        };
        assert!(
            message.starts_with("ERR unsupported full-text command FT."),
            "unexpected error for {args:?}: {message}"
        );
    }
}

#[test]
fn full_text_resp_golden_for_supported_subset() {
    let (_dir, db) = make_db();
    assert_eq!(
        apply(
            &db,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT"
            ],
        )
        .as_bytes(),
        b"+OK\r\n"
    );
    assert_eq!(
        apply(&db, &["HSET", "doc:1", "title", "redis search"]).as_bytes(),
        b":1\r\n"
    );
    let search = wait_for_search_bytes(
        &db,
        &["FT.SEARCH", "idx", "redis", "RETURN", "1", "title"],
        b"*3\r\n:1\r\n",
    );
    assert_eq!(
        search,
        b"*3\r\n:1\r\n$5\r\ndoc:1\r\n*2\r\n$5\r\ntitle\r\n$12\r\nredis search\r\n"
    );
    assert_eq!(
        apply(&db, &["FT.AGGREGATE", "idx", "*"]).as_bytes(),
        b"*2\r\n:1\r\n*0\r\n"
    );
}

#[test]
fn optional_redis_comparison_harness_is_wired() {
    let Some(addr) = std::env::var("ONEDIS_REDIS_COMPAT_ADDR").ok() else {
        return;
    };
    let response = send_resp_command(&addr, &["PING"]).expect("redis comparison endpoint failed");
    assert!(
        response.starts_with(b"+PONG") || response.starts_with(b"$4\r\nPONG"),
        "unexpected comparison endpoint response: {}",
        String::from_utf8_lossy(&response)
    );
}

fn send_resp_command(addr: &str, args: &[&str]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut request = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        request.extend_from_slice(arg.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    stream.write_all(&request)?;
    let mut response = vec![0u8; 4096];
    let len = stream.read(&mut response)?;
    response.truncate(len);
    Ok(response)
}
