use std::{
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
    onedis_server::command_dispatch::handle_command(db, command(args)).expect("command failed")
}

fn apply_err(db: &Db, args: &[&str]) -> Frame {
    match Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    )) {
        Ok(command) => {
            onedis_server::command_dispatch::handle_command(db, command).expect("command failed")
        }
        Err(error) => Frame::Error(error.to_string()),
    }
}

fn wait_total(db: &Db, args: &[&str], expected: i64) -> Frame {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = Frame::Null;
    while Instant::now() < deadline {
        last = apply(db, args);
        if total(&last) == Some(expected) {
            return last;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("expected total {expected}, got {}", last.to_string());
}

fn total(frame: &Frame) -> Option<i64> {
    let Frame::Array(items) = frame else {
        return None;
    };
    let Some(Frame::Integer(total)) = items.first() else {
        return None;
    };
    Some(*total)
}

fn bulk_text(frame: &Frame) -> String {
    match frame {
        Frame::BulkString(value) => String::from_utf8_lossy(value).into_owned(),
        Frame::SimpleString(value) => value.clone(),
        Frame::Integer(value) => value.to_string(),
        other => other.to_string(),
    }
}

#[test]
fn ft_search_text_analysis_stopwords_stemming_chinese_and_phonetic() {
    let (_dir, db) = make_db();
    assert!(matches!(
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
                "STOPWORDS",
                "1",
                "the",
                "SCHEMA",
                "body",
                "TEXT",
                "exact",
                "TEXT",
                "NOSTEM",
                "name",
                "TEXT",
                "PHONETIC",
                "dm:en",
                "title",
                "TEXT",
                "WITHSUFFIXTRIE"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(
            &db,
            &[
                "HSET",
                "doc:1",
                "body",
                "the runner was running near 北京大学",
                "exact",
                "running",
                "name",
                "Robert",
                "title",
                "microphone"
            ],
        )
        .to_string(),
        "4"
    );
    wait_total(&db, &["FT.SEARCH", "idx", "runner"], 1);

    assert_eq!(total(&apply(&db, &["FT.SEARCH", "idx", "the"])), Some(0));
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@body:run"])),
        Some(1)
    );
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@exact:run"])),
        Some(0)
    );
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@body:北京"])),
        Some(1)
    );
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@name:Rupert"])),
        Some(1)
    );
    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@title:phone"])),
        Some(1)
    );
}

#[test]
fn ft_search_text_analysis_highlight_and_summarize_return_snippets() {
    let (_dir, db) = make_db();
    assert!(matches!(
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
                "body",
                "TEXT"
            ],
        ),
        Frame::Ok
    ));
    let body = "alpha beta gamma delta epsilon quick zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau";
    assert_eq!(
        apply(&db, &["HSET", "doc:1", "body", body]).to_string(),
        "1"
    );
    wait_total(&db, &["FT.SEARCH", "idx", "quick"], 1);

    let highlighted = wait_total(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "quick",
            "RETURN",
            "1",
            "body",
            "HIGHLIGHT",
        ],
        1,
    );
    assert!(highlighted.to_string().contains("<b>quick</b>"));

    let summarized = wait_total(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "quick",
            "RETURN",
            "1",
            "body",
            "SUMMARIZE",
        ],
        1,
    );
    let Frame::Array(items) = summarized else {
        panic!("expected search array");
    };
    let Frame::Array(fields) = &items[2] else {
        panic!("expected field array");
    };
    let snippet = bulk_text(&fields[1]);
    assert!(snippet.contains("quick"));
    assert!(snippet.len() < body.len());
}

#[test]
fn ft_search_text_analysis_rejects_custom_scorer() {
    let (_dir, db) = make_db();
    assert!(matches!(
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
                "body",
                "TEXT"
            ],
        ),
        Frame::Ok
    ));
    let err = apply_err(&db, &["FT.SEARCH", "idx", "*", "SCORER", "custom"]);
    let Frame::Error(message) = err else {
        panic!("expected scorer error");
    };
    assert!(message.contains("unsupported fulltext scorer"));
}
