use std::sync::Arc;

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
    let db = open_db_at(dir.path());
    (dir, db)
}

fn open_db_at(root_dir: &std::path::Path) -> Db {
    let root = root_dir.join("db");
    let wal_dir = root_dir.join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn command(args: &[&str]) -> Result<Command, anyhow::Error> {
    Command::parse_from_frame(Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    ))
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command(
        db,
        command(args).expect("failed to parse command"),
    )
    .expect("command failed")
}

fn apply_result(db: &Db, args: &[&str]) -> Result<Frame, anyhow::Error> {
    onedis_server::command_dispatch::handle_command(db, command(args)?)
}

fn command_err(args: &[&str]) -> anyhow::Error {
    match command(args) {
        Ok(_) => panic!("command should fail"),
        Err(err) => err,
    }
}

fn apply_err(db: &Db, args: &[&str]) -> anyhow::Error {
    match apply_result(db, args) {
        Ok(_) => panic!("command should fail"),
        Err(err) => err,
    }
}

fn resp_text(frame: Frame) -> String {
    String::from_utf8(frame.as_bytes()).expect("frame should be UTF-8")
}

fn total(frame: Frame) -> i64 {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    let Some(Frame::Integer(total)) = items.first() else {
        panic!("expected total");
    };
    *total
}

#[test]
fn ft_create_parses_and_persists_hash_schema() {
    let (dir, db) = make_db();
    assert!(matches!(
        apply(
            &db,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "2",
                "doc:",
                "product:",
                "FILTER",
                "@published==1",
                "LANGUAGE",
                "english",
                "LANGUAGE_FIELD",
                "lang",
                "SCORE",
                "0.7",
                "SCORE_FIELD",
                "score",
                "PAYLOAD_FIELD",
                "payload",
                "MAXTEXTFIELDS",
                "TEMPORARY",
                "600",
                "NOOFFSETS",
                "NOHL",
                "NOFIELDS",
                "NOFREQS",
                "STOPWORDS",
                "2",
                "a",
                "the",
                "SKIPINITIALSCAN",
                "INDEXALL",
                "ENABLE",
                "SCHEMA",
                "title",
                "TEXT",
                "WEIGHT",
                "2.5",
                "NOSTEM",
                "PHONETIC",
                "dm:en",
                "SORTABLE",
                "UNF",
                "WITHSUFFIXTRIE",
                "INDEXEMPTY",
                "INDEXMISSING",
                "tags",
                "TAG",
                "SEPARATOR",
                ",",
                "CASESENSITIVE",
                "SORTABLE",
                "price",
                "NUMERIC",
                "SORTABLE",
                "INDEXMISSING",
                "location",
                "GEO",
                "NOINDEX",
                "shape",
                "GEOSHAPE",
                "FLAT",
                "INDEXMISSING",
                "embedding",
                "VECTOR",
                "HNSW",
                "8",
                "TYPE",
                "FLOAT32",
                "DIM",
                "4",
                "DISTANCE_METRIC",
                "COSINE",
                "M",
                "16",
            ],
        ),
        Frame::Ok
    ));
    let info = resp_text(apply(&db, &["FT.INFO", "idx"]));
    for expected in [
        "HASH",
        "@published==1",
        "english",
        "payload",
        "max_text_fields",
        "TEXT",
        "dm:en",
        "GEOSHAPE",
        "FLAT",
        "VECTOR",
        "HNSW",
        "COSINE",
    ] {
        assert!(info.contains(expected), "missing {expected} in {info}");
    }
    drop(db);

    let reopened = open_db_at(dir.path());
    let info = resp_text(apply(&reopened, &["FT.INFO", "idx"]));
    assert!(info.contains("product:"));
    assert!(info.contains("no_freqs"));
    assert!(info.contains("embedding"));
}

#[test]
fn ft_create_parses_json_schema_with_vector_and_aliases() {
    let (_dir, db) = make_db();
    assert!(matches!(
        apply(
            &db,
            &[
                "FT.CREATE",
                "jidx",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "json:",
                "SCHEMA",
                "$.title",
                "AS",
                "title",
                "TEXT",
                "SORTABLE",
                "$.tags[*]",
                "AS",
                "tags",
                "TAG",
                "SEPARATOR",
                "|",
                "$.price",
                "AS",
                "price",
                "NUMERIC",
                "SORTABLE",
                "$.location",
                "AS",
                "location",
                "GEO",
                "$.embedding",
                "AS",
                "embedding",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "L2",
            ],
        ),
        Frame::Ok
    ));
    let info = resp_text(apply(&db, &["FT.INFO", "jidx"]));
    for expected in ["JSON", "$.tags[*]", "tags", "GEO", "VECTOR", "FLAT", "L2"] {
        assert!(info.contains(expected), "missing {expected} in {info}");
    }
}

#[test]
fn ft_create_rejects_invalid_schema_combinations() {
    let (_dir, db) = make_db();
    let err = apply_err(
        &db,
        &[
            "FT.CREATE",
            "bad_vector_missing_dim",
            "SCHEMA",
            "embedding",
            "VECTOR",
            "HNSW",
            "4",
            "TYPE",
            "FLOAT32",
            "DISTANCE_METRIC",
            "COSINE",
        ],
    );
    assert!(err.to_string().contains("missing VECTOR attribute"));

    let err = command_err(&[
        "FT.CREATE",
        "bad_vector_count",
        "SCHEMA",
        "embedding",
        "VECTOR",
        "HNSW",
        "3",
        "TYPE",
        "FLOAT32",
        "DIM",
    ]);
    assert!(err.to_string().contains("syntax error"));

    let err = apply_err(
        &db,
        &[
            "FT.CREATE",
            "bad_separator",
            "SCHEMA",
            "tags",
            "TAG",
            "SEPARATOR",
            "::",
        ],
    );
    assert!(err.to_string().contains("invalid TAG separator"));

    let err = command_err(&["FT.CREATE", "bad_shape", "SCHEMA", "shape", "GEOSHAPE"]);
    assert!(err.to_string().contains("syntax error"));

    let err = apply_err(
        &db,
        &[
            "FT.CREATE",
            "bad_alias",
            "SCHEMA",
            "title",
            "AS",
            "same",
            "TEXT",
            "body",
            "AS",
            "same",
            "TEXT",
        ],
    );
    assert!(err.to_string().contains("invalid fulltext schema"));
}

#[test]
fn ft_create_filter_participates_in_backfill_and_incremental_updates() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &["HSET", "doc:1", "title", "visible", "published", "1"],
    );
    apply(&db, &["HSET", "doc:2", "title", "hidden", "published", "0"]);
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
            "FILTER",
            "@published==1",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );

    assert_eq!(total(apply(&db, &["FT.SEARCH", "idx", "*"])), 1);
    assert_eq!(total(apply(&db, &["FT.SEARCH", "idx", "hidden"])), 0);

    apply(&db, &["HSET", "doc:2", "published", "1"]);
    assert_eq!(total(apply(&db, &["FT.SEARCH", "idx", "hidden"])), 1);
}

#[test]
fn ft_create_temporary_index_expires_and_is_purged_from_list() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "ephemeral",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "TEMPORARY",
            "1",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    std::thread::sleep(std::time::Duration::from_millis(600));
    assert_eq!(total(apply(&db, &["FT.SEARCH", "ephemeral", "*"])), 0);
    std::thread::sleep(std::time::Duration::from_millis(600));
    let listed = resp_text(apply(&db, &["FT._LIST"]));
    assert!(listed.contains("ephemeral"));
    std::thread::sleep(std::time::Duration::from_millis(500));

    let listed = resp_text(apply(&db, &["FT._LIST"]));
    assert!(!listed.contains("ephemeral"));
    let err = apply_err(&db, &["FT.SEARCH", "ephemeral", "*"]);
    assert!(err.to_string().contains("index does not exist"));
}
