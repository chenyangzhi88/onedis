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
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    (dir, Db::new(0, store, version_counter, ttl_manager))
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

fn apply_err(db: &Db, args: &[&str]) -> anyhow::Error {
    let command = match command(args) {
        Ok(command) => command,
        Err(err) => return err,
    };
    match onedis_server::command_dispatch::handle_command(db, command) {
        Ok(_) => panic!("command should fail"),
        Err(err) => err,
    }
}

fn bulk_text(frame: &Frame) -> String {
    let Frame::BulkString(value) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(value.clone()).unwrap()
}

fn array(frame: Frame) -> Vec<Frame> {
    let Frame::Array(items) = frame else {
        panic!("expected array");
    };
    items
}

fn integer(frame: &Frame) -> i64 {
    let Frame::Integer(value) = frame else {
        panic!("expected integer");
    };
    *value
}

fn seed_search_db() -> (TempDir, Db) {
    let (dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "body",
            "TEXT",
            "category",
            "TAG",
            "price",
            "NUMERIC",
            "SORTABLE",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:1",
            "title",
            "quick fox",
            "body",
            "redis search",
            "category",
            "book",
            "price",
            "12",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:2",
            "title",
            "slow turtle",
            "body",
            "redis guide",
            "category",
            "book",
            "price",
            "25",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:3",
            "title",
            "quick tool",
            "body",
            "storage engine",
            "category",
            "tool",
            "price",
            "7",
        ],
    );
    (dir, db)
}

#[test]
fn ft_search_results_return_shapes_and_projection() {
    let (_dir, db) = seed_search_db();

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "quick",
            "RETURN",
            "2",
            "title",
            "AS",
            "headline",
            "price",
        ],
    ));
    assert_eq!(integer(&result[0]), 2);
    let fields = array(result[2].clone());
    assert_eq!(bulk_text(&fields[0]), "headline");
    assert!(["quick fox", "quick tool"].contains(&bulk_text(&fields[1]).as_str()));
    assert_eq!(bulk_text(&fields[2]), "price");

    let result = array(apply(&db, &["FT.SEARCH", "idx", "quick", "NOCONTENT"]));
    assert_eq!(integer(&result[0]), 2);
    assert_eq!(result.len(), 3);

    let result = array(apply(
        &db,
        &["FT.SEARCH", "idx", "quick", "NOCONTENT", "WITHSCORES"],
    ));
    assert_eq!(integer(&result[0]), 2);
    assert_eq!(result.len(), 5);
    assert!(bulk_text(&result[2]).parse::<f32>().is_ok());

    let result = array(apply(
        &db,
        &["FT.SEARCH", "idx", "quick", "NOCONTENT", "WITHPAYLOADS"],
    ));
    assert_eq!(integer(&result[0]), 2);
    assert!(matches!(result[2], Frame::Null));
}

#[test]
fn ft_search_results_filters_keys_fields_sort_and_limit() {
    let (_dir, db) = seed_search_db();

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*",
            "FILTER",
            "price",
            "10",
            "20",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&result[0]), 1);
    assert_eq!(bulk_text(&result[1]), "doc:1");

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "redis",
            "INKEYS",
            "1",
            "doc:2",
            "NOCONTENT",
            "LIMIT",
            "0",
            "1",
        ],
    ));
    assert_eq!(integer(&result[0]), 1);
    assert_eq!(bulk_text(&result[1]), "doc:2");

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "redis",
            "INFIELDS",
            "1",
            "title",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&result[0]), 0);

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*",
            "SORTBY",
            "price",
            "ASC",
            "WITHSORTKEYS",
            "NOCONTENT",
            "LIMIT",
            "0",
            "2",
        ],
    ));
    assert_eq!(integer(&result[0]), 3);
    assert_eq!(bulk_text(&result[1]), "doc:3");
    assert_eq!(bulk_text(&result[2]), "7");
    assert_eq!(bulk_text(&result[3]), "doc:1");
    assert_eq!(bulk_text(&result[4]), "12");

    let result = array(apply(&db, &["FT.SEARCH", "idx", "*", "LIMIT", "0", "0"]));
    assert_eq!(integer(&result[0]), 3);
    assert_eq!(result.len(), 1);
}

#[test]
fn ft_search_results_parses_execution_options_and_rejects_unsupported_ones() {
    let (_dir, db) = seed_search_db();

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "quick",
            "SUMMARIZE",
            "FIELDS",
            "1",
            "body",
            "HIGHLIGHT",
            "FIELDS",
            "1",
            "title",
            "SLOP",
            "2",
            "INORDER",
            "TIMEOUT",
            "100",
            "LANGUAGE",
            "english",
            "SCORER",
            "BM25STD",
            "EXPANDER",
            "DEFAULT",
            "PAYLOAD",
            "opaque",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&result[0]), 2);

    let err = apply_err(&db, &["FT.SEARCH", "idx", "*", "EXPANDER", "custom"]);
    assert!(err.to_string().contains("unsupported fulltext expander"));

    let err = apply_err(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*",
            "GEOFILTER",
            "loc",
            "0",
            "0",
            "1",
            "km",
        ],
    );
    assert!(err.to_string().contains("invalid geo field"));
}

#[test]
fn ft_search_executes_field_weights_optional_terms_and_selected_scorers() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "weighted",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "weight:",
            "SCHEMA",
            "title",
            "TEXT",
            "WEIGHT",
            "5",
            "body",
            "TEXT",
        ],
    );
    apply(
        &db,
        &["HSET", "weight:1", "title", "needle", "body", "plain"],
    );
    apply(
        &db,
        &["HSET", "weight:2", "title", "plain", "body", "needle"],
    );

    let weighted = array(apply(
        &db,
        &["FT.SEARCH", "weighted", "needle", "WITHSCORES", "NOCONTENT"],
    ));
    assert_eq!(bulk_text(&weighted[1]), "weight:1");

    let standard_score = bulk_text(&weighted[2]).parse::<f32>().unwrap();
    let legacy = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "weighted",
            "needle",
            "SCORER",
            "BM25",
            "WITHSCORES",
            "NOCONTENT",
        ],
    ));
    let legacy_score = bulk_text(&legacy[2]).parse::<f32>().unwrap();
    assert_ne!(standard_score, legacy_score);

    apply(
        &db,
        &["HSET", "weight:3", "title", "unrelated", "body", "plain"],
    );
    let optional = array(apply(
        &db,
        &["FT.SEARCH", "weighted", "~needle", "NOCONTENT"],
    ));
    assert_eq!(integer(&optional[0]), 3);
    assert_eq!(bulk_text(&optional[1]), "weight:1");
}

#[test]
fn ft_search_executes_score_and_payload_fields() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "scored",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "score:",
            "SCORE_FIELD",
            "rank",
            "PAYLOAD_FIELD",
            "payload",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    );
    apply(
        &db,
        &[
            "HSET", "score:1", "title", "same", "rank", "0.2", "payload", "first",
        ],
    );
    apply(
        &db,
        &[
            "HSET", "score:2", "title", "same", "rank", "0.9", "payload", "second",
        ],
    );

    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "scored",
            "same",
            "SCORER",
            "DOCSCORE",
            "WITHSCORES",
            "WITHPAYLOADS",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&result[0]), 2);
    assert_eq!(bulk_text(&result[1]), "score:2");
    assert_eq!(bulk_text(&result[2]), "0.9");
    assert_eq!(bulk_text(&result[3]), "second");
}

#[test]
fn ft_search_slop_and_inorder_change_phrase_execution() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "FT.CREATE",
            "phrases",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "phrase:",
            "SCHEMA",
            "body",
            "TEXT",
        ],
    );
    apply(&db, &["HSET", "phrase:1", "body", "quick brown fox"]);
    apply(&db, &["HSET", "phrase:2", "body", "fox brown quick"]);

    let relaxed = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "phrases",
            "\"quick fox\"",
            "SLOP",
            "4",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&relaxed[0]), 2);

    let ordered = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "phrases",
            "\"quick fox\"",
            "SLOP",
            "4",
            "INORDER",
            "NOCONTENT",
        ],
    ));
    assert_eq!(integer(&ordered[0]), 1);
    assert_eq!(bulk_text(&ordered[1]), "phrase:1");
}
