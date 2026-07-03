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
    match onedis_server::command_dispatch::handle_command(
        db,
        command(args).expect("failed to parse command"),
    ) {
        Ok(_) => panic!("command should fail"),
        Err(err) => err,
    }
}

fn search_ids(frame: Frame) -> Vec<String> {
    let Frame::Array(items) = frame else {
        panic!("expected search array");
    };
    items[1..]
        .chunks(2)
        .map(|chunk| {
            let Frame::BulkString(key) = &chunk[0] else {
                panic!("expected key");
            };
            String::from_utf8(key.clone()).unwrap()
        })
        .collect()
}

fn assert_search_ids(db: &Db, args: &[&str], expected: &[&str]) {
    let mut actual = search_ids(apply(db, args));
    let mut expected = expected
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "query args: {args:?}");
}

fn seed_query_db() -> (TempDir, Db) {
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
            "location",
            "GEO",
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
fn ft_search_query_parser_boolean_field_phrase_and_pattern_queries() {
    let (_dir, db) = seed_query_db();

    assert_search_ids(&db, &["FT.SEARCH", "idx", "quick redis"], &["doc:1"]);
    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "quick|slow"],
        &["doc:1", "doc:2", "doc:3"],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "quick -tool"], &["doc:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "-slow"], &["doc:1", "doc:3"]);
    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "@title:(quick|slow)"],
        &["doc:1", "doc:2", "doc:3"],
    );
    assert_search_ids(&db, &["FT.SEARCH", "idx", "\"quick fox\""], &["doc:1"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "qui*"], &["doc:1", "doc:3"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "q?ick"], &["doc:1", "doc:3"]);
    assert_search_ids(&db, &["FT.SEARCH", "idx", "%quik%"], &["doc:1", "doc:3"]);
    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "~missing quick"],
        &["doc:1", "doc:3"],
    );
    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "quick=>{$weight:2.0}"],
        &["doc:1", "doc:3"],
    );
}

#[test]
fn ft_search_query_parser_tag_numeric_params_and_dialect() {
    let (_dir, db) = seed_query_db();

    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "@category:{book|tool}"],
        &["doc:1", "doc:2", "doc:3"],
    );
    assert_search_ids(
        &db,
        &["FT.SEARCH", "idx", "@price:[(10 25]"],
        &["doc:1", "doc:2"],
    );
    assert_search_ids(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "@category:{$cat} @price:[$min $max]",
            "PARAMS",
            "6",
            "cat",
            "book",
            "min",
            "10",
            "max",
            "20",
            "DIALECT",
            "3",
        ],
        &["doc:1"],
    );
}

#[test]
fn ft_search_query_parser_rejects_bad_syntax_and_unimplemented_plans() {
    let (_dir, db) = seed_query_db();

    let err = apply_err(&db, &["FT.SEARCH", "idx", "quick |"]);
    assert!(err.to_string().contains("syntax error"));

    let err = apply_err(&db, &["FT.SEARCH", "idx", "@price:[10 nope]"]);
    assert!(err.to_string().contains("invalid numeric range"));

    assert_search_ids(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*=>[KNN 1 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "blob",
            "DIALECT",
            "2",
        ],
        &[],
    );

    assert_search_ids(&db, &["FT.SEARCH", "idx", "@location:[0 0 1 km]"], &[]);
}
