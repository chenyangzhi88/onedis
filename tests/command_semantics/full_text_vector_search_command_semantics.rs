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

fn reopen_db(dir: &TempDir) -> Db {
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
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
    db.handle_command(command(args).expect("failed to parse command"))
        .expect("command failed")
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

fn bulk_text(frame: &Frame) -> String {
    let Frame::BulkString(value) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(value.clone()).unwrap()
}

fn search_ids(frame: Frame) -> Vec<String> {
    let items = array(frame);
    let total = integer(&items[0]) as usize;
    if items.len() == total + 1 {
        return items[1..].iter().map(bulk_text).collect();
    }
    items[1..]
        .chunks(2)
        .map(|chunk| bulk_text(&chunk[0]))
        .collect()
}

fn field_value(fields: &Frame, name: &str) -> Option<String> {
    let Frame::Array(items) = fields else {
        panic!("expected fields array");
    };
    items.chunks(2).find_map(|chunk| {
        if bulk_text(&chunk[0]) == name {
            Some(bulk_text(&chunk[1]))
        } else {
            None
        }
    })
}

fn seed_vector_index() -> (TempDir, Db) {
    let (dir, db) = make_db();
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
            "TEXT",
            "category",
            "TAG",
            "embedding",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:1",
            "title",
            "red shirt",
            "category",
            "shirt",
            "embedding",
            "[1,0]",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:2",
            "title",
            "blue pants",
            "category",
            "pants",
            "embedding",
            "[0,1]",
        ],
    );
    apply(
        &db,
        &[
            "HSET",
            "doc:3",
            "title",
            "red jacket",
            "category",
            "jacket",
            "embedding",
            "[0.9,0.1]",
        ],
    );
    (dir, db)
}

#[test]
fn ft_search_vector_knn_uses_fulltext_vector_schema() {
    let (_dir, db) = seed_vector_index();
    let result = array(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*=>[KNN 2 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "RETURN",
            "2",
            "title",
            "__vector_score",
            "DIALECT",
            "2",
        ],
    ));

    assert_eq!(integer(&result[0]), 2);
    assert_eq!(bulk_text(&result[1]), "doc:1");
    assert_eq!(bulk_text(&result[3]), "doc:3");
    assert_eq!(
        field_value(&result[2], "__vector_score").as_deref(),
        Some("0.0")
    );
}

#[test]
fn ft_search_vector_hybrid_filter_and_range_are_ranked_by_vector() {
    let (_dir, db) = seed_vector_index();

    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "@category:{jacket|pants}=>[KNN 2 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:3", "doc:2"]);

    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "@embedding:[VECTOR_RANGE 0.03 $vec]",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:1", "doc:3"]);
}

#[test]
fn ft_hybrid_vector_reuses_search_vector_execution() {
    let (_dir, db) = seed_vector_index();
    let ids = search_ids(apply(
        &db,
        &[
            "FT.HYBRID",
            "idx",
            "*=>[KNN 1 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "[0,1]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:2"]);
}

#[test]
fn ft_vector_backend_survives_reopen_and_updates() {
    let (dir, db) = seed_vector_index();
    drop(db);
    let db = reopen_db(&dir);

    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*=>[KNN 1 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "[0,1]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:2"]);

    apply(&db, &["HSET", "doc:2", "embedding", "[1,0]"]);
    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "*=>[KNN 1 @embedding $vec]",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:1"]);
}
