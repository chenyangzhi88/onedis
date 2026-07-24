use std::path::{Path, PathBuf};
use std::sync::Arc;

use onedis_server::{
    command::Command,
    frame::Frame,
    store::{
        db::{Db, VectorCreateOptions, VectorFieldKind, VectorFieldSchema, VectorSearchOptions},
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    },
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn path(&self) -> &Path {
        &self.path
    }
}

fn test_root(prefix: &str) -> PathBuf {
    let unique = format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique)
}

fn make_db() -> (TestDir, Db) {
    let dir = TestDir {
        path: test_root("vector-command"),
    };
    let db = open_db_at(&dir);
    (dir, db)
}

fn open_db_at(dir: &TestDir) -> Db {
    let root = dir.path().join("db");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&root).expect("failed to create db dir");
    std::fs::create_dir_all(&wal_dir).expect("failed to create wal dir");
    let store = KvStore::new(root, wal_dir, 1);
    let vc = Arc::new(VersionCounter::new());
    let ttl = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, vc, ttl)
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

fn apply_autocommit(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command_autocommit(db, command(args))
        .expect("command failed")
}

fn try_apply(db: &Db, args: &[&str]) -> anyhow::Result<Frame> {
    onedis_server::command_dispatch::handle_command(db, command(args))
}

fn command_frame(frame: Frame) -> Command {
    Command::parse_from_frame(frame).expect("failed to parse command")
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    onedis_server::command_dispatch::handle_command_async(db, command(args))
        .await
        .expect("command failed")
}

fn assert_integer(frame: Frame, expected: i64) {
    assert!(matches!(frame, Frame::Integer(value) if value == expected));
}

fn bulk_string(frame: &Frame) -> String {
    let Frame::BulkString(bytes) = frame else {
        panic!("expected bulk string");
    };
    String::from_utf8(bytes.clone()).unwrap()
}

fn info_value(frame: &Frame, key: &str) -> Option<String> {
    let Frame::Array(values) = frame else {
        return None;
    };
    values.chunks_exact(2).find_map(|pair| {
        let Frame::BulkString(name) = &pair[0] else {
            return None;
        };
        if name.as_slice() != key.as_bytes() {
            return None;
        }
        let Frame::BulkString(value) = &pair[1] else {
            return None;
        };
        String::from_utf8(value.clone()).ok()
    })
}

fn vector_options(dim: usize) -> VectorCreateOptions {
    VectorCreateOptions {
        dim,
        distance: "COSINE".to_string(),
        schema: Vec::new(),
        segment_max_docs: None,
        m: None,
        ef_construction: None,
        ef_runtime: None,
        initial_cap: None,
    }
}

#[test]
fn old_vector_commands_are_removed() {
    let (_dir, db) = make_db();
    assert!(try_apply(&db, &["VEC.INFO", "points"]).is_err());
    assert!(try_apply(&db, &["VECTOR.INFO", "points"]).is_err());
}

#[test]
fn redis_vector_set_basic_commands_are_supported() {
    let (_dir, db) = make_db();
    assert_integer(
        apply(
            &db,
            &[
                "VADD",
                "points",
                "VALUES",
                "2",
                "1",
                "0",
                "pt:A",
                "SETATTR",
                r#"{"size":"large","price":18.99}"#,
                "M",
                "8",
                "EF",
                "16",
            ],
        ),
        1,
    );
    assert_integer(
        apply(
            &db,
            &[
                "VADD",
                "points",
                "VALUES",
                "2",
                "0",
                "1",
                "pt:B",
                "SETATTR",
                r#"{"size":"small","price":35.5}"#,
            ],
        ),
        1,
    );
    assert_integer(
        apply(&db, &["VADD", "points", "VALUES", "2", "2", "0", "pt:A"]),
        0,
    );
    assert_integer(apply(&db, &["VCARD", "points"]), 2);
    assert_integer(apply(&db, &["VDIM", "points"]), 2);

    let result = apply(
        &db,
        &[
            "VSIM",
            "points",
            "VALUES",
            "2",
            "2",
            "0",
            "COUNT",
            "2",
            "WITHSCORES",
        ],
    );
    let Frame::Array(values) = result else {
        panic!("expected VSIM array");
    };
    assert_eq!(values.len(), 4);
    assert_eq!(bulk_string(&values[0]), "pt:A");
    assert!(matches!(&values[1], Frame::BulkString(score) if !score.is_empty()));

    let emb = apply(&db, &["VEMB", "points", "pt:A"]);
    assert!(matches!(emb, Frame::Array(values) if values.len() == 2));

    let raw = apply(&db, &["VEMB", "points", "pt:A", "RAW"]);
    assert!(matches!(raw, Frame::BulkString(bytes) if bytes.len() == 8));

    let info = apply(&db, &["VINFO", "points"]);
    assert_eq!(info_value(&info, "doc_count"), Some("2".to_string()));

    let random = apply(&db, &["VRANDMEMBER", "points", "2"]);
    assert!(matches!(random, Frame::Array(values) if values.len() == 2));

    let links = apply(&db, &["VLINKS", "points", "pt:A", "WITHSCORES"]);
    let Frame::Array(layers) = links else {
        panic!("expected VLINKS layers");
    };
    assert_eq!(layers.len(), 1);
    assert!(matches!(&layers[0], Frame::Array(values) if values.len() == 2));
}

#[test]
fn redis_vector_set_autocommit_persists_multiple_elements() {
    let (_dir, db) = make_db();

    assert_integer(
        apply_autocommit(
            &db,
            &["VADD", "products", "VALUES", "3", "1", "0", "0", "p1"],
        ),
        1,
    );
    assert_integer(
        apply_autocommit(
            &db,
            &["VADD", "products", "VALUES", "3", "0", "1", "0", "p2"],
        ),
        1,
    );

    assert_integer(apply(&db, &["VCARD", "products"]), 2);
    assert!(matches!(
        apply(&db, &["VEMB", "products", "p2"]),
        Frame::Array(values) if values.len() == 3
    ));
}

#[test]
fn redis_vector_set_attrs_filter_update_and_remove_work() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "VADD",
            "items",
            "VALUES",
            "2",
            "1",
            "0",
            "a",
            "SETATTR",
            r#"{"size":"large","price":18.99}"#,
        ],
    );
    apply(
        &db,
        &[
            "VADD",
            "items",
            "VALUES",
            "2",
            "0",
            "1",
            "b",
            "SETATTR",
            r#"{"size":"large","price":35.99}"#,
        ],
    );
    apply(
        &db,
        &[
            "VADD",
            "items",
            "VALUES",
            "2",
            "-1",
            "0",
            "c",
            "SETATTR",
            r#"{"size":"small","price":25.99}"#,
        ],
    );

    let filtered = apply(
        &db,
        &[
            "VSIM",
            "items",
            "VALUES",
            "2",
            "1",
            "0",
            "COUNT",
            "3",
            "FILTER",
            r#".size == "large" && .price > 20"#,
        ],
    );
    assert!(
        matches!(filtered, Frame::Array(values) if values.len() == 1 && bulk_string(&values[0]) == "b")
    );

    let attrs = apply(&db, &["VGETATTR", "items", "b"]);
    assert!(
        matches!(attrs, Frame::BulkString(value) if value == br#"{"size":"large","price":35.99}"#)
    );

    assert_integer(
        apply(
            &db,
            &["VSETATTR", "items", "b", r#"{"size":"small","price":12}"#],
        ),
        1,
    );
    let with_attrs = apply(
        &db,
        &[
            "VSIM",
            "items",
            "ELE",
            "b",
            "COUNT",
            "1",
            "WITHSCORES",
            "WITHATTRIBS",
        ],
    );
    let Frame::Array(values) = with_attrs else {
        panic!("expected VSIM WITHATTRIBS array");
    };
    assert_eq!(values.len(), 3);
    assert_eq!(bulk_string(&values[0]), "b");
    assert!(
        matches!(&values[2], Frame::BulkString(attrs) if attrs == br#"{"size":"small","price":12}"#)
    );

    assert_integer(apply(&db, &["VREM", "items", "b"]), 1);
    assert!(matches!(
        apply(&db, &["VGETATTR", "items", "b"]),
        Frame::Null
    ));
    assert_integer(apply(&db, &["VCARD", "items"]), 2);
}

#[test]
fn redis_vector_set_update_hides_stale_attrs() {
    let (_dir, db) = make_db();
    apply(
        &db,
        &[
            "VADD",
            "stale",
            "VALUES",
            "2",
            "1",
            "0",
            "same",
            "SETATTR",
            r#"{"kind":"old"}"#,
        ],
    );
    apply(
        &db,
        &[
            "VADD",
            "stale",
            "VALUES",
            "2",
            "0",
            "1",
            "same",
            "SETATTR",
            r#"{"kind":"new"}"#,
        ],
    );

    let old = apply(
        &db,
        &[
            "VSIM",
            "stale",
            "VALUES",
            "2",
            "1",
            "0",
            "COUNT",
            "1",
            "FILTER",
            r#".kind == "old""#,
        ],
    );
    assert!(matches!(old, Frame::Array(values) if values.is_empty()));

    let new = apply(
        &db,
        &[
            "VSIM",
            "stale",
            "VALUES",
            "2",
            "0",
            "1",
            "COUNT",
            "1",
            "FILTER",
            r#".kind == "new""#,
        ],
    );
    assert!(
        matches!(new, Frame::Array(values) if values.len() == 1 && bulk_string(&values[0]) == "same")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_vector_set_async_path_supports_filter() {
    let (_dir, db) = make_db();
    apply_async(
        &db,
        &[
            "VADD",
            "async",
            "VALUES",
            "2",
            "1",
            "0",
            "a",
            "SETATTR",
            r#"{"tenant":"t1"}"#,
        ],
    )
    .await;
    apply_async(
        &db,
        &[
            "VADD",
            "async",
            "VALUES",
            "2",
            "0",
            "1",
            "b",
            "SETATTR",
            r#"{"tenant":"t2"}"#,
        ],
    )
    .await;
    let result = apply_async(
        &db,
        &[
            "VSIM",
            "async",
            "VALUES",
            "2",
            "1",
            "0",
            "COUNT",
            "2",
            "FILTER",
            r#".tenant == "t1""#,
        ],
    )
    .await;
    assert!(
        matches!(result, Frame::Array(values) if values.len() == 1 && bulk_string(&values[0]) == "a")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_vector_set_concurrent_adds_do_not_lose_meta_updates() {
    let (_dir, db) = make_db();
    let db = Arc::new(db);
    let mut tasks = Vec::new();
    for i in 0..32 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let id = format!("doc:{i}");
            let x = i.to_string();
            let args = [
                "VADD",
                "concurrent",
                "VALUES",
                "2",
                x.as_str(),
                "0",
                id.as_str(),
            ];
            onedis_server::command_dispatch::handle_command_async(&db, command(&args))
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        assert!(matches!(task.await.unwrap(), Frame::Integer(1)));
    }

    assert_integer(apply(&db, &["VCARD", "concurrent"]), 32);
    let result = apply(
        &db,
        &["VSIM", "concurrent", "VALUES", "2", "0", "0", "COUNT", "32"],
    );
    assert!(matches!(result, Frame::Array(values) if values.len() == 32));
}

#[test]
fn redis_vector_set_rebuilds_hnsw_runtime_after_reopen() {
    let (dir, db) = make_db();
    apply(&db, &["VADD", "reopen", "VALUES", "2", "1", "0", "a"]);
    apply(&db, &["VADD", "reopen", "VALUES", "2", "0", "1", "b"]);

    drop(db);
    let reopened = open_db_at(&dir);
    assert_integer(apply(&reopened, &["VCARD", "reopen"]), 2);
    let result = apply(
        &reopened,
        &["VSIM", "reopen", "VALUES", "2", "0", "1", "COUNT", "1"],
    );
    assert!(
        matches!(result, Frame::Array(values) if values.len() == 1 && bulk_string(&values[0]) == "b")
    );
}

#[test]
fn redis_vector_set_parser_and_missing_key_edges_are_deterministic() {
    let (_dir, db) = make_db();

    for args in [
        &["VADD", "v"][..],
        &["VADD", "v", "VALUES", "0", "e"][..],
        &["VADD", "v", "VALUES", "1", "nan", "e"][..],
        &["VADD", "v", "VALUES", "1", "1", "e", "REDUCE"][..],
        &["VADD", "v", "REDUCE", "0", "VALUES", "1", "1", "e"][..],
        &["VADD", "v", "VALUES", "1", "1", "e", "SETATTR"][..],
        &["VADD", "v", "VALUES", "1", "1", "e", "EF", "bad"][..],
        &["VADD", "v", "VALUES", "1", "1", "e", "M", "bad"][..],
        &["VADD", "v", "VALUES", "1", "1", "e", "BADOPT"][..],
        &["VSIM", "v"][..],
        &["VSIM", "v", "BAD", "e"][..],
        &["VSIM", "v", "VALUES", "1", "1", "COUNT", "bad"][..],
        &["VSIM", "v", "VALUES", "1", "1", "EF", "bad"][..],
        &["VSIM", "v", "VALUES", "1", "1", "FILTER"][..],
        &["VSIM", "v", "VALUES", "1", "1", "EPSILON", "inf"][..],
        &["VSIM", "v", "VALUES", "1", "1", "FILTER-EF", "bad"][..],
        &["VSIM", "v", "VALUES", "1", "1", "BADOPT"][..],
        &["VREM", "v"][..],
        &["VCARD"][..],
        &["VDIM"][..],
        &["VEMB", "v"][..],
        &["VGETATTR", "v"][..],
        &["VSETATTR", "v", "e"][..],
        &["VINFO"][..],
        &["VRANDMEMBER"][..],
        &["VRANDMEMBER", "v", "bad"][..],
        &["VLINKS", "v"][..],
    ] {
        assert!(
            Command::parse_from_frame(Frame::Array(
                args.iter()
                    .map(|arg| Frame::bulk_string((*arg).to_string()))
                    .collect(),
            ))
            .is_err(),
            "{args:?}"
        );
    }

    let invalid_blob = Frame::Array(vec![
        Frame::bulk_string("VADD"),
        Frame::bulk_string("v"),
        Frame::bulk_string("FP32"),
        Frame::BulkString(vec![1, 2, 3]),
        Frame::bulk_string("e"),
    ]);
    assert!(Command::parse_from_frame(invalid_blob).is_err());

    assert_integer(apply(&db, &["VCARD", "missing"]), 0);
    assert!(matches!(apply(&db, &["VDIM", "missing"]), Frame::Null));
    assert!(matches!(apply(&db, &["VEMB", "missing", "e"]), Frame::Null));
    assert!(matches!(
        apply(&db, &["VGETATTR", "missing", "e"]),
        Frame::Null
    ));
    assert!(try_apply(&db, &["VSETATTR", "missing", "e", r#"{"x":1}"#]).is_err());
    apply(&db, &["VADD", "existing", "VALUES", "1", "1", "present"]);
    assert_integer(
        apply(&db, &["VSETATTR", "existing", "absent", r#"{"x":1}"#]),
        0,
    );
    assert!(try_apply(&db, &["VREM", "missing", "e"]).is_err());
    assert_integer(apply(&db, &["VREM", "existing", "absent"]), 0);
    assert!(matches!(
        apply(&db, &["VRANDMEMBER", "missing"]),
        Frame::Null
    ));
    assert!(matches!(
        apply(&db, &["VRANDMEMBER", "missing", "2"]),
        Frame::Array(values) if values.is_empty()
    ));
    assert!(try_apply(&db, &["VSIM", "missing", "ELE", "e"]).is_err());
    assert!(try_apply(&db, &["VLINKS", "missing", "e"]).is_err());
}

#[test]
fn redis_vector_set_advanced_options_cover_reduce_fp32_epsilon_and_attr_clear() {
    let (_dir, db) = make_db();

    assert_integer(
        apply(
            &db,
            &[
                "VADD",
                "advanced",
                "REDUCE",
                "2",
                "VALUES",
                "3",
                "1",
                "0",
                "9",
                "a",
                "SETATTR",
                r#"{"group":"keep"}"#,
                "NOQUANT",
                "CAS",
                "Q8",
                "BIN",
            ],
        ),
        1,
    );
    assert_integer(apply(&db, &["VDIM", "advanced"]), 2);

    let mut blob = Vec::new();
    blob.extend_from_slice(&0.0f32.to_le_bytes());
    blob.extend_from_slice(&1.0f32.to_le_bytes());
    let fp32 = Frame::Array(vec![
        Frame::bulk_string("VADD"),
        Frame::bulk_string("advanced"),
        Frame::bulk_string("FP32"),
        Frame::BulkString(blob),
        Frame::bulk_string("b"),
        Frame::bulk_string("SETATTR"),
        Frame::bulk_string(r#"{"group":"drop"}"#),
    ]);
    assert!(matches!(
        onedis_server::command_dispatch::handle_command(&db, command_frame(fp32)).unwrap(),
        Frame::Integer(1)
    ));

    let exact_only = apply(
        &db,
        &[
            "VSIM",
            "advanced",
            "ELE",
            "a",
            "COUNT",
            "2",
            "EPSILON",
            "0",
            "TRUTH",
            "NOTHREAD",
            "WITHATTRIBS",
        ],
    );
    assert!(
        matches!(exact_only, Frame::Array(values) if values.len() == 2 && bulk_string(&values[0]) == "a")
    );

    let duplicates = apply(&db, &["VRANDMEMBER", "advanced", "-3"]);
    assert!(matches!(duplicates, Frame::Array(values) if values.len() == 3));

    assert_integer(apply(&db, &["VSETATTR", "advanced", "a", ""]), 1);
    assert!(matches!(
        apply(&db, &["VGETATTR", "advanced", "a"]),
        Frame::Null
    ));
}

#[test]
fn vector_store_create_schema_search_drop_rebuild_and_compact_paths() {
    let (_dir, db) = make_db();

    assert!(db.vector_create("bad-dim", vector_options(0)).is_err());
    let mut bad_distance = vector_options(2);
    bad_distance.distance = "BAD".to_string();
    assert!(db.vector_create("bad-distance", bad_distance).is_err());
    let mut bad_m = vector_options(2);
    bad_m.m = Some(0);
    assert!(db.vector_create("bad-m", bad_m).is_err());
    let mut duplicate_schema = vector_options(2);
    duplicate_schema.schema = vec![
        VectorFieldSchema {
            name: "tag".to_string(),
            kind: VectorFieldKind::Tag,
            indexed: true,
        },
        VectorFieldSchema {
            name: "tag".to_string(),
            kind: VectorFieldKind::Numeric,
            indexed: true,
        },
    ];
    assert!(db.vector_create("bad-schema", duplicate_schema).is_err());

    let mut options = vector_options(2);
    options.distance = "L2".to_string();
    options.segment_max_docs = Some(1);
    options.m = Some(4);
    options.ef_construction = Some(2);
    options.ef_runtime = Some(3);
    options.initial_cap = Some(1);
    options.schema = vec![
        VectorFieldSchema {
            name: "tenant".to_string(),
            kind: VectorFieldKind::Tag,
            indexed: true,
        },
        VectorFieldSchema {
            name: "price".to_string(),
            kind: VectorFieldKind::Numeric,
            indexed: true,
        },
        VectorFieldSchema {
            name: "note".to_string(),
            kind: VectorFieldKind::Text,
            indexed: false,
        },
    ];
    db.vector_create("store-api", options.clone()).unwrap();
    assert!(db.vector_create("store-api", options).is_err());
    assert!(db.vector_add("store-api", "bad", vec![1.0], None).is_err());

    db.vector_add(
        "store-api",
        "a",
        vec![1.0, 0.0],
        Some(r#"{"tenant":"t1","price":10,"note":"first"}"#.to_string()),
    )
    .unwrap();
    db.vector_add(
        "store-api",
        "b",
        vec![0.0, 1.0],
        Some(r#"{"tenant":"t2","price":20,"note":"second"}"#.to_string()),
    )
    .unwrap();
    db.vector_add(
        "store-api",
        "c",
        vec![0.5, 0.5],
        Some(r#"{"tenant":"t1","price":30,"note":"third"}"#.to_string()),
    )
    .unwrap();
    assert_eq!(db.vector_card("store-api").unwrap(), 3);
    assert_eq!(db.vector_dim("store-api").unwrap(), Some(2));

    let filtered = db
        .vector_search(
            "store-api",
            &[1.0, 0.0],
            VectorSearchOptions {
                k: 3,
                filter: Some(r#".tenant == "t1" && .price >= 10"#.to_string()),
                with_scores: true,
                with_attrs: vec!["tenant".to_string(), "price".to_string()],
                ef: Some(8),
                offset: 0,
                limit: Some(2),
            },
        )
        .unwrap();
    assert_eq!(filtered.len(), 2);
    assert!(
        filtered
            .iter()
            .all(|result| result.attrs.iter().any(|(key, _)| key == "tenant"))
    );

    let paged = db
        .vector_search(
            "store-api",
            &[1.0, 0.0],
            VectorSearchOptions {
                k: 3,
                filter: None,
                with_scores: false,
                with_attrs: Vec::new(),
                ef: None,
                offset: 1,
                limit: Some(1),
            },
        )
        .unwrap();
    assert_eq!(paged.len(), 1);

    assert_eq!(
        db.vector_del("store-api", &["b".to_string(), "b".to_string()])
            .unwrap(),
        1
    );
    assert_eq!(db.vector_del("store-api", &["b".to_string()]).unwrap(), 0);
    db.vector_rebuild("store-api").unwrap();
    db.vector_compact("store-api").unwrap();
    assert_eq!(db.vector_card("store-api").unwrap(), 2);

    let info = db.vector_info("store-api").unwrap();
    assert!(
        info.iter()
            .any(|(key, value)| key == "distance" && value == "L2")
    );
    assert_eq!(db.vector_drop("store-api").unwrap(), 1);
    assert!(db.vector_drop("store-api").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_store_async_create_add_search_rebuild_compact_and_drop_paths() {
    let (_dir, db) = make_db();
    let mut options = vector_options(2);
    options.distance = "IP".to_string();
    options.segment_max_docs = Some(1);
    options.schema = vec![VectorFieldSchema {
        name: "tenant".to_string(),
        kind: VectorFieldKind::Tag,
        indexed: true,
    }];

    db.vector_create_async("async-store", options)
        .await
        .unwrap();
    db.vector_add_async(
        "async-store",
        "a",
        vec![1.0, 0.0],
        Some(r#"{"tenant":"t1"}"#.to_string()),
    )
    .await
    .unwrap();
    db.vector_add_async(
        "async-store",
        "b",
        vec![0.0, 1.0],
        Some(r#"{"tenant":"t2"}"#.to_string()),
    )
    .await
    .unwrap();
    let results = db
        .vector_search_async(
            "async-store",
            &[1.0, 0.0],
            VectorSearchOptions {
                k: 2,
                filter: Some(r#".tenant == "t1""#.to_string()),
                with_scores: true,
                with_attrs: vec!["tenant".to_string()],
                ef: Some(4),
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "a");

    assert!(
        db.vector_set_attrs_async("async-store", "a", None)
            .await
            .unwrap()
    );
    assert_eq!(
        db.vector_element_async("async-store", "a")
            .await
            .unwrap()
            .unwrap()
            .attrs_json,
        "{}"
    );
    db.vector_rebuild_async("async-store").await.unwrap();
    db.vector_compact_async("async-store").await.unwrap();
    assert_eq!(db.vector_ids_async("async-store").await.unwrap().len(), 2);
    assert_eq!(db.vector_drop_async("async-store").await.unwrap(), 1);
    assert_eq!(db.vector_card_async("async-store").await.unwrap(), 0);
}
