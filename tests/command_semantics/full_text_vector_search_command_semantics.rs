use std::{sync::Arc, time::Duration};

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
    onedis_server::command_dispatch::handle_command(
        db,
        command(args).expect("failed to parse command"),
    )
    .expect("command failed")
}

fn apply_frames(db: &Db, args: Vec<Frame>) -> Frame {
    let command = Command::parse_from_frame(Frame::Array(args)).expect("failed to parse command");
    onedis_server::command_dispatch::handle_command(db, command).expect("command failed")
}

fn bulk(value: impl Into<Vec<u8>>) -> Frame {
    Frame::BulkString(value.into())
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
            "*=>[KNN 1 @embedding $vec]",
            "INKEYS",
            "1",
            "doc:2",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:2"]);

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
fn ft_search_vector_overfetches_past_expired_nearest_documents() {
    let (_dir, db) = seed_vector_index();
    apply(&db, &["PEXPIRE", "doc:1", "1"]);
    std::thread::sleep(Duration::from_millis(20));

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
    assert_eq!(ids, vec!["doc:3"]);
}

#[test]
fn ft_search_vector_clause_can_be_nested_in_conjunctions() {
    let (_dir, db) = seed_vector_index();

    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "(@category:{jacket|pants}=>[KNN 3 @embedding $vec]) @title:red",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:3"]);

    let ids = search_ids(apply(
        &db,
        &[
            "FT.SEARCH",
            "idx",
            "@embedding:[VECTOR_RANGE 0.03 $vec] @category:{jacket}",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "NOCONTENT",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(ids, vec!["doc:3"]);
}

#[test]
fn ft_hybrid_vector_reuses_search_vector_execution() {
    let (_dir, db) = seed_vector_index();
    let result = array(apply(
        &db,
        &[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "blue",
            "YIELD_SCORE_AS",
            "text_score",
            "VSIM",
            "@embedding",
            "$vec",
            "KNN",
            "4",
            "K",
            "1",
            "YIELD_DISTANCE_AS",
            "distance",
            "COMBINE",
            "RRF",
            "6",
            "WINDOW",
            "5",
            "CONSTANT",
            "10",
            "YIELD_SCORE_AS",
            "hybrid_score",
            "PARAMS",
            "2",
            "vec",
            "[0,1]",
            "LOAD",
            "3",
            "text_score",
            "distance",
            "hybrid_score",
            "WITHSCORES",
            "DIALECT",
            "2",
        ],
    ));
    assert_eq!(integer(&result[0]), 1);
    assert_eq!(bulk_text(&result[1]), "doc:2");
    let score = bulk_text(&result[2]).parse::<f32>().unwrap();
    assert!(score > 0.0);
    assert!(field_value(&result[3], "text_score").is_some());
    assert!(field_value(&result[3], "distance").is_some());
    assert!(field_value(&result[3], "hybrid_score").is_some());
}

#[test]
fn ft_hybrid_collects_only_the_requested_combiner_window() {
    let (_dir, db) = seed_vector_index();
    apply(&db, &["FT.CONFIG", "SET", "MAXAGGREGATERESULTS", "2"]);

    let result = array(apply(
        &db,
        &[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "red|blue",
            "VSIM",
            "@embedding",
            "$vec",
            "KNN",
            "2",
            "K",
            "2",
            "COMBINE",
            "RRF",
            "4",
            "WINDOW",
            "2",
            "CONSTANT",
            "10",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "DIALECT",
            "2",
        ],
    ));
    assert!(matches!(result.first(), Some(Frame::Integer(2))));
    assert_eq!(result.len(), 5);
}

#[test]
fn ft_search_vector_decodes_all_declared_binary_element_types() {
    let (_dir, db) = make_db();
    let fields = [
        ("v_f32", "FLOAT32"),
        ("v_f64", "FLOAT64"),
        ("v_bf16", "BFLOAT16"),
        ("v_f16", "FLOAT16"),
        ("v_i8", "INT8"),
        ("v_u8", "UINT8"),
    ];
    let mut create = vec![
        bulk(b"FT.CREATE".to_vec()),
        bulk(b"typed".to_vec()),
        bulk(b"ON".to_vec()),
        bulk(b"HASH".to_vec()),
        bulk(b"PREFIX".to_vec()),
        bulk(b"1".to_vec()),
        bulk(b"typed:".to_vec()),
        bulk(b"SCHEMA".to_vec()),
        bulk(b"title".to_vec()),
        bulk(b"TEXT".to_vec()),
    ];
    for (field, element_type) in fields {
        create.extend([
            bulk(field.as_bytes().to_vec()),
            bulk(b"VECTOR".to_vec()),
            bulk(b"HNSW".to_vec()),
            bulk(b"6".to_vec()),
            bulk(b"TYPE".to_vec()),
            bulk(element_type.as_bytes().to_vec()),
            bulk(b"DIM".to_vec()),
            bulk(b"2".to_vec()),
            bulk(b"DISTANCE_METRIC".to_vec()),
            bulk(b"L2".to_vec()),
        ]);
    }
    assert!(matches!(apply_frames(&db, create), Frame::Ok));

    let typed_vectors = [
        (
            "v_f32",
            [1.0_f32, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>(),
        ),
        (
            "v_f64",
            [1.0_f64, 0.0]
                .into_iter()
                .flat_map(f64::to_le_bytes)
                .collect::<Vec<_>>(),
        ),
        (
            "v_bf16",
            [half::bf16::from_f32(1.0), half::bf16::from_f32(0.0)]
                .into_iter()
                .flat_map(|value| value.to_bits().to_le_bytes())
                .collect::<Vec<_>>(),
        ),
        (
            "v_f16",
            [half::f16::from_f32(1.0), half::f16::from_f32(0.0)]
                .into_iter()
                .flat_map(|value| value.to_bits().to_le_bytes())
                .collect::<Vec<_>>(),
        ),
        ("v_i8", vec![1_i8 as u8, 0_i8 as u8]),
        ("v_u8", vec![1_u8, 0_u8]),
    ];
    let mut hset = vec![
        bulk(b"HSET".to_vec()),
        bulk(b"typed:1".to_vec()),
        bulk(b"title".to_vec()),
        bulk(b"typed vector".to_vec()),
    ];
    for (field, vector) in &typed_vectors {
        hset.push(bulk(field.as_bytes().to_vec()));
        hset.push(bulk(vector.clone()));
    }
    assert!(matches!(apply_frames(&db, hset), Frame::Integer(7)));

    for (field, vector) in typed_vectors {
        let query = format!("*=>[KNN 1 @{field} $vec]");
        let result = apply_frames(
            &db,
            vec![
                bulk(b"FT.SEARCH".to_vec()),
                bulk(b"typed".to_vec()),
                bulk(query.into_bytes()),
                bulk(b"PARAMS".to_vec()),
                bulk(b"2".to_vec()),
                bulk(b"vec".to_vec()),
                bulk(vector),
                bulk(b"NOCONTENT".to_vec()),
                bulk(b"DIALECT".to_vec()),
                bulk(b"2".to_vec()),
            ],
        );
        assert_eq!(search_ids(result), vec!["typed:1"], "field {field}");
    }
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
