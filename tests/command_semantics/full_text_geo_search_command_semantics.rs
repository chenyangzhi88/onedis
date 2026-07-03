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

fn total(frame: &Frame) -> Option<i64> {
    let Frame::Array(items) = frame else {
        return None;
    };
    let Some(Frame::Integer(total)) = items.first() else {
        return None;
    };
    Some(*total)
}

fn wait_total(db: &Db, args: &[&str], expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = Frame::Null;
    while Instant::now() < deadline {
        last = apply(db, args);
        if total(&last) == Some(expected) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("expected total {expected}, got {}", last.to_string());
}

#[test]
fn ft_geo_query_and_geofilter() {
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
                "place:",
                "SCHEMA",
                "name",
                "TEXT",
                "loc",
                "GEO"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(&db, &["HSET", "place:1", "name", "near", "loc", "0,0"]).to_string(),
        "2"
    );
    assert_eq!(
        apply(&db, &["HSET", "place:2", "name", "far", "loc", "10,10"]).to_string(),
        "2"
    );
    wait_total(&db, &["FT.SEARCH", "idx", "near"], 1);

    assert_eq!(
        total(&apply(&db, &["FT.SEARCH", "idx", "@loc:[0 0 5 km]"])),
        Some(1)
    );
    assert_eq!(
        total(&apply(
            &db,
            &[
                "FT.SEARCH",
                "idx",
                "*",
                "GEOFILTER",
                "loc",
                "0",
                "0",
                "5",
                "km"
            ],
        )),
        Some(1)
    );
    assert_eq!(
        total(&apply(
            &db,
            &[
                "FT.SEARCH",
                "idx",
                "far",
                "GEOFILTER",
                "loc",
                "0",
                "0",
                "5",
                "km"
            ],
        )),
        Some(0)
    );
}

#[test]
fn ft_geoshape_within_and_contains() {
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
                "shape:",
                "SCHEMA",
                "geom",
                "GEOSHAPE",
                "FLAT"
            ],
        ),
        Frame::Ok
    ));
    assert_eq!(
        apply(&db, &["HSET", "shape:1", "geom", "POINT(1 1)"]).to_string(),
        "1"
    );
    assert_eq!(
        apply(
            &db,
            &[
                "HSET",
                "shape:2",
                "geom",
                "POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))"
            ],
        )
        .to_string(),
        "1"
    );

    assert_eq!(
        total(&apply(
            &db,
            &[
                "FT.SEARCH",
                "idx",
                "@geom:[WITHIN POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))]"
            ],
        )),
        Some(1)
    );
    assert_eq!(
        total(&apply(
            &db,
            &["FT.SEARCH", "idx", "@geom:[CONTAINS POINT(2.5 2.5)]"]
        )),
        Some(1)
    );
}

#[test]
fn ft_geoshape_invalid_wkt_returns_error() {
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
                "shape:",
                "SCHEMA",
                "geom",
                "GEOSHAPE",
                "FLAT"
            ],
        ),
        Frame::Ok
    ));
    let err = match apply_result(
        &db,
        &["FT.SEARCH", "idx", "@geom:[WITHIN LINESTRING(0 0, 1 1)]"],
    ) {
        Ok(frame) => panic!("invalid WKT should fail, got {}", frame.to_string()),
        Err(err) => err,
    };
    assert!(err.to_string().contains("WKT"));

    assert_eq!(
        apply(&db, &["HSET", "shape:1", "geom", "POINT(1 1)"]).to_string(),
        "1"
    );
    let err = match apply_result(
        &db,
        &["FT.SEARCH", "idx", "@geom:[WITHIN LINESTRING(0 0, 1 1)]"],
    ) {
        Ok(frame) => panic!("invalid WKT should fail, got {}", frame.to_string()),
        Err(err) => err,
    };
    assert!(err.to_string().contains("WKT"));
}
