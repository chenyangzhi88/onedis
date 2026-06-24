use super::{
    decode_score, distance_m, encode_score, parse_f, parse_non_negative_f, redis_geohash,
    unit_factor, validate_coord,
};
use crate::command::Command;
use crate::frame::Frame;
use crate::store::db::Db;
use crate::store::kv_store::KvStore;
use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
use std::sync::Arc;

fn test_db() -> Db {
    let unique = format!(
        "onedis-geo-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"))
        .join(unique);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn frame(args: &[&str]) -> Frame {
    Frame::Array(
        args.iter()
            .map(|arg| Frame::bulk_string((*arg).to_string()))
            .collect(),
    )
}

fn apply(db: &Db, args: &[&str]) -> Frame {
    let command = Command::parse_from_frame(frame(args)).unwrap();
    db.handle_command(command).unwrap()
}

async fn apply_async(db: &Db, args: &[&str]) -> Frame {
    let command = Command::parse_from_frame(frame(args)).unwrap();
    db.handle_command_async(command).await.unwrap()
}

fn parse_err(args: &[&str]) -> String {
    match Command::parse_from_frame(frame(args)) {
        Ok(command) => panic!("expected parse error, got {}", command.name()),
        Err(error) => error.to_string(),
    }
}

fn array(frame: Frame) -> Vec<Frame> {
    match frame {
        Frame::Array(values) => values,
        other => panic!("expected array, got {}", other.to_string()),
    }
}

#[tokio::test]
async fn geo_commands_cover_sync_async_store_and_legacy_shapes() {
    let db = test_db();

    assert!(matches!(
        apply(
            &db,
            &[
                "geoadd",
                "places",
                "13.361389",
                "38.115556",
                "palermo",
                "15.087269",
                "37.502669",
                "catania",
                "12.496366",
                "41.902782",
                "rome",
            ],
        ),
        Frame::Integer(3)
    ));
    assert!(matches!(
        apply(&db, &["geoadd", "places", "NX", "13.0", "38.0", "palermo"]),
        Frame::Integer(0)
    ));
    assert!(matches!(
        apply(
            &db,
            &["geoadd", "places", "XX", "CH", "13.5", "38.2", "palermo"]
        ),
        Frame::Integer(1)
    ));
    assert!(matches!(
        apply(&db, &["geoadd", "places", "XX", "1", "1", "missing"]),
        Frame::Integer(0)
    ));

    let positions = array(apply(
        &db,
        &["geopos", "places", "palermo", "missing", "catania"],
    ));
    assert!(matches!(positions[0], Frame::Array(_)));
    assert!(matches!(positions[1], Frame::Null));
    assert!(matches!(positions[2], Frame::Array(_)));

    assert!(matches!(
        apply(&db, &["geodist", "places", "palermo", "catania", "km"]),
        Frame::BulkString(value) if !value.is_empty()
    ));
    assert!(matches!(
        apply(&db, &["geodist", "places", "palermo", "missing"]),
        Frame::Null
    ));
    assert!(matches!(
        apply(&db, &["geohash", "places", "palermo", "missing"]),
        Frame::Array(values) if matches!(values.first(), Some(Frame::BulkString(hash)) if hash.len() == 11)
            && matches!(values.get(1), Some(Frame::Null))
    ));

    let rich = array(apply(
        &db,
        &[
            "geosearch",
            "places",
            "fromlonlat",
            "15",
            "37",
            "byradius",
            "200",
            "km",
            "withdist",
            "withhash",
            "withcoord",
            "asc",
            "count",
            "2",
            "any",
        ],
    ));
    assert_eq!(rich.len(), 2);
    assert!(matches!(rich[0], Frame::Array(_)));

    let any = array(apply(
        &db,
        &[
            "geosearch",
            "places",
            "fromlonlat",
            "15",
            "37",
            "byradius",
            "1000",
            "km",
            "count",
            "1",
            "any",
        ],
    ));
    assert_eq!(any.len(), 1);

    let box_result = array(apply(
        &db,
        &[
            "geosearch",
            "places",
            "frommember",
            "palermo",
            "bybox",
            "400",
            "400",
            "km",
            "desc",
        ],
    ));
    assert!(!box_result.is_empty());

    assert!(matches!(
        apply(
            &db,
            &[
                "georadius",
                "places",
                "15",
                "37",
                "200",
                "km",
                "store",
                "stored",
            ],
        ),
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(apply(&db, &["zcard", "stored"]), Frame::Integer(n) if n > 0));
    assert!(matches!(
        apply(
            &db,
            &[
                "georadiusbymember",
                "places",
                "palermo",
                "200",
                "km",
                "storedist",
                "distances",
            ],
        ),
        Frame::Integer(n) if n > 0
    ));
    assert!(matches!(
        apply(
            &db,
            &[
                "geosearchstore",
                "copy",
                "places",
                "frommember",
                "palermo",
                "byradius",
                "500",
                "km",
                "storedist",
            ],
        ),
        Frame::Integer(n) if n > 0
    ));

    assert!(matches!(
        apply_async(
            &db,
            &[
                "geosearch",
                "places",
                "fromlonlat",
                "15",
                "37",
                "byradius",
                "300",
                "km",
                "desc",
                "count",
                "1",
            ],
        )
        .await,
        Frame::Array(values) if values.len() == 1
    ));
    assert!(matches!(
        apply_async(
            &db,
            &[
                "geosearch",
                "places",
                "fromlonlat",
                "15",
                "37",
                "byradius",
                "1000",
                "km",
                "count",
                "1",
                "any",
            ],
        )
        .await,
        Frame::Array(values) if values.len() == 1
    ));
    assert!(matches!(
        apply_async(&db, &["geodist", "places", "palermo", "catania", "mi"]).await,
        Frame::BulkString(_)
    ));
    assert!(matches!(
        apply_async(&db, &["geohash", "places", "palermo"]).await,
        Frame::Array(values) if values.len() == 1
    ));
}

#[test]
fn geo_parser_validation_and_math_helpers_cover_error_edges() {
    assert!(parse_err(&["geoadd", "g"]).contains("wrong number"));
    assert!(parse_err(&["geoadd", "g", "NX", "XX", "1", "1", "m"]).contains("not compatible"));
    assert!(parse_err(&["geoadd", "g", "nan", "1", "m"]).contains("valid float"));
    assert!(parse_err(&["geoadd", "g", "181", "1", "m"]).contains("invalid longitude"));
    assert!(parse_err(&["geopos", "g"]).contains("wrong number"));
    assert!(parse_err(&["geodist", "g", "a"]).contains("wrong number"));
    assert!(parse_err(&["geohash", "g"]).contains("wrong number"));
    assert!(parse_err(&["geosearch", "g"]).contains("syntax"));
    assert!(parse_err(&["geosearch", "g", "frommember", "m"]).contains("syntax"));
    assert!(
        parse_err(&[
            "geosearch",
            "g",
            "fromlonlat",
            "0",
            "0",
            "byradius",
            "-1",
            "m"
        ])
        .contains("out of range")
    );
    assert!(
        parse_err(&[
            "geosearch",
            "g",
            "fromlonlat",
            "0",
            "0",
            "byradius",
            "1",
            "bad"
        ])
        .contains("unsupported unit")
    );
    assert!(
        parse_err(&[
            "geosearch",
            "g",
            "fromlonlat",
            "0",
            "0",
            "bybox",
            "1",
            "2",
            "m",
            "bad"
        ])
        .contains("syntax")
    );
    assert!(
        parse_err(&[
            "geosearch",
            "g",
            "fromlonlat",
            "0",
            "0",
            "byradius",
            "1",
            "m",
            "count",
            "x"
        ])
        .contains("integer")
    );
    assert!(parse_err(&["georadius", "g", "0"]).contains("wrong number"));
    assert!(parse_err(&["georadiusbymember", "g", "m"]).contains("wrong number"));
    assert!(parse_err(&["geosearchstore", "dst", "g"]).contains("syntax"));

    assert!(parse_f("1.25").unwrap() == 1.25);
    assert!(parse_f("inf").is_err());
    assert!(parse_non_negative_f("-0.1").is_err());
    assert!(unit_factor("m").unwrap() == 1.0);
    assert!(unit_factor("km").unwrap() == 1000.0);
    assert!(unit_factor("ft").unwrap() < 1.0);
    assert!(unit_factor("bad").is_err());
    assert!(validate_coord(-180.0, -85.05112878).is_ok());
    assert!(validate_coord(180.0, 85.05112878).is_ok());

    let score = encode_score(13.361389, 38.115556);
    let decoded = decode_score(score);
    assert!(distance_m((13.361389, 38.115556), decoded) < 1.0);
    assert_eq!(redis_geohash(13.361389, 38.115556).len(), 11);
}

#[test]
fn geo_search_missing_center_member_returns_resp_error_frame() {
    let db = test_db();
    let frame = apply(
        &db,
        &[
            "geosearch",
            "places",
            "frommember",
            "missing",
            "byradius",
            "10",
            "km",
        ],
    );
    assert!(matches!(frame, Frame::Error(message) if message.contains("could not decode")));
}
