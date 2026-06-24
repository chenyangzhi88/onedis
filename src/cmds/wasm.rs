use std::sync::Arc;

use anyhow::{Error, Result};

use crate::frame::Frame;
use crate::store::db::Db;
use crate::wasm::{WasmRegistry, WasmValue};

mod apply;
mod command_types;
mod parsing;
mod response_frames;

pub use command_types::WasmCommand;

#[cfg(test)]
use response_frames::{wasm_error_frame, wasm_values_frame};

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(args: Vec<Frame>) -> Frame {
        Frame::Array(args)
    }

    fn text_args(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn test_db() -> Db {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"))
            .join(format!("onedis-wasm-cmd-{unique}"));
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = crate::store::kv_store::KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
        let ttl_manager = crate::store::ttl::TtlManager::new(
            store.clone(),
            crate::store::ttl::TtlConfig::default(),
        );
        Db::new(0, store, version_counter, ttl_manager)
    }

    #[test]
    fn load_parser_preserves_binary_module_bytes() {
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0xff, 0x00, 0x80];
        let frame = frame(vec![
            Frame::bulk_string("WASM.LOAD"),
            Frame::bulk_string("math"),
            Frame::BulkString(wasm_bytes.clone()),
        ]);

        match WasmCommand::parse_from_frame(frame).unwrap() {
            WasmCommand::Load { name, bytes } => {
                assert_eq!(name, "math");
                assert_eq!(bytes, wasm_bytes);
            }
            _ => panic!("expected WASM.LOAD"),
        }
    }

    #[test]
    fn wasm_parser_covers_all_variants_and_option_defaults() {
        match WasmCommand::parse_from_frame(text_args(&["WASM.CALL", "math", "add", "1", "2"]))
            .unwrap()
        {
            WasmCommand::Call {
                name,
                function,
                args,
                read_only,
            } => {
                assert_eq!(name, "math");
                assert_eq!(function, "add");
                assert_eq!(args, vec!["1".to_string(), "2".to_string()]);
                assert!(!read_only);
            }
            _ => panic!("expected WASM.CALL"),
        }

        match WasmCommand::parse_from_frame(text_args(&["WASM.CALL_RO", "math", "get"])).unwrap() {
            WasmCommand::Call {
                name,
                function,
                args,
                read_only,
            } => {
                assert_eq!(name, "math");
                assert_eq!(function, "get");
                assert!(args.is_empty());
                assert!(read_only);
            }
            _ => panic!("expected WASM.CALL_RO"),
        }

        match WasmCommand::parse_from_frame(text_args(&["WASM.SCAN", "mod", "filter", "user:"]))
            .unwrap()
        {
            WasmCommand::Scan {
                name,
                function,
                prefix,
                limit,
            } => {
                assert_eq!(name, "mod");
                assert_eq!(function, "filter");
                assert_eq!(prefix, "user:");
                assert_eq!(limit, 1000);
            }
            _ => panic!("expected WASM.SCAN"),
        }
        match WasmCommand::parse_from_frame(text_args(&[
            "WASM.SCAN",
            "mod",
            "filter",
            "user:",
            "7",
        ]))
        .unwrap()
        {
            WasmCommand::Scan { limit, .. } => assert_eq!(limit, 7),
            _ => panic!("expected WASM.SCAN"),
        }

        assert!(matches!(
            WasmCommand::parse_from_frame(text_args(&["WASM.DEL", "mod"])).unwrap(),
            WasmCommand::Delete { .. }
        ));
        assert!(matches!(
            WasmCommand::parse_from_frame(text_args(&["WASM.LIST"])).unwrap(),
            WasmCommand::List
        ));
        assert!(matches!(
            WasmCommand::parse_from_frame(frame(vec![
                Frame::bulk_string("FUNCTION"),
                Frame::bulk_string("LOAD"),
                Frame::bulk_string("mod"),
                Frame::BulkString(vec![0, 97, 115, 109]),
            ]))
            .unwrap(),
            WasmCommand::FunctionLoad { .. }
        ));
        assert!(matches!(
            WasmCommand::parse_from_frame(text_args(&["FUNCTION", "DELETE", "mod"])).unwrap(),
            WasmCommand::FunctionDelete { .. }
        ));
        assert!(matches!(
            WasmCommand::parse_from_frame(text_args(&["FUNCTION", "DEL", "mod"])).unwrap(),
            WasmCommand::FunctionDelete { .. }
        ));
        assert!(matches!(
            WasmCommand::parse_from_frame(text_args(&["FUNCTION", "LIST"])).unwrap(),
            WasmCommand::FunctionList
        ));

        match WasmCommand::parse_from_frame(text_args(&[
            "FCALL_RO", "mod.sum", "2", "k1", "k2", "a",
        ]))
        .unwrap()
        {
            WasmCommand::Call {
                name,
                function,
                args,
                read_only,
            } => {
                assert_eq!(name, "mod");
                assert_eq!(function, "sum");
                assert_eq!(
                    args,
                    vec!["k1".to_string(), "k2".to_string(), "a".to_string()]
                );
                assert!(read_only);
            }
            _ => panic!("expected FCALL_RO"),
        }
    }

    #[test]
    fn wasm_parser_reports_argument_errors_and_helper_frames_are_stable() {
        for args in [
            vec!["WASM.LOAD", "mod"],
            vec!["WASM.CALL", "mod"],
            vec!["WASM.DEL"],
            vec!["WASM.SCAN", "mod", "filter"],
            vec!["WASM.SCAN", "mod", "filter", "p", "bad"],
            vec!["WASM.LIST", "extra"],
            vec!["FUNCTION"],
            vec!["FUNCTION", "LOAD", "mod"],
            vec!["FUNCTION", "DELETE"],
            vec!["FUNCTION", "LIST", "extra"],
            vec!["FUNCTION", "UNKNOWN"],
            vec!["FCALL", "missing-dot", "0"],
            vec!["FCALL", "mod.fn", "bad"],
            vec!["FCALL", "mod.fn", "2", "only-one-key"],
            vec!["WASM.UNKNOWN"],
        ] {
            assert!(
                WasmCommand::parse_from_frame(text_args(&args)).is_err(),
                "{args:?} should fail"
            );
        }

        let error = wasm_error_frame(Error::msg("line1\r\nline2"));
        assert!(matches!(error, Frame::Error(message) if message == "line1  line2"));

        let values = wasm_values_frame(vec![
            WasmValue::I32(1),
            WasmValue::I64(2),
            WasmValue::F32(3.5),
            WasmValue::F64(4.5),
        ]);
        let Frame::Array(items) = values else {
            panic!("expected value array");
        };
        assert_eq!(items.len(), 4);
        assert!(
            matches!(&items[0], Frame::Array(pair) if matches!(pair.as_slice(), [Frame::BulkString(kind), Frame::BulkString(value)] if kind == b"i32" && value == b"1"))
        );
        assert!(
            matches!(&items[3], Frame::Array(pair) if matches!(pair.as_slice(), [Frame::BulkString(kind), Frame::BulkString(value)] if kind == b"f64" && value == b"4.5"))
        );
    }

    #[tokio::test]
    async fn wasm_apply_covers_registry_list_delete_and_invalid_load_errors() {
        let registry = Arc::new(WasmRegistry::new());
        let db = Arc::new(test_db());

        assert!(matches!(
            WasmCommand::List.apply(&registry, db.clone()).await,
            Frame::Array(items) if items.is_empty()
        ));
        assert!(matches!(
            (WasmCommand::Delete {
                name: "missing".to_string()
            })
            .apply(&registry, db.clone())
            .await,
            Frame::Integer(0)
        ));
        assert!(matches!(
            (WasmCommand::Load {
                name: "".to_string(),
                bytes: vec![0, 1, 2]
            })
            .apply(&registry, db)
            .await,
            Frame::Error(_)
        ));
    }
}
