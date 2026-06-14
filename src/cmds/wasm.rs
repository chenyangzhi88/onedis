use std::sync::Arc;

use anyhow::{Error, Result};

use crate::frame::Frame;
use crate::store::db::Db;
use crate::wasm::{WasmRegistry, WasmValue};

pub enum WasmCommand {
    Load {
        name: String,
        bytes: Vec<u8>,
    },
    Call {
        name: String,
        function: String,
        args: Vec<String>,
        read_only: bool,
    },
    Scan {
        name: String,
        function: String,
        prefix: String,
        limit: usize,
    },
    Delete {
        name: String,
    },
    FunctionLoad {
        name: String,
        bytes: Vec<u8>,
    },
    FunctionDelete {
        name: String,
    },
    FunctionList,
    List,
}

impl WasmCommand {
    pub fn parse_from_frame(frame: Frame) -> Result<Self> {
        let command = frame
            .get_arg(0)
            .ok_or_else(|| Error::msg("ERR empty command"))?
            .to_ascii_uppercase();
        match command.as_str() {
            "WASM.LOAD" => {
                if frame.arg_len() != 3 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.load' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let bytes = frame
                    .get_arg_bytes(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module bytes"))?;
                Ok(Self::Load { name, bytes })
            }
            "WASM.CALL" | "WASM.CALL_RO" => {
                if frame.arg_len() < 3 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.call' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let function = frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm function name"))?;
                let args = frame.get_args_from_index(3);
                Ok(Self::Call {
                    name,
                    function,
                    args,
                    read_only: command == "WASM.CALL_RO",
                })
            }
            "WASM.DEL" => {
                if frame.arg_len() != 2 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.del' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                Ok(Self::Delete { name })
            }
            "WASM.SCAN" => {
                if frame.arg_len() < 4 || frame.arg_len() > 5 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.scan' command",
                    ));
                }
                let name = frame
                    .get_arg(1)
                    .ok_or_else(|| Error::msg("ERR invalid wasm module name"))?;
                let function = frame
                    .get_arg(2)
                    .ok_or_else(|| Error::msg("ERR invalid wasm function name"))?;
                let prefix = frame
                    .get_arg(3)
                    .ok_or_else(|| Error::msg("ERR invalid wasm scan prefix"))?;
                let limit = match frame.get_arg(4) {
                    Some(value) => value
                        .parse::<usize>()
                        .map_err(|_| Error::msg("ERR invalid wasm scan limit"))?,
                    None => 1000,
                };
                Ok(Self::Scan {
                    name,
                    function,
                    prefix,
                    limit,
                })
            }
            "WASM.LIST" => {
                if frame.arg_len() != 1 {
                    return Err(Error::msg(
                        "ERR wrong number of arguments for 'wasm.list' command",
                    ));
                }
                Ok(Self::List)
            }
            "FUNCTION" => parse_function_command(frame),
            "FCALL" | "FCALL_RO" => parse_fcall_command(frame, command == "FCALL_RO"),
            _ => Err(Error::msg("ERR unknown wasm command")),
        }
    }

    pub async fn apply(self, registry: &Arc<WasmRegistry>, db: Arc<Db>) -> Frame {
        match self {
            Self::Load { name, bytes } => match registry.load(&name, &bytes) {
                Ok(()) => Frame::Ok,
                Err(error) => wasm_error_frame(error),
            },
            Self::Call {
                name,
                function,
                args,
                read_only,
            } => match call_wasm(registry, db, &name, &function, &args, read_only).await {
                Ok(values) => wasm_values_frame(values),
                Err(error) => wasm_error_frame(error),
            },
            Self::Scan {
                name,
                function,
                prefix,
                limit,
            } => match registry.scan(db, &name, &function, &prefix, limit).await {
                Ok(keys) => Frame::Array(keys.into_iter().map(Frame::bulk_string).collect()),
                Err(error) => wasm_error_frame(error),
            },
            Self::Delete { name } => Frame::Integer(i64::from(registry.delete(&name))),
            Self::FunctionLoad { name, bytes } => match registry.load(&name, &bytes) {
                Ok(()) => Frame::Ok,
                Err(error) => wasm_error_frame(error),
            },
            Self::FunctionDelete { name } => Frame::Integer(i64::from(registry.delete(&name))),
            Self::FunctionList => Frame::Array(
                registry
                    .list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
            Self::List => Frame::Array(
                registry
                    .list()
                    .into_iter()
                    .map(Frame::bulk_string)
                    .collect(),
            ),
        }
    }
}

fn wasm_error_frame(error: Error) -> Frame {
    Frame::Error(error.to_string().replace(['\r', '\n'], " "))
}

async fn call_wasm(
    registry: &Arc<WasmRegistry>,
    db: Arc<Db>,
    name: &str,
    function: &str,
    args: &[String],
    read_only: bool,
) -> Result<Vec<WasmValue>> {
    if read_only {
        return registry.call(db, name, function, args, true).await;
    }
    let txn_db = Arc::new(db.transactional_view()?);
    let values = registry
        .call(txn_db.clone(), name, function, args, false)
        .await?;
    txn_db.commit_transaction_async().await?;
    Ok(values)
}

fn parse_function_command(frame: Frame) -> Result<WasmCommand> {
    if frame.arg_len() < 2 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'function' command",
        ));
    }
    let subcommand = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid function subcommand"))?
        .to_ascii_uppercase();
    match subcommand.as_str() {
        "LOAD" => {
            if frame.arg_len() != 4 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function load' command",
                ));
            }
            let name = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR invalid function name"))?;
            let bytes = frame
                .get_arg_bytes(3)
                .ok_or_else(|| Error::msg("ERR invalid function payload"))?;
            Ok(WasmCommand::FunctionLoad { name, bytes })
        }
        "DELETE" | "DEL" => {
            if frame.arg_len() != 3 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function delete' command",
                ));
            }
            let name = frame
                .get_arg(2)
                .ok_or_else(|| Error::msg("ERR invalid function name"))?;
            Ok(WasmCommand::FunctionDelete { name })
        }
        "LIST" => {
            if frame.arg_len() != 2 {
                return Err(Error::msg(
                    "ERR wrong number of arguments for 'function list' command",
                ));
            }
            Ok(WasmCommand::FunctionList)
        }
        _ => Err(Error::msg("ERR unsupported function subcommand")),
    }
}

fn parse_fcall_command(frame: Frame, read_only: bool) -> Result<WasmCommand> {
    if frame.arg_len() < 3 {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'fcall' command",
        ));
    }
    let function_ref = frame
        .get_arg(1)
        .ok_or_else(|| Error::msg("ERR invalid function name"))?;
    let numkeys = frame
        .get_arg(2)
        .ok_or_else(|| Error::msg("ERR invalid numkeys"))?
        .parse::<usize>()
        .map_err(|_| Error::msg("ERR invalid numkeys"))?;
    if frame.arg_len() < 3 + numkeys {
        return Err(Error::msg(
            "ERR wrong number of arguments for 'fcall' command",
        ));
    }
    let (name, function) = function_ref
        .split_once('.')
        .ok_or_else(|| Error::msg("ERR function name must be module.function"))?;
    let args = frame.get_args_from_index(3);
    Ok(WasmCommand::Call {
        name: name.to_string(),
        function: function.to_string(),
        args,
        read_only,
    })
}

fn wasm_values_frame(values: Vec<WasmValue>) -> Frame {
    Frame::Array(
        values
            .into_iter()
            .map(|value| {
                Frame::Array(vec![
                    Frame::bulk_string(value.type_name()),
                    Frame::bulk_string(value.value_string()),
                ])
            })
            .collect(),
    )
}

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
