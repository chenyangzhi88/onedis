#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        kv_store::KvStore,
        ttl::{TtlConfig, TtlManager, VersionCounter},
    };
    use std::sync::Arc;

    fn add_i64_module() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7e,
            0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64,
            0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x7c, 0x0b,
        ]
    }

    fn wasm_leb(mut value: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }

    fn wasm_name(name: &str) -> Vec<u8> {
        let mut bytes = wasm_leb(name.len() as u32);
        bytes.extend_from_slice(name.as_bytes());
        bytes
    }

    fn wasm_vec(items: Vec<Vec<u8>>) -> Vec<u8> {
        let mut bytes = wasm_leb(items.len() as u32);
        for item in items {
            bytes.extend(item);
        }
        bytes
    }

    fn wasm_section(id: u8, payload: Vec<u8>) -> Vec<u8> {
        let mut bytes = vec![id];
        bytes.extend(wasm_leb(payload.len() as u32));
        bytes.extend(payload);
        bytes
    }

    fn wasm_func_type(params: &[u8], results: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x60];
        bytes.extend(wasm_leb(params.len() as u32));
        bytes.extend_from_slice(params);
        bytes.extend(wasm_leb(results.len() as u32));
        bytes.extend_from_slice(results);
        bytes
    }

    fn wasm_import_func(name: &str, type_idx: u32) -> Vec<u8> {
        let mut bytes = wasm_name("onedis");
        bytes.extend(wasm_name(name));
        bytes.push(0x00);
        bytes.extend(wasm_leb(type_idx));
        bytes
    }

    fn wasm_export_func(name: &str, func_idx: u32) -> Vec<u8> {
        let mut bytes = wasm_name(name);
        bytes.push(0x00);
        bytes.extend(wasm_leb(func_idx));
        bytes
    }

    fn wasm_i32_const(value: i32) -> Vec<u8> {
        let mut bytes = vec![0x41];
        let mut remaining = value;
        loop {
            let byte = (remaining as u8) & 0x7f;
            remaining >>= 7;
            let done =
                (remaining == 0 && (byte & 0x40) == 0) || (remaining == -1 && (byte & 0x40) != 0);
            bytes.push(if done { byte } else { byte | 0x80 });
            if done {
                break;
            }
        }
        bytes
    }

    fn wasm_call(func_idx: u32) -> Vec<u8> {
        let mut bytes = vec![0x10];
        bytes.extend(wasm_leb(func_idx));
        bytes
    }

    fn wasm_body(instructions: Vec<u8>) -> Vec<u8> {
        let mut body = vec![0x00];
        body.extend(instructions);
        body.push(0x0b);
        let mut bytes = wasm_leb(body.len() as u32);
        bytes.extend(body);
        bytes
    }

    fn wasm_data(offset: u32, data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend(wasm_i32_const(offset as i32));
        bytes.push(0x0b);
        bytes.extend(wasm_leb(data.len() as u32));
        bytes.extend_from_slice(data);
        bytes
    }

    fn host_import_module() -> Vec<u8> {
        const I32: u8 = 0x7f;
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        module.extend(wasm_section(
            1,
            wasm_vec(vec![
                wasm_func_type(&[I32, I32, I32, I32], &[I32]),
                wasm_func_type(&[I32, I32], &[I32]),
                wasm_func_type(&[I32, I32, I32, I32, I32, I32], &[I32]),
                wasm_func_type(&[], &[I32]),
                wasm_func_type(&[I32, I32, I32, I32], &[I32]),
            ]),
        ));
        module.extend(wasm_section(
            2,
            wasm_vec(vec![
                wasm_import_func("redis_get", 0),
                wasm_import_func("redis_set", 0),
                wasm_import_func("redis_del", 1),
                wasm_import_func("redis_hget", 2),
                wasm_import_func("redis_hset", 2),
                wasm_import_func("redis_call", 2),
            ]),
        ));
        module.extend(wasm_section(
            3,
            wasm_vec(vec![
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(3),
                wasm_leb(4),
            ]),
        ));
        module.extend(wasm_section(5, wasm_vec(vec![vec![0x00, 0x02]])));
        let mut memory_export = wasm_name("memory");
        memory_export.push(0x02);
        memory_export.extend(wasm_leb(0));
        module.extend(wasm_section(
            7,
            wasm_vec(vec![
                memory_export,
                wasm_export_func("set_key", 6),
                wasm_export_func("get_key", 7),
                wasm_export_func("get_key_tiny_cap", 8),
                wasm_export_func("del_key", 9),
                wasm_export_func("hset_field", 10),
                wasm_export_func("hget_field", 11),
                wasm_export_func("call_set", 12),
                wasm_export_func("call_get", 13),
                wasm_export_func("call_unknown", 14),
                wasm_export_func("scan_accept", 15),
            ]),
        ));

        let call4 = |func_idx: u32, a: i32, b: i32, c: i32, d: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_i32_const(c));
            body.extend(wasm_i32_const(d));
            body.extend(wasm_call(func_idx));
            body
        };
        let call2 = |func_idx: u32, a: i32, b: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_call(func_idx));
            body
        };
        let call6 = |func_idx: u32, a: i32, b: i32, c: i32, d: i32, e: i32, f: i32| {
            let mut body = Vec::new();
            body.extend(wasm_i32_const(a));
            body.extend(wasm_i32_const(b));
            body.extend(wasm_i32_const(c));
            body.extend(wasm_i32_const(d));
            body.extend(wasm_i32_const(e));
            body.extend(wasm_i32_const(f));
            body.extend(wasm_call(func_idx));
            body
        };
        module.extend(wasm_section(
            10,
            wasm_vec(vec![
                wasm_body(call4(1, 0, 4, 16, 6)),
                wasm_body(call4(0, 0, 4, 256, 64)),
                wasm_body(call4(0, 0, 4, 256, 1)),
                wasm_body(call2(2, 0, 4)),
                wasm_body(call6(4, 0, 4, 32, 5, 48, 6)),
                wasm_body(call6(3, 0, 4, 32, 5, 256, 64)),
                wasm_body(call6(5, 80, 3, 128, 20, 256, 64)),
                wasm_body(call6(5, 84, 3, 160, 9, 256, 64)),
                wasm_body(call6(5, 88, 4, 180, 0, 256, 64)),
                wasm_body(wasm_i32_const(1)),
            ]),
        ));
        module.extend(wasm_section(
            11,
            wasm_vec(vec![
                wasm_data(0, b"wkey"),
                wasm_data(16, b"wvalue"),
                wasm_data(32, b"field"),
                wasm_data(48, b"hvalue"),
                wasm_data(80, b"SET"),
                wasm_data(84, b"GET"),
                wasm_data(88, b"NOPE"),
                wasm_data(128, b"call-key\0call-value\0"),
                wasm_data(160, b"call-key\0"),
            ]),
        ));
        module
    }

    fn test_db() -> Arc<Db> {
        let unique = format!(
            "onedis-wasm-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target"))
            .join("onedis-test-data")
            .join(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Arc::new(Db::new(0, store, version_counter, ttl_manager))
    }

    #[tokio::test]
    async fn wasm_registry_loads_and_calls_i64_function() {
        let registry = WasmRegistry::new();
        registry.load("math", &add_i64_module()).unwrap();
        let result = registry
            .call(
                test_db(),
                "math",
                "add",
                &["40".to_string(), "2".to_string()],
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, vec![WasmValue::I64(42)]);
    }

    #[tokio::test]
    async fn wasm_host_imports_drive_string_hash_call_scan_and_readonly_edges() {
        let registry = WasmRegistry::new();
        registry
            .load("host", &host_import_module())
            .unwrap_or_else(|err| panic!("{err:#}"));
        let db = test_db();

        assert_eq!(
            registry
                .call(db.clone(), "host", "set_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            db.get_string_bytes_async("wkey").await.unwrap(),
            Some(b"wvalue".to_vec())
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "get_key", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(6)]
        );
        assert!(
            registry
                .call(db.clone(), "host", "get_key_tiny_cap", &[], true)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(db.clone(), "host", "set_key", &[], true)
                .await
                .is_err()
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "del_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(db.get_string_bytes_async("wkey").await.unwrap(), None);

        assert_eq!(
            registry
                .call(db.clone(), "host", "hset_field", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "hget_field", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(6)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "call_set", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "call_get", &[], true)
                .await
                .unwrap(),
            vec![WasmValue::I32(10)]
        );
        assert!(
            registry
                .call(db.clone(), "host", "call_unknown", &[], true)
                .await
                .is_err()
        );
        assert_eq!(
            registry
                .call(db.clone(), "host", "del_key", &[], false)
                .await
                .unwrap(),
            vec![WasmValue::I32(1)]
        );
        assert!(db.hash_get_async("wkey", "field").await.unwrap().is_none());

        db.insert_string_ref("scan:1", "one");
        db.insert_string_ref("scan:2", "two");
        let mut matched = registry
            .scan(db.clone(), "host", "scan_accept", "scan:", 10)
            .await
            .unwrap();
        matched.sort();
        assert_eq!(matched, vec!["scan:1".to_string(), "scan:2".to_string()]);
    }

    #[test]
    fn wasm_registry_validates_names_lists_deletes_and_rejects_invalid_modules() {
        let registry = WasmRegistry::new();

        for name in ["", "bad name", "bad/slash", "bad:colon"] {
            assert!(registry.load(name, &add_i64_module()).is_err(), "{name}");
            assert!(validate_name(name).is_err(), "{name}");
        }
        for name in ["math", "math.v1", "math-v1", "math_v1"] {
            validate_name(name).unwrap();
        }

        registry.load("z", &add_i64_module()).unwrap();
        registry.load("a", &add_i64_module()).unwrap();
        assert_eq!(registry.list(), vec!["a".to_string(), "z".to_string()]);
        assert!(registry.delete("a"));
        assert!(!registry.delete("a"));
        assert_eq!(registry.list(), vec!["z".to_string()]);

        assert!(registry.load("bad", b"not wasm").is_err());
        assert!(
            registry
                .load("huge", &vec![0u8; 16 * 1024 * 1024 + 1])
                .is_err()
        );
    }

    #[test]
    fn wasm_private_helpers_cover_argument_and_value_conversion_edges() {
        assert!(is_allowed_host_import("redis_get"));
        assert!(is_allowed_host_import("redis_call"));
        assert!(!is_allowed_host_import("redis_eval"));

        assert_eq!(
            split_nul_args(b"GET\0key\0\0bad-\xff"),
            vec!["GET".to_string(), "key".to_string()]
        );

        assert!(matches!(
            parse_wasm_arg("7", ValType::I32).unwrap(),
            Val::I32(7)
        ));
        assert!(matches!(
            parse_wasm_arg("8", ValType::I64).unwrap(),
            Val::I64(8)
        ));
        assert!(matches!(
            parse_wasm_arg("1.5", ValType::F32).unwrap(),
            Val::F32(_)
        ));
        assert!(matches!(
            parse_wasm_arg("2.5", ValType::F64).unwrap(),
            Val::F64(_)
        ));
        assert!(parse_wasm_arg("bad", ValType::I32).is_err());
        assert!(parse_wasm_arg("bad", ValType::I64).is_err());
        assert!(parse_wasm_arg("bad", ValType::F32).is_err());
        assert!(parse_wasm_arg("bad", ValType::F64).is_err());

        let values = [
            WasmValue::I32(1),
            WasmValue::I64(2),
            WasmValue::F32(3.5),
            WasmValue::F64(4.5),
        ];
        assert_eq!(values[0].type_name(), "i32");
        assert_eq!(values[1].type_name(), "i64");
        assert_eq!(values[2].type_name(), "f32");
        assert_eq!(values[3].type_name(), "f64");
        assert_eq!(values[0].value_string(), "1");
        assert_eq!(values[1].value_string(), "2");
        assert_eq!(values[2].value_string(), "3.5");
        assert_eq!(values[3].value_string(), "4.5");

        assert_eq!(WasmValue::from_val(Val::I32(9)).unwrap(), WasmValue::I32(9));
        assert_eq!(
            WasmValue::from_val(Val::I64(10)).unwrap(),
            WasmValue::I64(10)
        );
        assert!(matches!(
            WasmValue::from_val(Val::F32(1.25f32.to_bits())).unwrap(),
            WasmValue::F32(value) if value == 1.25
        ));
        assert!(matches!(
            WasmValue::from_val(Val::F64(2.25f64.to_bits())).unwrap(),
            WasmValue::F64(value) if value == 2.25
        ));
    }

    #[tokio::test]
    async fn wasm_call_and_scan_report_missing_function_argument_and_signature_errors() {
        let registry = WasmRegistry::new();
        registry.load("math", &add_i64_module()).unwrap();

        assert!(
            registry
                .call(test_db(), "missing", "add", &[], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(test_db(), "math", "missing", &[], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(test_db(), "math", "add", &["1".to_string()], false)
                .await
                .is_err()
        );
        assert!(
            registry
                .call(
                    test_db(),
                    "math",
                    "add",
                    &["bad".to_string(), "2".to_string()],
                    false,
                )
                .await
                .is_err()
        );
        assert!(
            registry
                .scan(test_db(), "missing", "filter", "", 10)
                .await
                .is_err()
        );
        assert!(
            registry
                .scan(test_db(), "math", "add", "", 10)
                .await
                .is_err()
        );
    }

    #[test]
    fn wasm_limits_and_import_validation_cover_resource_edges() {
        let mut limits = WasmLimits::new(128);
        assert!(limits.memory_growing(0, 128, None).unwrap());
        assert!(!limits.memory_growing(0, 129, None).unwrap());
        assert!(limits.table_growing(0, 1024, None).unwrap());
        assert!(!limits.table_growing(0, 1025, None).unwrap());
        assert_eq!(limits.instances(), 4);
        assert_eq!(limits.tables(), 4);
        assert_eq!(limits.memories(), 4);

        let registry = WasmRegistry::new();
        let allowed_import = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
            0x02, 0x14, 0x01, 0x06, b'o', b'n', b'e', b'd', b'i', b's', 0x09, b'r', b'e', b'd',
            b'i', b's', b'_', b'g', b'e', b't', 0x00, 0x00,
        ];
        registry.load("allowed_import", &allowed_import).unwrap();

        let bad_import = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
            0x02, 0x09, 0x01, 0x03, b'b', b'a', b'd', 0x01, b'x', 0x00, 0x00,
        ];
        assert!(registry.load("bad_import", &bad_import).is_err());
    }
}
