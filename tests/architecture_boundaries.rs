use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN_ROOT_STORE_PATTERNS: &[&str] = &[".db()"];
const FORBIDDEN_KV_ENGINE_PATTERNS: &[&str] = &[
    "DbImpl",
    "SchemalessTable",
    "SchemalessTransaction",
    "open_schemaless_table",
    "create_schemaless_table",
];
const DB_PREFIX_CALL_PATTERN: &str = "db_prefix(";
const DB_PREFIX_ALLOWED_FILES: &[&str] = &[
    "src/store/db/key_encoding.rs",
    "src/store/db/keyspace_scan_admin.rs",
    "src/store/db/string_keyspace_flush.rs",
];

#[test]
fn production_code_does_not_bypass_kv_store_table_boundary() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_files(&src, &mut files);

    let mut violations = Vec::new();
    for file in files {
        if is_test_file(&file) {
            continue;
        }
        let relative = file
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap_or(&file)
            .to_path_buf();
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", file.display()));

        for pattern in FORBIDDEN_ROOT_STORE_PATTERNS {
            collect_pattern_violations(&relative, &content, pattern, &mut violations);
        }

        if !is_db_prefix_allowed_file(&relative) {
            collect_function_call_violations(
                &relative,
                &content,
                DB_PREFIX_CALL_PATTERN,
                &mut violations,
            );
        }

        if is_kv_store_wrapper_file(&relative) {
            continue;
        }
        for pattern in FORBIDDEN_KV_ENGINE_PATTERNS {
            collect_pattern_violations(&relative, &content, pattern, &mut violations);
        }
    }

    assert!(
        violations.is_empty(),
        "production code must use KvStore table-aware wrapper APIs instead of root DbImpl/schemaless table access:\n{}",
        violations.join("\n")
    );
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|err| panic!("failed to read {}: {err}", dir.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|err| panic!("failed to read entry in {}: {err}", dir.display()))
            .path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn is_test_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
        || path
            .components()
            .any(|component| component.as_os_str() == "tests")
}

fn is_kv_store_wrapper_file(path: &Path) -> bool {
    path.parent()
        .is_some_and(|parent| parent == Path::new("src/store"))
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name == "kv_store.rs"
                    || (name.starts_with("kv_store_") && !name.ends_with("_tests.rs"))
            })
}

fn is_db_prefix_allowed_file(path: &Path) -> bool {
    let path = path.to_string_lossy();
    DB_PREFIX_ALLOWED_FILES
        .iter()
        .any(|allowed| path == *allowed)
}

fn collect_function_call_violations(
    path: &Path,
    content: &str,
    pattern: &str,
    violations: &mut Vec<String>,
) {
    for (line_index, line) in content.lines().enumerate() {
        for (offset, _) in line.match_indices(pattern) {
            let previous = line[..offset].chars().next_back();
            if previous.is_some_and(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
                continue;
            }
            violations.push(format!(
                "{}:{} contains forbidden function call `{}`",
                path.display(),
                line_index + 1,
                pattern
            ));
        }
    }
}

fn collect_pattern_violations(
    path: &Path,
    content: &str,
    pattern: &str,
    violations: &mut Vec<String>,
) {
    for (line_index, line) in content.lines().enumerate() {
        if line.contains(pattern) {
            violations.push(format!(
                "{}:{} contains forbidden pattern `{}`",
                path.display(),
                line_index + 1,
                pattern
            ));
        }
    }
}
