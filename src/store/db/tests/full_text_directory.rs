use super::super::full_text_directory::KvTantivyDirectory;
use crate::store::kv_store::KvStore;
use std::{
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tantivy::directory::{
    Directory, TerminatingWrite, WatchCallback,
    error::{DeleteError, OpenReadError, OpenWriteError},
};

fn test_store() -> KvStore {
    let unique = format!(
        "onedis-full-text-dir-test-{}",
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
    KvStore::new(db_path, wal_dir, 1)
}

#[test]
fn chunked_directory_preserves_open_handles_and_incremental_manifest_bytes() {
    let store = test_store();
    let directory = KvTantivyDirectory::new(store.clone(), 7, "idx");
    assert!(format!("{directory:?}").contains("idx"));
    assert!(!directory.exists(Path::new("missing")).unwrap());
    assert!(matches!(
        directory.open_read(Path::new("missing")),
        Err(OpenReadError::FileDoesNotExist(_))
    ));
    assert!(matches!(
        directory.delete(Path::new("missing")),
        Err(DeleteError::FileDoesNotExist(_))
    ));

    let old = vec![b'a'; 1024 * 1024 + 37];
    directory
        .atomic_write(Path::new("segment.bin"), &old)
        .unwrap();
    assert_eq!(
        KvTantivyDirectory::storage_bytes(&store, 7, "idx"),
        old.len()
    );
    let open_handle = directory.get_file_handle(Path::new("segment.bin")).unwrap();
    assert_eq!(
        open_handle
            .read_bytes(1024 * 1024 - 8..1024 * 1024 + 8)
            .unwrap()
            .as_slice(),
        &old[1024 * 1024 - 8..1024 * 1024 + 8]
    );

    directory.delete(Path::new("segment.bin")).unwrap();
    assert_eq!(KvTantivyDirectory::storage_bytes(&store, 7, "idx"), 0);
    assert_eq!(
        open_handle.read_bytes(0..old.len()).unwrap().as_slice(),
        old.as_slice()
    );

    let replacement = b"replacement";
    directory
        .atomic_write(Path::new("segment.bin"), replacement)
        .unwrap();
    assert_eq!(
        open_handle.read_bytes(0..old.len()).unwrap().as_slice(),
        old.as_slice()
    );
    assert_eq!(
        directory.atomic_read(Path::new("segment.bin")).unwrap(),
        replacement
    );
    let mut chunk_root = crate::store::TABLE_LOCAL_INTERNAL_PREFIX.to_vec();
    chunk_root.extend_from_slice(&super::super::FULLTEXT_FILE_NAMESPACE);
    chunk_root.extend_from_slice(b"idx\0c\0");
    let retained_chunks = store.scan_prefix_raw(&chunk_root).len();
    assert!(retained_chunks >= 3);
    drop(open_handle);
    assert!(store.scan_prefix_raw(&chunk_root).len() < retained_chunks);
}

#[test]
fn directory_write_reservations_watch_and_sync_are_consistent() {
    let store = test_store();
    let directory = KvTantivyDirectory::new(store.clone(), 2, "idx");
    let watch_count = Arc::new(AtomicUsize::new(0));
    let watched = watch_count.clone();
    let _watch = directory
        .watch(WatchCallback::new(move || {
            watched.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap();
    directory
        .atomic_write(Path::new("meta.json"), br#"{"generation":1}"#)
        .unwrap();
    let watch_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < watch_deadline {
        if watch_count.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(watch_count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        directory.open_write(Path::new("meta.json")),
        Err(OpenWriteError::FileAlreadyExists(_))
    ));

    let mut writer = directory.open_write(Path::new("new.bin")).unwrap();
    assert!(matches!(
        directory.open_write(Path::new("new.bin")),
        Err(OpenWriteError::FileAlreadyExists(_))
    ));
    writer.write_all(b"abc").unwrap();
    writer.flush().unwrap();
    writer.write_all(b"def").unwrap();
    writer.terminate().unwrap();
    assert_eq!(
        directory.atomic_read(Path::new("new.bin")).unwrap(),
        b"abcdef"
    );
    directory.sync_directory().unwrap();
}
