use std::{
    fmt, io,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use common::types::write_batch::WriteBatch;
use tantivy::directory::{
    AntiCallToken, Directory, FileHandle, FileSlice, TerminatingWrite, WatchCallback,
    WatchCallbackList, WatchHandle, WritePtr,
    error::{DeleteError, OpenReadError, OpenWriteError},
};

use super::FULLTEXT_FILE_NAMESPACE;
use crate::store::kv_store::KvStore;

#[derive(Clone)]
pub struct KvTantivyDirectory {
    store: KvStore,
    db_index: u16,
    index: String,
    watchers: Arc<WatchCallbackList>,
}

struct KvDirectoryWriter {
    directory: KvTantivyDirectory,
    path: PathBuf,
    data: Cursor<Vec<u8>>,
}

impl KvTantivyDirectory {
    pub fn new(store: KvStore, db_index: u16, index: &str) -> Self {
        Self {
            store,
            db_index,
            index: index.to_string(),
            watchers: Arc::new(WatchCallbackList::default()),
        }
    }

    fn path_key(&self, path: &Path) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(path_to_key(path).as_bytes());
        key
    }

    fn file_prefix(&self) -> Vec<u8> {
        let mut key = self.db_index.to_be_bytes().to_vec();
        key.extend_from_slice(&FULLTEXT_FILE_NAMESPACE);
        key.extend_from_slice(self.index.as_bytes());
        key.push(0x00);
        key
    }

    fn put_file(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.store.blob_put_raw(&self.path_key(path), data);
        if path == Path::new("meta.json") {
            drop(self.watchers.broadcast());
        }
        Ok(())
    }
}

impl fmt::Debug for KvTantivyDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvTantivyDirectory")
            .field("db_index", &self.db_index)
            .field("index", &self.index)
            .finish()
    }
}

impl Directory for KvTantivyDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(self.open_read(path)?))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let raw = self
            .store
            .get_raw(&self.path_key(path))
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(FileSlice::from(raw))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let key = self.path_key(path);
        if !self.store.contains_key(&key) {
            return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
        }
        let mut batch = WriteBatch::new();
        batch.delete(&key);
        self.store.write_batch(&batch);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.store.contains_key(&self.path_key(path)))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        if self.exists(path).map_err(|err| match err {
            OpenReadError::FileDoesNotExist(path) => OpenWriteError::FileAlreadyExists(path),
            OpenReadError::IoError { io_error, filepath } => {
                OpenWriteError::IoError { io_error, filepath }
            }
            OpenReadError::IncompatibleIndex(_) => OpenWriteError::IoError {
                io_error: Arc::new(io::Error::other("incompatible index")),
                filepath: path.to_path_buf(),
            },
        })? {
            return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
        }
        self.put_file(path, &[])
            .map_err(|err| OpenWriteError::wrap_io_error(err, path.to_path_buf()))?;
        Ok(std::io::BufWriter::new(Box::new(KvDirectoryWriter {
            directory: self.clone(),
            path: path.to_path_buf(),
            data: Cursor::new(Vec::new()),
        })))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.store
            .get_raw(&self.path_key(path))
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.put_file(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watchers.subscribe(watch_callback))
    }
}

impl Write for KvDirectoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.directory.put_file(&self.path, self.data.get_ref())
    }
}

impl TerminatingWrite for KvDirectoryWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

fn path_to_key(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::kv_store::KvStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    fn kv_tantivy_directory_covers_read_write_delete_watch_and_error_edges() {
        let directory = KvTantivyDirectory::new(test_store(), 7, "idx");
        assert!(format!("{directory:?}").contains("idx"));
        assert_eq!(
            path_to_key(Path::new("segment/meta.json")),
            "segment/meta.json"
        );
        assert!(!directory.exists(Path::new("missing")).unwrap());
        assert!(matches!(
            directory.open_read(Path::new("missing")),
            Err(OpenReadError::FileDoesNotExist(_))
        ));
        assert!(matches!(
            directory.atomic_read(Path::new("missing")),
            Err(OpenReadError::FileDoesNotExist(_))
        ));
        assert!(matches!(
            directory.delete(Path::new("missing")),
            Err(DeleteError::FileDoesNotExist(_))
        ));

        let watch_count = Arc::new(AtomicUsize::new(0));
        let watched = watch_count.clone();
        let _handle = directory
            .watch(WatchCallback::new(move || {
                watched.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();

        directory
            .atomic_write(Path::new("meta.json"), br#"{"generation":1}"#)
            .unwrap();
        for _ in 0..100 {
            if watch_count.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(watch_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            directory.atomic_read(Path::new("meta.json")).unwrap(),
            br#"{"generation":1}"#.to_vec()
        );
        assert!(directory.exists(Path::new("meta.json")).unwrap());
        assert!(directory.get_file_handle(Path::new("meta.json")).is_ok());

        assert!(matches!(
            directory.open_write(Path::new("meta.json")),
            Err(OpenWriteError::FileAlreadyExists(_))
        ));

        {
            let mut writer = directory.open_write(Path::new("segment.bin")).unwrap();
            writer.write_all(b"abc").unwrap();
            writer.flush().unwrap();
            writer.terminate().unwrap();
        }
        assert_eq!(
            directory.atomic_read(Path::new("segment.bin")).unwrap(),
            b"abc".to_vec()
        );
        directory.delete(Path::new("segment.bin")).unwrap();
        assert!(!directory.exists(Path::new("segment.bin")).unwrap());
    }
}
