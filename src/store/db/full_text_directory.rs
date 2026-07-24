use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    io::{Cursor, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use common::types::write_batch::WriteBatch;
use tantivy::{
    HasLen,
    directory::{
        AntiCallToken, Directory, FileHandle, FileSlice, OwnedBytes, TerminatingWrite,
        WatchCallback, WatchCallbackList, WatchHandle, WritePtr,
        error::{DeleteError, OpenReadError, OpenWriteError},
    },
};

use super::FULLTEXT_FILE_NAMESPACE;
use crate::store::{
    TABLE_LOCAL_INTERNAL_PREFIX,
    kv_store::{CompareCondition, KvStore},
};

const FULLTEXT_FILE_CHUNK_BYTES: usize = 1024 * 1024;
const MANIFEST_BYTES: usize = 16;

#[derive(Clone)]
pub struct KvTantivyDirectory {
    store: KvStore,
    db_index: u16,
    index: String,
    watchers: Arc<WatchCallbackList>,
    writes: Arc<Mutex<()>>,
    reservations: Arc<Mutex<HashSet<PathBuf>>>,
    chunk_leases: Arc<Mutex<HashMap<Vec<u8>, Weak<KvChunkLease>>>>,
}

struct KvDirectoryWriter {
    directory: KvTantivyDirectory,
    path: PathBuf,
    data: Cursor<Vec<u8>>,
    dirty: bool,
    ever_persisted: bool,
}

struct KvChunkFileHandle {
    store: KvStore,
    chunk_prefix: Vec<u8>,
    len: usize,
    _lease: Arc<KvChunkLease>,
}

struct KvChunkLease {
    store: KvStore,
    chunk_prefix: Vec<u8>,
    retired: AtomicBool,
}

impl fmt::Debug for KvChunkFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvChunkFileHandle")
            .field("len", &self.len)
            .finish()
    }
}

impl KvTantivyDirectory {
    pub fn new(store: KvStore, db_index: u16, index: &str) -> Self {
        Self {
            store,
            db_index,
            index: index.to_string(),
            watchers: Arc::new(WatchCallbackList::default()),
            writes: Arc::new(Mutex::new(())),
            reservations: Arc::new(Mutex::new(HashSet::new())),
            chunk_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn file_prefix(&self) -> Vec<u8> {
        let mut key = TABLE_LOCAL_INTERNAL_PREFIX.to_vec();
        key.extend_from_slice(&FULLTEXT_FILE_NAMESPACE);
        key.extend_from_slice(self.index.as_bytes());
        key.push(0x00);
        key
    }

    fn manifest_key(&self, path: &Path) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(b"m\0");
        key.extend_from_slice(path_to_key(path).as_bytes());
        key
    }

    fn reservation_key(&self, path: &Path) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(b"r\0");
        key.extend_from_slice(path_to_key(path).as_bytes());
        key
    }

    fn total_key(&self) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(b"t");
        key
    }

    fn version_key(&self) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(b"v");
        key
    }

    fn chunk_prefix(&self, path: &Path, version: u64) -> Vec<u8> {
        let mut key = self.file_prefix();
        key.extend_from_slice(b"c\0");
        key.extend_from_slice(path_to_key(path).as_bytes());
        key.push(0x00);
        key.extend_from_slice(&version.to_be_bytes());
        key
    }

    fn manifest(&self, path: &Path) -> Option<(u64, usize)> {
        decode_manifest(&self.store.get_raw(&self.manifest_key(path))?)
    }

    fn total_bytes(&self) -> usize {
        self.store
            .get_raw(&self.total_key())
            .and_then(|raw| raw.try_into().ok())
            .map(u64::from_be_bytes)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0)
    }

    pub fn storage_bytes(store: &KvStore, db_index: u16, index: &str) -> usize {
        Self::new(store.clone(), db_index, index).total_bytes()
    }

    fn put_file(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let _guard = self
            .writes
            .lock()
            .map_err(|_| io::Error::other("fulltext directory write lock poisoned"))?;
        let previous = self.manifest(path);
        let previous_len = previous.map(|(_, len)| len).unwrap_or(0);
        let version = self
            .store
            .get_raw(&self.version_key())
            .and_then(|raw| raw.try_into().ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0)
            .saturating_add(1);
        let total = self
            .total_bytes()
            .saturating_sub(previous_len)
            .saturating_add(data.len());
        for (chunk_index, chunk) in data.chunks(FULLTEXT_FILE_CHUNK_BYTES).enumerate() {
            let mut key = self.chunk_prefix(path, version);
            key.extend_from_slice(&(chunk_index as u32).to_be_bytes());
            self.store.blob_put_raw(&key, chunk);
        }
        let mut batch = WriteBatch::new();
        batch.put(
            &self.manifest_key(path),
            &encode_manifest(version, data.len()),
        );
        batch.put(&self.total_key(), &(total as u64).to_be_bytes());
        batch.put(&self.version_key(), &version.to_be_bytes());
        batch.delete(&self.reservation_key(path));
        self.store.write_batch(&batch);
        if let Some((previous_version, _)) = previous {
            self.retire_chunks(path, previous_version);
        }
        if let Ok(mut reservations) = self.reservations.lock() {
            reservations.remove(path);
        }
        if path == Path::new("meta.json") {
            drop(self.watchers.broadcast());
        }
        Ok(())
    }

    fn release_reservation(&self, path: &Path) {
        self.store.delete_key(&self.reservation_key(path));
        if let Ok(mut reservations) = self.reservations.lock() {
            reservations.remove(path);
        }
    }

    fn chunk_lease(&self, chunk_prefix: Vec<u8>) -> io::Result<Arc<KvChunkLease>> {
        let mut leases = self
            .chunk_leases
            .lock()
            .map_err(|_| io::Error::other("fulltext directory chunk lease lock poisoned"))?;
        if let Some(lease) = leases.get(&chunk_prefix).and_then(Weak::upgrade) {
            return Ok(lease);
        }
        let lease = Arc::new(KvChunkLease {
            store: self.store.clone(),
            chunk_prefix: chunk_prefix.clone(),
            retired: AtomicBool::new(false),
        });
        leases.insert(chunk_prefix, Arc::downgrade(&lease));
        Ok(lease)
    }

    fn retire_chunks(&self, path: &Path, version: u64) {
        let chunk_prefix = self.chunk_prefix(path, version);
        let lease = self
            .chunk_leases
            .lock()
            .ok()
            .and_then(|mut leases| leases.remove(&chunk_prefix))
            .and_then(|lease| lease.upgrade());
        if let Some(lease) = lease {
            lease.retired.store(true, Ordering::Release);
        } else {
            delete_chunk_prefix(&self.store, &chunk_prefix);
        }
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

impl FileHandle for KvChunkFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.start > range.end || range.end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fulltext file read outside manifest bounds",
            ));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::new(Vec::new()));
        }
        let first_chunk = range.start / FULLTEXT_FILE_CHUNK_BYTES;
        let last_chunk = (range.end - 1) / FULLTEXT_FILE_CHUNK_BYTES;
        let mut bytes = Vec::with_capacity(range.len());
        for chunk_index in first_chunk..=last_chunk {
            let mut key = self.chunk_prefix.clone();
            key.extend_from_slice(&(chunk_index as u32).to_be_bytes());
            let chunk = self.store.get_raw(&key).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing fulltext file chunk")
            })?;
            let chunk_start = chunk_index * FULLTEXT_FILE_CHUNK_BYTES;
            let start = range.start.saturating_sub(chunk_start).min(chunk.len());
            let end = range.end.saturating_sub(chunk_start).min(chunk.len());
            if start > end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid fulltext file chunk manifest",
                ));
            }
            bytes.extend_from_slice(&chunk[start..end]);
        }
        if bytes.len() != range.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short fulltext file chunk read",
            ));
        }
        Ok(OwnedBytes::new(bytes))
    }
}

impl HasLen for KvChunkFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for KvChunkLease {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            delete_chunk_prefix(&self.store, &self.chunk_prefix);
        }
    }
}

impl Directory for KvTantivyDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let _guard = self.writes.lock().map_err(|_| {
            OpenReadError::wrap_io_error(
                io::Error::other("fulltext directory write lock poisoned"),
                path.to_path_buf(),
            )
        })?;
        let (version, len) = self
            .manifest(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let chunk_prefix = self.chunk_prefix(path, version);
        let lease = self
            .chunk_lease(chunk_prefix.clone())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        Ok(Arc::new(KvChunkFileHandle {
            store: self.store.clone(),
            chunk_prefix,
            len,
            _lease: lease,
        }))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        Ok(FileSlice::new(self.get_file_handle(path)?))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let _guard = self.writes.lock().map_err(|_| DeleteError::IoError {
            io_error: Arc::new(io::Error::other("fulltext directory write lock poisoned")),
            filepath: path.to_path_buf(),
        })?;
        let Some((version, len)) = self.manifest(path) else {
            return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
        };
        let mut batch = WriteBatch::new();
        batch.delete(&self.manifest_key(path));
        batch.delete(&self.reservation_key(path));
        batch.put(
            &self.total_key(),
            &(self.total_bytes().saturating_sub(len) as u64).to_be_bytes(),
        );
        // Chunks are immutable and remain readable by already-open FileHandles;
        // their final lease reclaims the retired version.
        self.store.write_batch(&batch);
        self.retire_chunks(path, version);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.store.contains_key(&self.manifest_key(path)))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let manifest_key = self.manifest_key(path);
        let reservation_key = self.reservation_key(path);
        let mut reservations = self.reservations.lock().map_err(|_| {
            OpenWriteError::wrap_io_error(
                io::Error::other("fulltext directory reservation lock poisoned"),
                path.to_path_buf(),
            )
        })?;
        if reservations.contains(path) {
            return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
        }
        let mut reservation = WriteBatch::new();
        reservation.put(&reservation_key, b"\x01");
        if self
            .store
            .compare_and_write_batch(&[CompareCondition::absent(&manifest_key)], &reservation)
            .is_err()
        {
            return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
        }
        reservations.insert(path.to_path_buf());
        drop(reservations);
        Ok(std::io::BufWriter::new(Box::new(KvDirectoryWriter {
            directory: self.clone(),
            path: path.to_path_buf(),
            data: Cursor::new(Vec::new()),
            dirty: true,
            ever_persisted: false,
        })))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let handle = self.get_file_handle(path)?;
        handle
            .read_bytes(0..handle.len())
            .map(|bytes| bytes.as_slice().to_vec())
            .map_err(|err| OpenReadError::wrap_io_error(err, path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.put_file(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.store
            .sync_wal()
            .map_err(|err| io::Error::other(err.to_string()))
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watchers.subscribe(watch_callback))
    }
}

impl KvDirectoryWriter {
    fn persist(&mut self) -> io::Result<()> {
        if self.dirty {
            self.directory.put_file(&self.path, self.data.get_ref())?;
            self.dirty = false;
            self.ever_persisted = true;
        }
        Ok(())
    }
}

impl Write for KvDirectoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.write_all(buf)?;
        self.dirty = true;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.persist()
    }
}

impl TerminatingWrite for KvDirectoryWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.persist()
    }
}

impl Drop for KvDirectoryWriter {
    fn drop(&mut self) {
        if !self.ever_persisted {
            self.directory.release_reservation(&self.path);
        }
    }
}

fn encode_manifest(version: u64, len: usize) -> [u8; MANIFEST_BYTES] {
    let mut raw = [0u8; MANIFEST_BYTES];
    raw[..8].copy_from_slice(&version.to_be_bytes());
    raw[8..].copy_from_slice(&(len as u64).to_be_bytes());
    raw
}

fn decode_manifest(raw: &[u8]) -> Option<(u64, usize)> {
    if raw.len() != MANIFEST_BYTES {
        return None;
    }
    let version = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let len = usize::try_from(u64::from_be_bytes(raw[8..].try_into().ok()?)).ok()?;
    Some((version, len))
}

fn path_to_key(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn delete_chunk_prefix(store: &KvStore, prefix: &[u8]) {
    let mut batch = WriteBatch::new();
    if let Some(end) = exclusive_upper_bound(prefix) {
        batch.delete_range(prefix, &end);
    } else {
        for (key, _) in store.scan_prefix_raw(prefix) {
            batch.delete(&key);
        }
    }
    store.write_batch(&batch);
}

fn exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}
