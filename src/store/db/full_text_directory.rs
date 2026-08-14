use std::{
    collections::{HashMap, HashSet, VecDeque},
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
        AntiCallToken, Directory, FileHandle, FileSlice, INDEX_WRITER_LOCK, OwnedBytes,
        TerminatingWrite, WatchCallback, WatchCallbackList, WatchHandle, WritePtr,
        error::{DeleteError, OpenReadError, OpenWriteError},
    },
};

use super::FULLTEXT_FILE_NAMESPACE;
use crate::store::{
    TABLE_LOCAL_INTERNAL_PREFIX,
    kv_store::{CompareCondition, KvStore},
};

const FULLTEXT_FILE_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_FULLTEXT_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
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
    hot: Option<Arc<Mutex<HotDirectoryState>>>,
    block_cache: Arc<Mutex<KvBlockCache>>,
}

#[derive(Default)]
struct HotDirectoryState {
    files: HashMap<PathBuf, Arc<[u8]>>,
    deleted: HashSet<PathBuf>,
    dirty: bool,
}

struct KvBlockCache {
    entries: HashMap<Vec<u8>, Arc<[u8]>>,
    order: VecDeque<Vec<u8>>,
    bytes: usize,
    max_bytes: usize,
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
    block_cache: Arc<Mutex<KvBlockCache>>,
}

struct HotFileHandle {
    data: Arc<[u8]>,
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

impl fmt::Debug for HotFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotFileHandle")
            .field("len", &self.data.len())
            .finish()
    }
}

impl KvBlockCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Arc<[u8]>> {
        let value = self.entries.get(key)?.clone();
        self.order.retain(|existing| existing.as_slice() != key);
        self.order.push_back(key.to_vec());
        Some(value)
    }

    fn insert(&mut self, key: Vec<u8>, value: Arc<[u8]>) {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return;
        }
        if let Some(previous) = self.entries.insert(key.clone(), value.clone()) {
            self.bytes = self.bytes.saturating_sub(previous.len());
            self.order.retain(|existing| existing != &key);
        }
        self.bytes = self.bytes.saturating_add(value.len());
        self.order.push_back(key);
        while self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
    }

    fn remove_prefix(&mut self, prefix: &[u8]) {
        let keys = self
            .entries
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(removed) = self.entries.remove(&key) {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
        self.order.retain(|key| !key.as_slice().starts_with(prefix));
    }
}

impl KvTantivyDirectory {
    pub fn new(store: KvStore, db_index: u16, index: &str) -> Self {
        Self::with_mode(
            store,
            db_index,
            index,
            false,
            DEFAULT_FULLTEXT_BLOCK_CACHE_BYTES,
        )
    }

    /// Creates a two-tier directory. Tantivy commits are first published in the
    /// process-local overlay and become searchable immediately. `checkpoint`
    /// durably installs the same immutable files in the KV-backed directory.
    pub fn new_tiered(
        store: KvStore,
        db_index: u16,
        index: &str,
        block_cache_bytes: usize,
    ) -> Self {
        Self::with_mode(store, db_index, index, true, block_cache_bytes)
    }

    fn with_mode(
        store: KvStore,
        db_index: u16,
        index: &str,
        tiered: bool,
        block_cache_bytes: usize,
    ) -> Self {
        Self {
            store,
            db_index,
            index: index.to_string(),
            watchers: Arc::new(WatchCallbackList::default()),
            writes: Arc::new(Mutex::new(())),
            reservations: Arc::new(Mutex::new(HashSet::new())),
            chunk_leases: Arc::new(Mutex::new(HashMap::new())),
            hot: tiered.then(|| Arc::new(Mutex::new(HotDirectoryState::default()))),
            block_cache: Arc::new(Mutex::new(KvBlockCache::new(block_cache_bytes))),
        }
    }

    pub fn has_hot_changes(&self) -> bool {
        self.hot
            .as_ref()
            .and_then(|hot| hot.lock().ok().map(|hot| hot.dirty))
            .unwrap_or(false)
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

    /// Removes a writer lock left behind by an unclean process exit.
    ///
    /// The caller must hold the index lifecycle write lock and must have confirmed that no
    /// in-process runtime owns an `IndexWriter` for this storage generation.
    pub(crate) fn remove_stale_writer_lock(&self) -> io::Result<bool> {
        let path = INDEX_WRITER_LOCK.filepath.as_path();
        if self.manifest(path).is_none() {
            // A crash between reservation and manifest publication can leave this bookkeeping key.
            if self.store.contains_key(&self.reservation_key(path)) {
                self.release_reservation(path);
            }
            return Ok(false);
        }
        self.delete(path).map_err(|error| {
            io::Error::other(format!("failed to remove stale writer lock: {error}"))
        })?;
        Ok(true)
    }

    fn put_file(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let _guard = self
            .writes
            .lock()
            .map_err(|_| io::Error::other("fulltext directory write lock poisoned"))?;
        if let Some(hot) = &self.hot {
            let mut hot = hot
                .lock()
                .map_err(|_| io::Error::other("fulltext hot directory lock poisoned"))?;
            hot.files
                .insert(path.to_path_buf(), Arc::<[u8]>::from(data));
            hot.deleted.remove(path);
            hot.dirty = true;
            drop(hot);
            if let Ok(mut reservations) = self.reservations.lock() {
                reservations.remove(path);
            }
            if path == Path::new("meta.json") {
                drop(self.watchers.broadcast());
            }
            return Ok(());
        }
        self.put_file_durable(path, data, true)
    }

    fn put_file_durable(&self, path: &Path, data: &[u8], notify: bool) -> io::Result<()> {
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
        batch
            .put(
                &self.manifest_key(path),
                &encode_manifest(version, data.len()),
            )
            .map_err(|error| io::Error::other(error.to_string()))?;
        batch
            .put(&self.total_key(), &(total as u64).to_be_bytes())
            .map_err(|error| io::Error::other(error.to_string()))?;
        batch
            .put(&self.version_key(), &version.to_be_bytes())
            .map_err(|error| io::Error::other(error.to_string()))?;
        batch
            .delete(&self.reservation_key(path))
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.store.write_batch(&batch);
        if let Some((previous_version, _)) = previous {
            self.retire_chunks(path, previous_version);
        }
        if let Ok(mut reservations) = self.reservations.lock() {
            reservations.remove(path);
        }
        if notify && path == Path::new("meta.json") {
            drop(self.watchers.broadcast());
        }
        Ok(())
    }

    /// Persists one coherent hot Tantivy generation. Immutable data files are
    /// installed first and `meta.json` is installed last, so a crash can leave
    /// only unreachable files, never a durable manifest referring to missing
    /// segment data.
    pub fn checkpoint(&self) -> io::Result<bool> {
        let Some(hot) = &self.hot else {
            self.store
                .sync_wal()
                .map_err(|err| io::Error::other(err.to_string()))?;
            return Ok(false);
        };
        let _guard = self
            .writes
            .lock()
            .map_err(|_| io::Error::other("fulltext directory write lock poisoned"))?;
        let mut hot = hot
            .lock()
            .map_err(|_| io::Error::other("fulltext hot directory lock poisoned"))?;
        if !hot.dirty {
            return Ok(false);
        }

        let meta_path = Path::new("meta.json");
        let lock_paths = [
            Path::new(".tantivy-writer.lock"),
            Path::new(".tantivy-meta.lock"),
        ];
        let files = hot
            .files
            .iter()
            .filter(|(path, _)| {
                path.as_path() != meta_path && !lock_paths.contains(&path.as_path())
            })
            .map(|(path, data)| (path.clone(), data.clone()))
            .collect::<Vec<_>>();
        for (path, data) in files {
            self.put_file_durable(&path, &data, false)?;
        }
        if let Some(meta) = hot.files.get(meta_path).cloned() {
            self.put_file_durable(meta_path, &meta, false)?;
        }

        let deleted = hot
            .deleted
            .iter()
            .filter(|path| !lock_paths.contains(&path.as_path()))
            .cloned()
            .collect::<Vec<_>>();
        for path in deleted {
            self.delete_durable_if_present(&path)?;
        }
        self.store
            .sync_wal()
            .map_err(|err| io::Error::other(err.to_string()))?;
        hot.files
            .retain(|path, _| lock_paths.contains(&path.as_path()));
        hot.deleted.clear();
        hot.dirty = false;
        Ok(true)
    }

    fn delete_durable_if_present(&self, path: &Path) -> io::Result<()> {
        let Some((version, len)) = self.manifest(path) else {
            return Ok(());
        };
        let mut batch = WriteBatch::new();
        batch
            .delete(&self.manifest_key(path))
            .map_err(|error| io::Error::other(error.to_string()))?;
        batch
            .delete(&self.reservation_key(path))
            .map_err(|error| io::Error::other(error.to_string()))?;
        batch
            .put(
                &self.total_key(),
                &(self.total_bytes().saturating_sub(len) as u64).to_be_bytes(),
            )
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.store.write_batch(&batch);
        self.retire_chunks(path, version);
        Ok(())
    }

    fn release_reservation(&self, path: &Path) {
        if self.hot.is_none() {
            self.store.delete_key(&self.reservation_key(path));
        }
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
        if let Ok(mut cache) = self.block_cache.lock() {
            cache.remove_prefix(&chunk_prefix);
        }
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
        let keys = (first_chunk..=last_chunk)
            .map(|chunk_index| {
                let mut key = self.chunk_prefix.clone();
                key.extend_from_slice(&(chunk_index as u32).to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        let mut chunks = if let Ok(mut cache) = self.block_cache.lock() {
            keys.iter().map(|key| cache.get(key)).collect::<Vec<_>>()
        } else {
            vec![None; keys.len()]
        };
        let missing = chunks
            .iter()
            .enumerate()
            .filter(|(_, chunk)| chunk.is_none())
            .map(|(index, _)| (index, keys[index].clone()))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let missing_keys = missing
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let loaded = self.store.multi_get_raw(&missing_keys);
            let mut cache = self.block_cache.lock().ok();
            for ((chunk_offset, key), value) in missing.into_iter().zip(loaded) {
                let chunk = Arc::<[u8]>::from(value.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "missing fulltext file chunk")
                })?);
                if let Some(cache) = cache.as_mut() {
                    cache.insert(key, chunk.clone());
                }
                chunks[chunk_offset] = Some(chunk);
            }
        }
        let mut bytes = Vec::with_capacity(range.len());
        for (chunk_offset, chunk) in chunks.into_iter().enumerate() {
            let chunk_index = first_chunk + chunk_offset;
            let chunk = chunk.ok_or_else(|| {
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

impl FileHandle for HotFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.start > range.end || range.end > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fulltext hot file read outside bounds",
            ));
        }
        Ok(OwnedBytes::new(self.data.clone()).slice(range))
    }
}

impl HasLen for KvChunkFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl HasLen for HotFileHandle {
    fn len(&self) -> usize {
        self.data.len()
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
        if let Some(hot) = &self.hot {
            let hot = hot.lock().map_err(|_| {
                OpenReadError::wrap_io_error(
                    io::Error::other("fulltext hot directory lock poisoned"),
                    path.to_path_buf(),
                )
            })?;
            if let Some(data) = hot.files.get(path) {
                return Ok(Arc::new(HotFileHandle { data: data.clone() }));
            }
            if hot.deleted.contains(path) {
                return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
            }
        }
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
            block_cache: self.block_cache.clone(),
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
        if let Some(hot) = &self.hot {
            let mut hot = hot.lock().map_err(|_| DeleteError::IoError {
                io_error: Arc::new(io::Error::other("fulltext hot directory lock poisoned")),
                filepath: path.to_path_buf(),
            })?;
            let hot_exists = hot.files.remove(path).is_some();
            let durable_exists = !hot.deleted.contains(path) && self.manifest(path).is_some();
            if !hot_exists && !durable_exists {
                return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
            }
            hot.deleted.insert(path.to_path_buf());
            hot.dirty = true;
            return Ok(());
        }
        let Some((version, len)) = self.manifest(path) else {
            return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
        };
        let mut batch = WriteBatch::new();
        batch
            .delete(&self.manifest_key(path))
            .map_err(|error| DeleteError::IoError {
                io_error: Arc::new(io::Error::other(error.to_string())),
                filepath: path.to_path_buf(),
            })?;
        batch
            .delete(&self.reservation_key(path))
            .map_err(|error| DeleteError::IoError {
                io_error: Arc::new(io::Error::other(error.to_string())),
                filepath: path.to_path_buf(),
            })?;
        batch
            .put(
                &self.total_key(),
                &(self.total_bytes().saturating_sub(len) as u64).to_be_bytes(),
            )
            .map_err(|error| DeleteError::IoError {
                io_error: Arc::new(io::Error::other(error.to_string())),
                filepath: path.to_path_buf(),
            })?;
        // Chunks are immutable and remain readable by already-open FileHandles;
        // their final lease reclaims the retired version.
        self.store.write_batch(&batch);
        self.retire_chunks(path, version);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if let Some(hot) = &self.hot {
            let hot = hot.lock().map_err(|_| {
                OpenReadError::wrap_io_error(
                    io::Error::other("fulltext hot directory lock poisoned"),
                    path.to_path_buf(),
                )
            })?;
            if hot.files.contains_key(path) {
                return Ok(true);
            }
            if hot.deleted.contains(path) {
                return Ok(false);
            }
        }
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
        if self.hot.is_some() {
            if self.exists(path).map_err(|error| match error {
                OpenReadError::FileDoesNotExist(path) => OpenWriteError::FileAlreadyExists(path),
                OpenReadError::IoError { io_error, filepath } => {
                    OpenWriteError::IoError { io_error, filepath }
                }
                OpenReadError::IncompatibleIndex(_) => OpenWriteError::wrap_io_error(
                    io::Error::other("incompatible fulltext index"),
                    path.to_path_buf(),
                ),
            })? {
                return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
            }
            reservations.insert(path.to_path_buf());
            drop(reservations);
            return Ok(std::io::BufWriter::new(Box::new(KvDirectoryWriter {
                directory: self.clone(),
                path: path.to_path_buf(),
                data: Cursor::new(Vec::new()),
                dirty: true,
                ever_persisted: false,
            })));
        }
        let mut reservation = WriteBatch::new();
        reservation
            .put(&reservation_key, b"\x01")
            .map_err(|error| {
                OpenWriteError::wrap_io_error(
                    io::Error::other(error.to_string()),
                    path.to_path_buf(),
                )
            })?;
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
        if self.hot.is_some() {
            return Ok(());
        }
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
        if let Err(error) = batch.delete_range(prefix, &end) {
            log::warn!("failed to plan retired fulltext chunk cleanup: {error}");
            return;
        }
    } else {
        for (key, _) in store.scan_prefix_raw(prefix) {
            if let Err(error) = batch.delete(&key) {
                log::warn!("failed to plan retired fulltext chunk cleanup: {error}");
                return;
            }
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
