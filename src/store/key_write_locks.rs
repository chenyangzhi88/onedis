use std::sync::Arc;

pub(crate) const KEY_WRITE_LOCK_SHARDS: usize = 1 << 16;

/// Shared structural barrier for one logical-key shard.
///
/// Existing callers use `lock()` for an exclusive structural mutation. Field-local HSET writes
/// use `read()` so independent fields under one hash can proceed concurrently while operations
/// such as RENAME, DEL, type replacement, and TTL sweeping still exclude them.
pub(crate) struct KeyWriteLock {
    inner: Arc<tokio::sync::RwLock<()>>,
}

impl KeyWriteLock {
    fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    pub(crate) async fn lock(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.inner.write().await
    }

    pub(crate) async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.inner.read().await
    }

    pub(crate) async fn read_owned(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.inner.clone().read_owned().await
    }

    pub(crate) async fn lock_owned(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.inner.clone().write_owned().await
    }

    pub(crate) fn blocking_lock(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.inner.blocking_write()
    }
}

pub(crate) type KeyWriteLocks = Arc<[KeyWriteLock]>;

pub(crate) fn new_key_write_locks() -> KeyWriteLocks {
    (0..KEY_WRITE_LOCK_SHARDS)
        .map(|_| KeyWriteLock::new())
        .collect::<Vec<_>>()
        .into()
}

pub(crate) fn key_write_lock_shard(db_index: u16, key: &str) -> usize {
    key_write_lock_shard_bytes(db_index, key.as_bytes())
}

pub(crate) fn key_write_lock_shard_bytes(db_index: u16, key: &[u8]) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in db_index
        .to_be_bytes()
        .into_iter()
        .chain(key.iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as usize & (KEY_WRITE_LOCK_SHARDS - 1)
}

pub(crate) fn hash_field_write_lock_shard(db_index: u16, key: &str, field: &str) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in db_index
        .to_be_bytes()
        .into_iter()
        .chain(key.len().to_be_bytes())
        .chain(key.bytes())
        .chain(field.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as usize & (KEY_WRITE_LOCK_SHARDS - 1)
}

pub(crate) fn unique_hash_field_write_lock_shards<'a>(
    db_index: u16,
    key: &str,
    fields: impl IntoIterator<Item = &'a str>,
) -> Vec<usize> {
    let mut shards = fields
        .into_iter()
        .map(|field| hash_field_write_lock_shard(db_index, key, field))
        .collect::<Vec<_>>();
    shards.sort_unstable();
    shards.dedup();
    shards
}

pub(crate) fn unique_key_write_lock_shards<'a>(
    db_index: u16,
    keys: impl IntoIterator<Item = &'a [u8]>,
) -> Vec<usize> {
    let mut shards = keys
        .into_iter()
        .map(|key| key_write_lock_shard_bytes(db_index, key))
        .collect::<Vec<_>>();
    shards.sort_unstable();
    shards.dedup();
    shards
}
