use super::*;

#[derive(Clone, Encode, Decode)]
pub enum Structure {
    String(String),
    Hash(HashMap<String, String>),
    SortedSet(BTreeMap<String, f64>),
    VectorCollection(Vector),
    Set(HashSet<String>),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
    Json(String), // Indexed JSON layout marker; document nodes are stored separately.
}

#[derive(Clone, PartialEq, Eq, Encode, Decode)]
pub(in crate::store::db) enum JsonNode {
    Scalar(String),
    /// Generation changes only when direct children are added or removed. Existing child updates
    /// stay independent, while ancestor replacement can detect concurrent structural changes.
    Object(u64),
    /// Stable physical element ids in logical array order. Deleting one element only changes
    /// this directory and removes that element subtree; later elements keep their storage keys.
    Array(Vec<u64>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpireCondition {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
    XxGt,
    XxLt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetCondition {
    Always,
    Nx,
    Xx,
}

#[derive(Clone, Copy)]
pub(crate) struct DbKeyRef<'a> {
    pub(crate) db_index: u16,
    pub(crate) key: &'a str,
}

impl<'a> DbKeyRef<'a> {
    pub(crate) fn new(db_index: u16, key: &'a str) -> Self {
        Self { db_index, key }
    }
}

pub(in crate::store::db) struct StructureCopyContext<'a> {
    pub(in crate::store::db) source_store: &'a KvStore,
    pub(in crate::store::db) target_store: &'a KvStore,
    pub(in crate::store::db) source: DbKeyRef<'a>,
    pub(in crate::store::db) target: DbKeyRef<'a>,
    pub(in crate::store::db) raw: &'a [u8],
    pub(in crate::store::db) version_counter: &'a VersionCounter,
}

impl<'a> StructureCopyContext<'a> {
    pub(in crate::store::db) fn new(
        source_store: &'a KvStore,
        target_store: &'a KvStore,
        source: DbKeyRef<'a>,
        target: DbKeyRef<'a>,
        raw: &'a [u8],
        version_counter: &'a VersionCounter,
    ) -> Self {
        Self {
            source_store,
            target_store,
            source,
            target,
            raw,
            version_counter,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetExpiration {
    Clear,
    KeepTtl,
    At(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringExpireUpdate {
    Persist,
    RelativeMs(u64),
    AbsoluteMs(u64),
}

#[derive(Clone, Copy)]
pub(crate) enum StringBatchMutation<'a> {
    Append {
        key: &'a str,
        value: &'a [u8],
    },
    GetSet {
        key: &'a str,
        value: &'a [u8],
    },
    GetDel {
        key: &'a str,
    },
    SetNx {
        key: &'a str,
        value: &'a [u8],
    },
    SetBit {
        key: &'a str,
        offset: usize,
        bit: u8,
    },
    SetRange {
        key: &'a str,
        offset: usize,
        value: &'a [u8],
    },
    Psetex {
        key: &'a str,
        ttl_ms: u64,
        value: &'a [u8],
    },
}

impl StringBatchMutation<'_> {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Append { key, .. }
            | Self::GetDel { key }
            | Self::GetSet { key, .. }
            | Self::SetNx { key, .. }
            | Self::SetBit { key, .. }
            | Self::SetRange { key, .. }
            | Self::Psetex { key, .. } => key,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StringBatchReply {
    Bulk(Option<Vec<u8>>),
    Integer(i64),
    Ok,
}

#[derive(Clone, Copy)]
pub(crate) enum KeyExpirationBatchMutation<'a> {
    Expire { key: &'a str, ttl_ms: u64 },
    Persist { key: &'a str },
}

#[derive(Clone)]
pub(crate) enum SetBatchMutation<'a> {
    Add { key: &'a str, members: Vec<&'a str> },
    Remove { key: &'a str, members: Vec<&'a str> },
}

/// One HSET command in an already ordered RESP pipeline.
///
/// The server keeps command boundaries so the database layer can return the
/// added-field count for every command while still committing independent
/// keys in one storage batch.
pub(crate) struct HashSetBatchMutation<'a> {
    pub(crate) key: &'a str,
    pub(crate) fields: Vec<(&'a str, &'a [u8])>,
}

pub(crate) type StreamAddBatchCommand<'a> = (&'a str, Option<StreamId>, Vec<(&'a str, &'a str)>);

impl SetBatchMutation<'_> {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Add { key, .. } | Self::Remove { key, .. } => key,
        }
    }

    pub(crate) fn members(&self) -> &[&str] {
        match self {
            Self::Add { members, .. } | Self::Remove { members, .. } => members,
        }
    }
}

impl KeyExpirationBatchMutation<'_> {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Expire { key, .. } | Self::Persist { key } => key,
        }
    }
}

/// Redis reserves the upper two bits of its 48-bit hash-field expiry timestamp.
pub const HASH_FIELD_MAX_EXPIRE_MS: u64 = 0x3fff_ffff_ffff;

#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    Set { old_value: Option<Vec<u8>> },
    NotSet,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZsetAddOptions {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub increment: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ZsetAddOutcome {
    pub added: usize,
    pub changed: usize,
    pub score: Option<f64>,
    pub applied: bool,
}

pub(crate) const COUNTER_CACHE_MAX_ENTRIES: usize = 1 << 16;

#[derive(Default)]
pub(crate) struct CounterCommitProgress {
    pub(crate) committed_sequence: u64,
    pub(crate) completed: BTreeSet<u64>,
    pub(crate) failure: Option<String>,
}

#[derive(Default)]
pub(crate) struct CounterCommitState {
    pub(crate) progress: Mutex<CounterCommitProgress>,
    pub(crate) notify: tokio::sync::Notify,
}

impl CounterCommitState {
    pub(crate) fn complete(&self, sequence: u64) {
        let mut progress = self
            .progress
            .lock()
            .expect("counter commit progress mutex poisoned");
        progress.completed.insert(sequence);
        while let Some(next) = progress.committed_sequence.checked_add(1) {
            if !progress.completed.remove(&next) {
                break;
            }
            progress.committed_sequence += 1;
        }
        drop(progress);
        self.notify.notify_waiters();
    }

    pub(crate) fn fail(&self, error: String) {
        let mut progress = self
            .progress
            .lock()
            .expect("counter commit progress mutex poisoned");
        if progress.failure.is_none() {
            progress.failure = Some(error);
        }
        drop(progress);
        self.notify.notify_waiters();
    }

    pub(crate) fn failure(&self) -> Option<String> {
        self.progress
            .lock()
            .expect("counter commit progress mutex poisoned")
            .failure
            .clone()
    }

    pub(crate) async fn wait_for(&self, sequence: u64) -> Result<(), Error> {
        loop {
            let notified = self.notify.notified();
            {
                let progress = self
                    .progress
                    .lock()
                    .expect("counter commit progress mutex poisoned");
                if let Some(error) = &progress.failure {
                    return Err(Error::msg(error.clone()));
                }
                if progress.committed_sequence >= sequence {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub(in crate::store::db) struct CounterCacheEntry {
    pub(in crate::store::db) value: i64,
    pub(in crate::store::db) next_sequence: u64,
    pub(in crate::store::db) commit_state: Arc<CounterCommitState>,
}

#[derive(Default)]
pub(crate) struct CounterCacheRuntime {
    pub(in crate::store::db) entries: DashMap<(u16, Vec<u8>), CounterCacheEntry>,
    /// This flag is intentionally monotonic. Resetting it during an eviction can race with an
    /// insertion for another key and make a later structural write skip invalidation.
    pub(in crate::store::db) ever_populated: AtomicBool,
    pub(in crate::store::db) hash_entries: DashMap<(u16, Vec<u8>), HashCounterCacheEntry>,
    pub(in crate::store::db) hash_routes: DashMap<(u16, Vec<u8>), Vec<u8>>,
    pub(in crate::store::db) hash_lengths: DashMap<(u16, Vec<u8>), HashLenCacheEntry>,
    pub(in crate::store::db) hash_key_epochs: DashMap<(u16, Vec<u8>), u64>,
    pub(in crate::store::db) hash_ever_populated: AtomicBool,
    pub(in crate::store::db) zset_lengths: DashMap<(u16, Vec<u8>), ZsetLenCacheEntry>,
    pub(in crate::store::db) zset_key_epochs: DashMap<(u16, Vec<u8>), u64>,
    pub(in crate::store::db) zset_db_epochs: DashMap<u16, u64>,
    pub(in crate::store::db) zset_ever_populated: AtomicBool,
    pub(in crate::store::db) zset_increment_queues:
        DashMap<(u16, Vec<u8>, Vec<u8>), Arc<ZsetIncrementMergeQueue>>,
    pub(in crate::store::db) zset_add_queues:
        DashMap<(u16, Vec<u8>, Vec<u8>), Arc<ZsetAddMergeQueue>>,
    pub(in crate::store::db) list_push_queues: DashMap<(u16, Vec<u8>), Arc<ListPushMergeQueue>>,
    pub(in crate::store::db) list_pop_queues: DashMap<(u16, Vec<u8>), Arc<ListPopMergeQueue>>,
    pub(in crate::store::db) stream_add_queues: DashMap<(u16, Vec<u8>), Arc<StreamAddMergeQueue>>,
    pub(in crate::store::db) bitop_queues: DashMap<(u16, Vec<u8>), Arc<BitopMergeQueue>>,
}

pub(in crate::store::db) struct ZsetIncrementMergeRequest {
    pub(in crate::store::db) increment: f64,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<f64, Error>>,
}

pub(in crate::store::db) struct ZsetAddMergeRequest {
    pub(in crate::store::db) score: f64,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<usize, Error>>,
}

#[derive(Default)]
pub(in crate::store::db) struct ZsetAddMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<ZsetAddMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

#[derive(Default)]
pub(in crate::store::db) struct ZsetIncrementMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<ZsetIncrementMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

pub(in crate::store::db) struct ListPushMergeRequest {
    pub(in crate::store::db) left: bool,
    pub(in crate::store::db) values: Vec<Vec<u8>>,
    pub(in crate::store::db) only_if_exists: bool,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<usize, Error>>,
}

#[derive(Default)]
pub(in crate::store::db) struct ListPushMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<ListPushMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

pub(in crate::store::db) struct ListPopMergeRequest {
    pub(in crate::store::db) left: bool,
    pub(in crate::store::db) count: usize,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<Vec<Vec<u8>>, Error>>,
}

#[derive(Default)]
pub(in crate::store::db) struct ListPopMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<ListPopMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

pub(in crate::store::db) struct StreamAddMergeRequest {
    pub(in crate::store::db) requested_id: Option<StreamId>,
    pub(in crate::store::db) fields: Vec<(String, String)>,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<StreamId, Error>>,
}

#[derive(Default)]
pub(in crate::store::db) struct StreamAddMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<StreamAddMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

pub(in crate::store::db) struct BitopMergeRequest {
    pub(in crate::store::db) operation: String,
    pub(in crate::store::db) sources: Vec<String>,
    pub(in crate::store::db) reply: tokio::sync::oneshot::Sender<Result<usize, Error>>,
}

#[derive(Default)]
pub(in crate::store::db) struct BitopMergeQueue {
    pub(in crate::store::db) pending: Mutex<VecDeque<BitopMergeRequest>>,
    pub(in crate::store::db) running: AtomicBool,
}

#[derive(Clone)]
pub(in crate::store::db) struct HashCounterCacheEntry {
    pub(in crate::store::db) value: i64,
    pub(in crate::store::db) next_sequence: u64,
    pub(in crate::store::db) key_epoch: u64,
    pub(in crate::store::db) commit_state: Arc<CounterCommitState>,
}

#[derive(Clone, Copy)]
pub(in crate::store::db) struct HashLenCacheEntry {
    pub(in crate::store::db) len: usize,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) key_epoch: u64,
}

#[derive(Clone, Copy)]
pub(in crate::store::db) struct ZsetLenCacheEntry {
    pub(in crate::store::db) len: usize,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) key_epoch: u64,
    pub(in crate::store::db) db_epoch: u64,
}

impl CounterCacheRuntime {
    pub(crate) fn invalidate_key(&self, db_index: u16, key: &[u8]) {
        if self.ever_populated.load(Ordering::Acquire) {
            self.entries.remove(&(db_index, key.to_vec()));
        }
    }

    pub(crate) fn invalidate_db(&self, db_index: u16) {
        if self.ever_populated.load(Ordering::Acquire) {
            self.entries
                .retain(|(cached_db, _), _| *cached_db != db_index);
        }
        if self.hash_ever_populated.load(Ordering::Acquire) {
            self.hash_entries
                .retain(|(cached_db, _), _| *cached_db != db_index);
            self.hash_routes
                .retain(|(cached_db, _), _| *cached_db != db_index);
            self.hash_lengths
                .retain(|(cached_db, _), _| *cached_db != db_index);
            self.hash_key_epochs
                .retain(|(cached_db, _), _| *cached_db != db_index);
        }
        if self.zset_ever_populated.load(Ordering::Acquire) {
            self.zset_lengths
                .retain(|(cached_db, _), _| *cached_db != db_index);
            self.zset_key_epochs
                .retain(|(cached_db, _), _| *cached_db != db_index);
            self.zset_db_epochs
                .entry(db_index)
                .and_modify(|epoch| *epoch = epoch.wrapping_add(1))
                .or_insert(1);
        }
    }

    pub(crate) fn evict_if_full(&self) {
        if self.entries.len() >= COUNTER_CACHE_MAX_ENTRIES {
            self.entries.clear();
        }
    }

    pub(crate) fn hash_key_epoch(&self, db_index: u16, key: &[u8]) -> u64 {
        self.hash_key_epochs
            .get(&(db_index, key.to_vec()))
            .map(|epoch| *epoch)
            .unwrap_or(0)
    }

    pub(crate) fn invalidate_hash_key(&self, db_index: u16, key: &[u8]) {
        if !self.hash_ever_populated.load(Ordering::Acquire) {
            return;
        }
        self.hash_key_epochs
            .entry((db_index, key.to_vec()))
            .and_modify(|epoch| *epoch = epoch.wrapping_add(1))
            .or_insert(1);
    }

    pub(crate) fn invalidate_hash_field(&self, db_index: u16, raw_field_key: &[u8]) {
        if self.hash_ever_populated.load(Ordering::Acquire) {
            self.hash_entries
                .remove(&(db_index, raw_field_key.to_vec()));
        }
    }

    pub(crate) fn evict_hash_if_full(&self) {
        if self.hash_entries.len() >= COUNTER_CACHE_MAX_ENTRIES {
            self.hash_entries.clear();
            self.hash_routes.clear();
            self.hash_lengths.clear();
        }
    }

    pub(crate) fn zset_key_epoch(&self, db_index: u16, key: &[u8]) -> u64 {
        self.zset_key_epochs
            .get(&(db_index, key.to_vec()))
            .map(|epoch| *epoch)
            .unwrap_or(0)
    }

    pub(crate) fn zset_db_epoch(&self, db_index: u16) -> u64 {
        self.zset_db_epochs
            .get(&db_index)
            .map(|epoch| *epoch)
            .unwrap_or(0)
    }

    pub(crate) fn invalidate_zset_key(&self, db_index: u16, key: &[u8]) {
        if !self.zset_ever_populated.load(Ordering::Acquire) {
            return;
        }
        self.zset_lengths.remove(&(db_index, key.to_vec()));
        self.zset_key_epochs
            .entry((db_index, key.to_vec()))
            .and_modify(|epoch| *epoch = epoch.wrapping_add(1))
            .or_insert(1);
    }

    pub(crate) fn evict_zset_if_full(&self) {
        if self.zset_lengths.len() >= COUNTER_CACHE_MAX_ENTRIES {
            self.zset_lengths.clear();
        }
    }
}

#[derive(Clone, Encode, Decode, Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroupInfo {
    pub name: String,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: String,
    pub entries_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumerInfo {
    pub name: String,
    pub pending: usize,
    pub idle_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPendingSummary {
    pub total: usize,
    pub smallest_id: Option<String>,
    pub greatest_id: Option<String>,
    pub consumers: Vec<(String, usize)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPendingEntry {
    pub id: String,
    pub consumer: String,
    pub idle_ms: u64,
    pub deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamClaimedEntries {
    pub next_id: String,
    pub entries: Vec<StreamEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TtlObservabilitySnapshot {
    pub expired_keys: u64,
    pub stale_entries_skipped: u64,
    pub sweep_cycles: u64,
    pub expires: usize,
    pub avg_ttl_millis: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FullTextObservabilitySnapshot {
    pub creating: u64,
    pub backfilling: u64,
    pub ready: u64,
    pub dirty: u64,
    pub rebuilding: u64,
    pub dropping: u64,
    pub outbox_pending: u64,
    pub backfill_pending: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamObservabilitySnapshot {
    pub groups: u64,
    pub pending_entries: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VectorObservabilitySnapshot {
    pub indexes: u64,
    pub segments: u64,
    pub pending_segments: u64,
    pub hnsw_nodes: u64,
    pub hnsw_deleted_nodes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

#[derive(Clone, Copy)]
pub struct ZsetScoreWindow<'a> {
    pub key: &'a str,
    pub min: f64,
    pub min_inclusive: bool,
    pub max: f64,
    pub max_inclusive: bool,
    pub reverse: bool,
    pub limit: Option<(i64, i64)>,
}

#[derive(Clone, Copy)]
pub struct StreamPendingRange<'a> {
    pub key: &'a str,
    pub group: &'a str,
    pub start: StreamId,
    pub end: StreamId,
    pub count: usize,
    pub consumer: Option<&'a str>,
    pub min_idle_ms: Option<u64>,
}

impl StreamId {
    pub fn parse(text: &str) -> Option<Self> {
        parse_stream_id(text)
    }

    pub fn to_redis_id(self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamReadStart {
    Id(StreamId),
    Latest,
}

#[derive(Clone, Encode, Decode)]
pub struct Vector {
    pub dimension: usize,
    pub vectors: HashMap<String, Vec<f32>>,
    pub norms: HashMap<String, f32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct ListMeta {
    pub(in crate::store::db) expire_ms: u64,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) head: i64,
    pub(in crate::store::db) tail: i64,
}

#[derive(Default)]
pub struct KeyMutationTracker {
    clock: AtomicU64,
    key_versions: DashMap<(u16, Vec<u8>), WatchedKeyMutation>,
    db_versions: DashMap<u16, u64>,
    key_waiters: DashMap<(u16, Vec<u8>), Vec<Weak<Notify>>>,
}

pub(crate) struct KeyMutationWaiter {
    tracker: Arc<KeyMutationTracker>,
    keys: Vec<(u16, Vec<u8>)>,
    signal: Arc<Notify>,
}

impl KeyMutationWaiter {
    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.signal.notified()
    }
}

impl Drop for KeyMutationWaiter {
    fn drop(&mut self) {
        for key in &self.keys {
            if let Entry::Occupied(mut entry) = self.tracker.key_waiters.entry(key.clone()) {
                entry.get_mut().retain(|waiter| {
                    waiter
                        .upgrade()
                        .is_some_and(|signal| !Arc::ptr_eq(&signal, &self.signal))
                });
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct WatchedKeyMutation {
    version: u64,
    watchers: usize,
}

impl KeyMutationTracker {
    pub(in crate::store::db) fn register_waiter(
        self: &Arc<Self>,
        keys: Vec<(u16, Vec<u8>)>,
    ) -> KeyMutationWaiter {
        let mut unique_keys = keys;
        unique_keys.sort();
        unique_keys.dedup();
        let signal = Arc::new(Notify::new());
        for key in &unique_keys {
            self.key_waiters
                .entry(key.clone())
                .or_default()
                .push(Arc::downgrade(&signal));
        }
        KeyMutationWaiter {
            tracker: Arc::clone(self),
            keys: unique_keys,
            signal,
        }
    }

    fn notify_key_waiters(&self, db_index: u16, key: &[u8]) {
        if let Entry::Occupied(mut entry) = self.key_waiters.entry((db_index, key.to_vec())) {
            entry.get_mut().retain(|waiter| {
                let Some(signal) = waiter.upgrade() else {
                    return false;
                };
                signal.notify_one();
                true
            });
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    fn notify_db_waiters(&self, db_index: u16) {
        let keys = self
            .key_waiters
            .iter()
            .filter(|entry| entry.key().0 == db_index)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for (_, key) in keys {
            self.notify_key_waiters(db_index, &key);
        }
    }

    pub(crate) fn notify_all_waiters(&self) {
        for entry in &self.key_waiters {
            for waiter in entry.value() {
                if let Some(signal) = waiter.upgrade() {
                    signal.notify_one();
                }
            }
        }
    }

    pub(in crate::store::db) fn register_key(&self, db_index: u16, key: Vec<u8>) {
        match self.key_versions.entry((db_index, key)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().watchers += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(WatchedKeyMutation {
                    version: self.clock.load(Ordering::Acquire),
                    watchers: 1,
                });
            }
        }
    }

    pub(in crate::store::db) fn unregister_key(&self, db_index: u16, key: &[u8]) {
        let map_key = (db_index, key.to_vec());
        if let Entry::Occupied(mut entry) = self.key_versions.entry(map_key) {
            if entry.get().watchers > 1 {
                entry.get_mut().watchers -= 1;
            } else {
                entry.remove();
            }
        }
    }

    pub(in crate::store::db) fn bump_key(&self, db_index: u16, key: Vec<u8>) {
        self.notify_key_waiters(db_index, &key);
        if let Some(mut entry) = self.key_versions.get_mut(&(db_index, key)) {
            let version = self.clock.fetch_add(1, Ordering::AcqRel) + 1;
            entry.version = version;
        }
    }

    pub(in crate::store::db) fn has_observers(&self) -> bool {
        !self.key_versions.is_empty() || !self.key_waiters.is_empty()
    }

    pub(in crate::store::db) fn bump_db(&self, db_index: u16) {
        self.notify_db_waiters(db_index);
        let version = self.clock.fetch_add(1, Ordering::AcqRel) + 1;
        self.db_versions.insert(db_index, version);
    }

    pub fn key_version(&self, db_index: u16, key: &[u8]) -> u64 {
        self.key_versions
            .get(&(db_index, key.to_vec()))
            .map(|entry| entry.version)
            .unwrap_or(0)
    }

    pub fn db_version(&self, db_index: u16) -> u64 {
        self.db_versions
            .get(&db_index)
            .map(|entry| *entry)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(in crate::store::db) fn tracked_key_count(&self) -> usize {
        self.key_versions.len()
    }
}

#[derive(Default)]
pub(in crate::store::db) struct PendingMutations {
    pub(in crate::store::db) keys: Vec<(u16, Vec<u8>)>,
    pub(in crate::store::db) dbs: Vec<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct StreamMeta {
    pub(in crate::store::db) expire_ms: u64,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) last_id: StreamId,
    pub(in crate::store::db) length: u64,
    pub(in crate::store::db) entries_added: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamReadGroupStart {
    New,
    Id(StreamId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZsetAggregate {
    Sum,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct StreamGroupState {
    pub(in crate::store::db) last_delivered_id: StreamId,
    pub(in crate::store::db) entries_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct StreamPelState {
    pub(in crate::store::db) consumer: String,
    pub(in crate::store::db) last_delivery_ms: u64,
    pub(in crate::store::db) deliveries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct StreamConsumerState {
    pub(in crate::store::db) last_seen_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::store::db) struct SetMeta {
    pub(in crate::store::db) expire_ms: u64,
    pub(in crate::store::db) version: u64,
    pub(in crate::store::db) len: usize,
    pub(in crate::store::db) packed: bool,
}
