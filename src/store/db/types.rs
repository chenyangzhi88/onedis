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
    Json(String), // 使用字符串存储JSON数据
}

#[derive(Clone, Encode, Decode)]
pub(in crate::store::db) enum JsonNode {
    Scalar(String),
    Object(Vec<String>),
    Array(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpireCondition {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetCondition {
    Always,
    Nx,
    Xx,
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

#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    Set { old_value: Option<Vec<u8>> },
    NotSet,
}

#[derive(Clone, Copy)]
pub(in crate::store::db) struct CounterCacheEntry {
    pub(in crate::store::db) value: i64,
    pub(in crate::store::db) expire_ms: u64,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
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
    enabled: AtomicBool,
    clock: AtomicU64,
    key_versions: DashMap<Vec<u8>, u64>,
    db_versions: DashMap<u16, u64>,
}

impl KeyMutationTracker {
    pub(in crate::store::db) fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    pub(in crate::store::db) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(in crate::store::db) fn bump_key(&self, key: Vec<u8>) {
        let version = self.clock.fetch_add(1, Ordering::AcqRel) + 1;
        self.key_versions.insert(key, version);
    }

    pub(in crate::store::db) fn bump_db(&self, db_index: u16) {
        let version = self.clock.fetch_add(1, Ordering::AcqRel) + 1;
        self.db_versions.insert(db_index, version);
    }

    pub fn key_version(&self, key: &[u8]) -> u64 {
        self.key_versions.get(key).map(|entry| *entry).unwrap_or(0)
    }

    pub fn db_version(&self, db_index: u16) -> u64 {
        self.db_versions
            .get(&db_index)
            .map(|entry| *entry)
            .unwrap_or(0)
    }
}

#[derive(Default)]
pub(in crate::store::db) struct PendingMutations {
    pub(in crate::store::db) keys: Vec<Vec<u8>>,
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
}
