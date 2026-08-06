use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashSet;
use kv_engine::function::{CompactionFilter, CompactionFilterDecision};

use super::*;

const VERSION_OWNER_VALUE_VERSION: u8 = 1;
const VERSION_REFRESH_BATCH_LIMIT: usize = 1024;

#[derive(Debug, Clone)]
struct VersionOwner {
    version: u64,
    type_tag: u8,
    key: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct VersionCompactionTracker {
    live_versions: DashSet<u64>,
    owner_scan_next: DashMap<u16, u64>,
    ready: AtomicBool,
}

impl VersionCompactionTracker {
    pub(crate) fn register_live(&self, version: u64) {
        if version != 0 {
            self.live_versions.insert(version);
        }
    }

    pub(crate) fn retire(&self, version: u64) {
        if version != 0 {
            self.live_versions.remove(&version);
        }
    }

    pub(crate) fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub(crate) fn owner_scan_start(&self, db_index: u16, prefix: &[u8]) -> Vec<u8> {
        let Some(next_version) = self.owner_scan_next.get(&db_index).map(|entry| *entry) else {
            return prefix.to_vec();
        };
        let mut start = Vec::with_capacity(prefix.len() + 8);
        start.extend_from_slice(prefix);
        start.extend_from_slice(&next_version.to_be_bytes());
        start
    }

    pub(crate) fn finish_owner_scan(
        &self,
        db_index: u16,
        last_version: Option<u64>,
        exhausted: bool,
    ) {
        if exhausted {
            self.owner_scan_next.remove(&db_index);
        } else if let Some(next_version) = last_version.and_then(|version| version.checked_add(1)) {
            self.owner_scan_next.insert(db_index, next_version);
        } else {
            self.owner_scan_next.remove(&db_index);
        }
    }

    fn should_remove(&self, key: &[u8]) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            return false;
        }
        version_from_compaction_key(key)
            .is_some_and(|version| !self.live_versions.contains(&version))
    }
}

pub(crate) struct OnedisVersionCompactionFilter {
    tracker: Arc<VersionCompactionTracker>,
}

impl OnedisVersionCompactionFilter {
    pub(crate) fn new(tracker: Arc<VersionCompactionTracker>) -> Self {
        Self { tracker }
    }
}

impl CompactionFilter for OnedisVersionCompactionFilter {
    fn name(&self) -> &str {
        "onedis_version_compaction"
    }

    fn filter(
        &self,
        key: &[u8],
        _value: &[u8],
        _seq: u64,
        _write_type: WriteType,
    ) -> CompactionFilterDecision {
        if self.tracker.should_remove(key) {
            CompactionFilterDecision::Remove
        } else {
            CompactionFilterDecision::Keep
        }
    }
}

fn version_from_compaction_key(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(crate::store::TABLE_LOCAL_INTERNAL_PREFIX)?;
    if let Some(suffix) = rest.strip_prefix(&VERSION_OWNER_NAMESPACE) {
        return (suffix.len() == 8)
            .then(|| u64::from_be_bytes(suffix.try_into().expect("checked version length")));
    }
    let namespace = rest.get(..3)?;
    if !is_versioned_namespace(namespace) {
        return None;
    }
    let encoded_owner = rest.get(3..)?;
    let delimiter = if encoded_owner.first() == Some(&0xff) {
        let owner_len = u64::from_be_bytes(encoded_owner.get(1..9)?.try_into().ok()?) as usize;
        let delimiter = 9usize.checked_add(owner_len)?;
        (encoded_owner.get(delimiter) == Some(&0)).then_some(delimiter)?
    } else {
        encoded_owner.iter().position(|byte| *byte == 0)?
    };
    let version_start = delimiter.checked_add(1)?;
    let version = encoded_owner.get(version_start..version_start + 8)?;
    Some(u64::from_be_bytes(version.try_into().ok()?))
}

fn is_versioned_namespace(namespace: &[u8]) -> bool {
    [
        HASH_FIELD_NAMESPACE,
        HASH_FIELD_EXPIRE_NAMESPACE,
        LIST_ITEM_NAMESPACE,
        SET_MEMBER_NAMESPACE,
        ZSET_MEMBER_NAMESPACE,
        ZSET_RANK_NAMESPACE,
        STREAM_ENTRY_NAMESPACE,
        STREAM_GROUP_NAMESPACE,
        STREAM_PEL_NAMESPACE,
        STREAM_CONSUMER_NAMESPACE,
        JSON_NODE_NAMESPACE,
        VECTOR_META_NAMESPACE,
        VECTOR_DOC_NAMESPACE,
        VECTOR_TAG_NAMESPACE,
        VECTOR_NUMERIC_NAMESPACE,
        VECTOR_SEGMENT_NAMESPACE,
        VECTOR_GRAPH_NAMESPACE,
    ]
    .iter()
    .any(|candidate| namespace == candidate)
}

pub(crate) fn version_owner_prefix(db_index: u16) -> Vec<u8> {
    let mut key = internal_prefix(db_index);
    key.extend_from_slice(&VERSION_OWNER_NAMESPACE);
    key
}

fn version_owner_key(db_index: u16, version: u64) -> Vec<u8> {
    let mut key = version_owner_prefix(db_index);
    key.extend_from_slice(&version.to_be_bytes());
    key
}

fn put_version_owner_to_batch(
    batch: &mut WriteBatch,
    db_index: u16,
    key: &[u8],
    version: u64,
    type_tag: u8,
) {
    if version == 0 || type_tag == TYPE_STRING {
        return;
    }
    let mut raw = Vec::with_capacity(2 + key.len());
    raw.push(VERSION_OWNER_VALUE_VERSION);
    raw.push(type_tag);
    raw.extend_from_slice(key);
    batch.put(&version_owner_key(db_index, version), &raw);
}

fn decode_version_owner(prefix: &[u8], raw_key: &[u8], raw_value: &[u8]) -> Option<VersionOwner> {
    let suffix = raw_key.strip_prefix(prefix)?;
    if suffix.len() != 8 || raw_value.len() < 2 || raw_value[0] != VERSION_OWNER_VALUE_VERSION {
        return None;
    }
    Some(VersionOwner {
        version: u64::from_be_bytes(suffix.try_into().ok()?),
        type_tag: raw_value[1],
        key: raw_value[2..].to_vec(),
    })
}

impl Db {
    pub(in crate::store::db) fn batch_with_version_owner_markers(
        &self,
        batch: &WriteBatch,
    ) -> Option<WriteBatch> {
        let mut augmented: Option<WriteBatch> = None;
        for (write_type, raw_key, raw_value) in batch.iter() {
            if !matches!(
                write_type,
                WriteType::Put | WriteType::PutBlobMedium | WriteType::PutBlobExternal
            ) {
                continue;
            }
            let Some(header) = decode_meta_header(raw_value) else {
                continue;
            };
            if header.type_tag == TYPE_STRING || header.version == 0 {
                continue;
            }
            let Some(logical_key) =
                logical_main_key_from_raw_key(self.key_layout, self.db_index, raw_key)
            else {
                continue;
            };
            self.store.register_live_version(header.version);
            let owner_batch = augmented.get_or_insert_with(|| batch.clone());
            put_version_owner_to_batch(
                owner_batch,
                self.db_index,
                &logical_key,
                header.version,
                header.type_tag,
            );
        }
        augmented
    }

    pub(crate) fn refresh_retired_versions_for_compaction(&self) -> usize {
        let prefix = version_owner_prefix(self.db_index);
        let start = self.store.version_owner_scan_start(self.db_index, &prefix);
        let owners = self.store.scan_range_raw_limited(
            &start,
            prefix_exclusive_upper_bound(&prefix),
            VERSION_REFRESH_BATCH_LIMIT,
        );
        let exhausted = owners.len() < VERSION_REFRESH_BATCH_LIMIT;
        let last_version = owners
            .last()
            .and_then(|(key, _)| key.strip_prefix(prefix.as_slice()))
            .filter(|suffix| suffix.len() == 8)
            .map(|suffix| u64::from_be_bytes(suffix.try_into().expect("checked version length")));
        let retired = self.refresh_retired_versions(&prefix, owners);
        self.store
            .finish_version_owner_scan(self.db_index, last_version, exhausted);
        retired
    }

    #[cfg(test)]
    pub(in crate::store::db) fn refresh_retired_versions_once(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let prefix = version_owner_prefix(self.db_index);
        let owners = self.store.scan_range_raw_limited(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            limit,
        );
        self.refresh_retired_versions(&prefix, owners)
    }

    fn refresh_retired_versions(&self, prefix: &[u8], owners: Vec<(Vec<u8>, Vec<u8>)>) -> usize {
        let mut retired = 0usize;
        for (owner_key, owner_raw) in owners {
            let Some(owner) = decode_version_owner(prefix, &owner_key, &owner_raw) else {
                continue;
            };
            if self.version_owner_is_current(&owner) {
                self.store.register_live_version(owner.version);
            } else {
                self.store.retire_version(owner.version);
                retired += 1;
            }
        }
        retired
    }

    fn version_owner_is_current(&self, owner: &VersionOwner) -> bool {
        let Some(raw) = self
            .store
            .get_raw(&main_key_bytes(self.db_index, &owner.key))
        else {
            return false;
        };
        let Some(header) = decode_meta_header(&raw) else {
            return false;
        };
        header.type_tag == owner.type_tag && header.version == owner.version
    }
}
