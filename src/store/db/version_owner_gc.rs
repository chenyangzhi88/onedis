use super::*;

const VERSION_OWNER_VALUE_VERSION: u8 = 1;
const RETIRED_VERSION_GC_BATCH_LIMIT: usize = 128;

#[derive(Debug, Clone)]
pub(in crate::store::db) struct VersionOwner {
    version: u64,
    type_tag: u8,
    key: Vec<u8>,
}

pub(in crate::store::db) fn version_owner_prefix(db_index: u16) -> Vec<u8> {
    let mut key = internal_prefix(db_index);
    key.extend_from_slice(&VERSION_OWNER_NAMESPACE);
    key
}

pub(in crate::store::db) fn version_owner_key(db_index: u16, version: u64) -> Vec<u8> {
    let mut key = version_owner_prefix(db_index);
    key.extend_from_slice(&version.to_be_bytes());
    key
}

pub(in crate::store::db) fn put_version_owner_to_batch(
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
    if suffix.len() != 8 || raw_value.len() < 2 {
        return None;
    }
    if raw_value[0] != VERSION_OWNER_VALUE_VERSION {
        return None;
    }
    let version = u64::from_be_bytes(suffix.try_into().ok()?);
    Some(VersionOwner {
        version,
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

    pub(crate) fn retired_version_gc_tick(&self) -> usize {
        self.retired_version_gc_once(RETIRED_VERSION_GC_BATCH_LIMIT)
    }

    pub(in crate::store::db) fn retired_version_gc_once(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let prefix = version_owner_prefix(self.db_index);
        let owners = self.store.scan_range_raw_limited(
            &prefix,
            prefix_exclusive_upper_bound(&prefix),
            limit,
        );
        let mut reclaimed = 0usize;
        for (owner_key, owner_raw) in owners {
            let Some(owner) = decode_version_owner(&prefix, &owner_key, &owner_raw) else {
                continue;
            };
            if !self.version_owner_is_retired(&owner) {
                continue;
            }
            let mut batch = WriteBatch::new();
            delete_sub_keys_to_batch_bytes(
                &mut batch,
                self.db_index,
                &owner.key,
                owner.version,
                owner.type_tag,
            );
            delete_sub_keys_by_scan_to_batch_bytes(
                &self.store,
                &mut batch,
                self.db_index,
                &owner.key,
                owner.version,
                owner.type_tag,
            );
            batch.delete(&owner_key);
            if let Ok(key) = std::str::from_utf8(&owner.key) {
                match owner.type_tag {
                    TYPE_HASH => {
                        if let Err(err) =
                            self.fulltext_enqueue_hash_delete_to_batch(&mut batch, key)
                        {
                            log::error!(
                                "failed to enqueue fulltext delete for retired {key}: {err}"
                            );
                        }
                    }
                    TYPE_JSON => {
                        if let Err(err) =
                            self.fulltext_enqueue_json_delete_to_batch(&mut batch, key)
                        {
                            log::error!(
                                "failed to enqueue fulltext JSON delete for retired {key}: {err}"
                            );
                        }
                    }
                    _ => {}
                }
            }
            self.write_batch_if_not_empty(&batch);
            reclaimed += 1;
        }
        reclaimed
    }

    fn version_owner_is_retired(&self, owner: &VersionOwner) -> bool {
        let Some(raw) = self
            .store
            .get_raw(&main_key_bytes(self.db_index, &owner.key))
        else {
            return true;
        };
        let Some(header) = decode_meta_header(&raw) else {
            return true;
        };
        header.type_tag != owner.type_tag || header.version != owner.version
    }
}
