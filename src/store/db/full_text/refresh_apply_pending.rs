use super::*;
use crate::store::db::{
    decode_hash_meta_checked, decode_packed_hash, decode_u64_be, hash_field_expire_key,
    hash_field_key, now_ms,
};

enum FullTextPendingSourceAction {
    Upsert {
        fields: Vec<(String, String)>,
        expires_at_ms: u64,
        json_root: Option<serde_json::Value>,
    },
    Delete,
    Noop,
}

struct FullTextPendingSourceMutation {
    seq: u64,
    key: String,
    action: FullTextPendingSourceAction,
}

enum FullTextPreparedRefreshAction {
    Upsert {
        document: FullTextPreparedDocument,
        json_root: Option<serde_json::Value>,
    },
    Delete,
    Noop,
}

struct FullTextPreparedRefreshMutation {
    outbox_seq: Option<u64>,
    key: String,
    action: FullTextPreparedRefreshAction,
}

struct FullTextHashSourceSnapshot {
    fields: Vec<(String, String)>,
    expires_at_ms: u64,
    source_exists: bool,
}

impl Db {
    /// Loads the hash fields needed by an index with two KV multi-gets instead
    /// of one prefix scan per document. Index filters may reference fields that
    /// are not in the schema, so those indexes keep the complete-scan path.
    fn fulltext_hash_sources_for_refresh(
        &self,
        meta: &FullTextIndexMeta,
        keys: &[String],
    ) -> Result<Vec<FullTextHashSourceSnapshot>, Error> {
        if meta.index_options.filter.is_some() {
            return keys
                .iter()
                .map(|key| {
                    let fields = self.hash_get_all(key)?;
                    Ok(FullTextHashSourceSnapshot {
                        source_exists: !fields.is_empty(),
                        fields,
                        expires_at_ms: self.fulltext_source_expire_ms(key),
                    })
                })
                .collect();
        }

        let mut field_names = meta
            .schema
            .iter()
            .map(|field| field.name.as_str())
            .chain(meta.index_options.language_field.as_deref())
            .chain(meta.index_options.score_field.as_deref())
            .chain(meta.index_options.payload_field.as_deref())
            .collect::<Vec<_>>();
        field_names.sort_unstable();
        field_names.dedup();

        let meta_keys = keys.iter().map(|key| self.mk(key)).collect::<Vec<_>>();
        let raw_metas = self.store.multi_get_raw(&meta_keys);
        let now = now_ms();
        let hash_metas = raw_metas
            .iter()
            .map(|raw| {
                let Some(raw) = raw.as_ref() else {
                    return Ok(None);
                };
                let header = decode_meta_header(&raw)
                    .ok_or_else(|| Error::msg("Failed to decode hash metadata"))?;
                if header.type_tag != TYPE_HASH || (header.expire_ms > 0 && now >= header.expire_ms)
                {
                    return Ok(None);
                }
                Ok(Some(decode_hash_meta_checked(&raw)?))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let mut lookup_keys = Vec::with_capacity(
            keys.len()
                .saturating_mul(field_names.len())
                .saturating_mul(2),
        );
        let mut lookups = Vec::with_capacity(keys.len().saturating_mul(field_names.len()));
        let mut packed_values = Vec::new();
        for (key_offset, (key, hash_meta)) in keys.iter().zip(&hash_metas).enumerate() {
            let Some(hash_meta) = hash_meta else {
                continue;
            };
            if hash_meta.packed {
                let packed = raw_metas[key_offset]
                    .as_deref()
                    .and_then(decode_packed_hash)
                    .ok_or_else(|| Error::msg("Failed to decode packed hash"))?;
                for field in &field_names {
                    if let Some(value) = packed.get(*field)
                        && let Ok(value) = String::from_utf8(value.clone())
                    {
                        packed_values.push((key_offset, (*field).to_string(), value));
                    }
                }
                continue;
            }
            for field in &field_names {
                let value_offset = lookup_keys.len();
                lookup_keys.push(hash_field_key(self.db_index, key, hash_meta.version, field));
                let expire_offset = hash_meta.may_have_field_ttl.then(|| {
                    let offset = lookup_keys.len();
                    lookup_keys.push(hash_field_expire_key(
                        self.db_index,
                        key,
                        hash_meta.version,
                        field,
                    ));
                    offset
                });
                lookups.push((key_offset, *field, value_offset, expire_offset));
            }
        }
        let values = self.store.multi_get_raw(&lookup_keys);
        let mut snapshots = hash_metas
            .iter()
            .map(|hash_meta| FullTextHashSourceSnapshot {
                fields: Vec::with_capacity(field_names.len()),
                expires_at_ms: hash_meta.map_or(0, |hash_meta| hash_meta.expire_ms),
                source_exists: hash_meta.is_some(),
            })
            .collect::<Vec<_>>();
        for (key_offset, field, value) in packed_values {
            snapshots[key_offset].fields.push((field, value));
        }
        for (key_offset, field, value_offset, expire_offset) in lookups {
            let expired = expire_offset
                .and_then(|offset| values.get(offset))
                .and_then(|raw| raw.as_deref())
                .and_then(decode_u64_be)
                .is_some_and(|expire_ms| expire_ms > 0 && now >= expire_ms);
            if expired {
                continue;
            }
            let Some(value) = values
                .get(value_offset)
                .and_then(|raw| raw.as_ref())
                .and_then(|raw| String::from_utf8(raw.clone()).ok())
            else {
                continue;
            };
            snapshots[key_offset]
                .fields
                .push((field.to_string(), value));
        }
        Ok(snapshots)
    }

    pub(super) fn fulltext_apply_pending(
        &self,
        index: &str,
        meta: &mut FullTextIndexMeta,
        expected_meta_raw: &[u8],
        runtime: &Arc<RwLock<FullTextRuntime>>,
        policy: &FullTextRefreshPolicy,
        deadline: Instant,
        durable_checkpoint: bool,
    ) -> Result<(), Error> {
        let mut changed = false;
        let mut indexed_docs = 0usize;
        let mut indexed_bytes = 0usize;
        let mut checkpoint_state = None;
        let mut prepared = Vec::new();
        let mut backfill_progress = None;
        if matches!(
            meta.state,
            FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
        ) && policy.max_docs > 0
            && policy.max_bytes > 0
            && Instant::now() < deadline
        {
            let cursor = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
                .published_backfill_cursor
                .clone();
            let (keys, has_more) =
                self.fulltext_source_keys_page(meta, cursor.as_deref(), policy.max_docs)?;
            let mut next_cursor = cursor;
            let mut finished = !has_more;
            let mut sources = Vec::with_capacity(keys.len());
            for key in keys {
                if Instant::now() >= deadline {
                    finished = false;
                    break;
                }
                let action = match meta.source_type {
                    FullTextSourceType::Hash => {
                        let fields = self.hash_get_all(&key)?;
                        if fields.is_empty() || !fulltext_index_filter_matches(meta, &fields)? {
                            FullTextPendingSourceAction::Delete
                        } else {
                            FullTextPendingSourceAction::Upsert {
                                fields,
                                expires_at_ms: self.fulltext_source_expire_ms(&key),
                                json_root: None,
                            }
                        }
                    }
                    FullTextSourceType::Json => {
                        if let Some(root) = self.fulltext_json_root(&key)? {
                            let fields = self.fulltext_json_fields_from_root(&root, meta)?;
                            if fulltext_index_filter_matches(meta, &fields)? {
                                FullTextPendingSourceAction::Upsert {
                                    fields,
                                    expires_at_ms: self.fulltext_source_expire_ms(&key),
                                    json_root: Some(root),
                                }
                            } else {
                                FullTextPendingSourceAction::Delete
                            }
                        } else {
                            FullTextPendingSourceAction::Delete
                        }
                    }
                };
                sources.push(FullTextPendingSourceMutation {
                    seq: 0,
                    key,
                    action,
                });
            }
            {
                let runtime = runtime
                    .read()
                    .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
                for source in sources {
                    let action = match source.action {
                        FullTextPendingSourceAction::Upsert {
                            fields,
                            expires_at_ms,
                            json_root,
                        } => {
                            let document = runtime.prepare_fields_document(
                                &source.key,
                                &fields,
                                expires_at_ms,
                            )?;
                            indexed_bytes = indexed_bytes.saturating_add(document.indexed_bytes);
                            FullTextPreparedRefreshAction::Upsert {
                                document,
                                json_root,
                            }
                        }
                        FullTextPendingSourceAction::Delete => {
                            FullTextPreparedRefreshAction::Delete
                        }
                        FullTextPendingSourceAction::Noop => FullTextPreparedRefreshAction::Noop,
                    };
                    indexed_docs = indexed_docs.saturating_add(1);
                    next_cursor = Some(source.key.clone());
                    prepared.push(FullTextPreparedRefreshMutation {
                        outbox_seq: None,
                        key: source.key,
                        action,
                    });
                    // Source reads stop at the deadline above. Finish analyzing the
                    // already-staged keys so every refresh makes bounded forward
                    // progress instead of repeatedly paying the reads and only
                    // publishing the first document after the deadline expires.
                    if indexed_bytes >= policy.max_bytes {
                        finished = false;
                        break;
                    }
                }
            }
            backfill_progress = Some((next_cursor, finished));
        }

        let published_outbox_seq = runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?
            .published_outbox_seq;
        let mut scanned_through = published_outbox_seq;
        let mut processed_all = true;
        let mut pending_sources = Vec::new();
        if indexed_docs < policy.max_docs
            && indexed_bytes < policy.max_bytes
            && Instant::now() < deadline
        {
            let prefix = fulltext_outbox_prefix(self.db_index, index);
            let start = published_outbox_seq
                .checked_add(1)
                .map(|seq| fulltext_outbox_key(self.db_index, index, seq));
            let remaining_docs = policy.max_docs.saturating_sub(indexed_docs);
            let entries = start
                .into_iter()
                .flat_map(|start| {
                    self.store.scan_range_raw_limited(
                        &start,
                        prefix_exclusive_upper_bound(&prefix),
                        remaining_docs.max(1),
                    )
                })
                .collect::<Vec<_>>();
            let mut latest_by_key = HashMap::<String, (u64, FullTextMutationRecord)>::new();
            for (outbox_key, raw) in entries {
                let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key)
                else {
                    continue;
                };
                let records = decode_fulltext_mutation_records(&raw)?;
                if !latest_by_key.is_empty()
                    && latest_by_key.len().saturating_add(records.len()) > remaining_docs
                {
                    processed_all = false;
                    break;
                }
                scanned_through = scanned_through.max(seq);
                for record in records {
                    if record.incarnation != meta.incarnation {
                        continue;
                    }
                    latest_by_key.insert(record.key.clone(), (seq, record));
                }
            }
            let mut pending = latest_by_key.into_values().collect::<Vec<_>>();
            pending.sort_by_key(|(seq, _)| *seq);
            let hash_snapshots = if matches!(meta.source_type, FullTextSourceType::Hash) {
                let missing = pending
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, record))| record.projection.is_none())
                    .map(|(offset, (_, record))| (offset, record.key.clone()))
                    .collect::<Vec<_>>();
                let loaded = self.fulltext_hash_sources_for_refresh(
                    meta,
                    &missing
                        .iter()
                        .map(|(_, key)| key.clone())
                        .collect::<Vec<_>>(),
                )?;
                let mut snapshots = std::iter::repeat_with(|| None)
                    .take(pending.len())
                    .collect::<Vec<_>>();
                for ((offset, _), snapshot) in missing.into_iter().zip(loaded) {
                    snapshots[offset] = Some(snapshot);
                }
                Some(snapshots)
            } else {
                None
            };
            let mut current_seq = None;
            let mut stop_after_seq = None;
            for (pending_offset, (seq, record)) in pending.into_iter().enumerate() {
                if current_seq != Some(seq) {
                    if stop_after_seq.is_some() || Instant::now() >= deadline {
                        processed_all = false;
                        break;
                    }
                    current_seq = Some(seq);
                }
                if stop_after_seq.is_some_and(|stop_seq| stop_seq != seq) {
                    processed_all = false;
                    break;
                }
                let action = match record.kind {
                    FullTextMutationKind::UpsertKey => {
                        if !matches!(meta.source_type, FullTextSourceType::Hash) {
                            FullTextPendingSourceAction::Noop
                        } else {
                            let (fields, expires_at_ms, source_exists) = if let Some(projection) =
                                record.projection
                            {
                                (projection.fields, projection.expires_at_ms, true)
                            } else {
                                let snapshot = hash_snapshots
                                    .as_ref()
                                    .expect("hash refresh snapshots were loaded")[pending_offset]
                                    .as_ref()
                                    .expect("missing hash refresh snapshot was loaded");
                                (
                                    snapshot.fields.clone(),
                                    snapshot.expires_at_ms,
                                    snapshot.source_exists,
                                )
                            };
                            // An empty projection still represents a live HASH: it can
                            // legitimately omit every indexed field and must remain in
                            // the index for `ismissing(@field)`. Only an empty source
                            // snapshot proves that the key disappeared.
                            if !source_exists || !fulltext_index_filter_matches(meta, &fields)? {
                                FullTextPendingSourceAction::Delete
                            } else {
                                FullTextPendingSourceAction::Upsert {
                                    fields,
                                    expires_at_ms,
                                    json_root: None,
                                }
                            }
                        }
                    }
                    FullTextMutationKind::UpsertJson => {
                        if !matches!(meta.source_type, FullTextSourceType::Json) {
                            FullTextPendingSourceAction::Noop
                        } else if let Some(root) = self.fulltext_json_root(&record.key)? {
                            let fields = self.fulltext_json_fields_from_root(&root, meta)?;
                            if fulltext_index_filter_matches(meta, &fields)? {
                                FullTextPendingSourceAction::Upsert {
                                    fields,
                                    expires_at_ms: self.fulltext_source_expire_ms(&record.key),
                                    json_root: Some(root),
                                }
                            } else {
                                FullTextPendingSourceAction::Delete
                            }
                        } else {
                            FullTextPendingSourceAction::Delete
                        }
                    }
                    FullTextMutationKind::DeleteKey => FullTextPendingSourceAction::Delete,
                };
                pending_sources.push(FullTextPendingSourceMutation {
                    seq,
                    key: record.key,
                    action,
                });
                if indexed_docs.saturating_add(pending_sources.len()) >= policy.max_docs {
                    stop_after_seq = Some(seq);
                }
            }
        }

        prepared.reserve(pending_sources.len());
        {
            // Text analysis can be CPU-heavy. A read lease keeps the schema stable
            // while allowing searches to continue on the last published reader.
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            let mut current_outbox_seq = None;
            let mut stop_after_seq = None;
            for pending in pending_sources {
                if current_outbox_seq != Some(pending.seq) {
                    if stop_after_seq.is_some() {
                        processed_all = false;
                        break;
                    }
                    current_outbox_seq = Some(pending.seq);
                }
                let action = match pending.action {
                    FullTextPendingSourceAction::Upsert {
                        fields,
                        expires_at_ms,
                        json_root,
                    } => {
                        let document = runtime.prepare_fields_document(
                            &pending.key,
                            &fields,
                            expires_at_ms,
                        )?;
                        indexed_bytes = indexed_bytes.saturating_add(document.indexed_bytes);
                        indexed_docs = indexed_docs.saturating_add(1);
                        FullTextPreparedRefreshAction::Upsert {
                            document,
                            json_root,
                        }
                    }
                    FullTextPendingSourceAction::Delete => {
                        indexed_docs = indexed_docs.saturating_add(1);
                        FullTextPreparedRefreshAction::Delete
                    }
                    FullTextPendingSourceAction::Noop => FullTextPreparedRefreshAction::Noop,
                };
                prepared.push(FullTextPreparedRefreshMutation {
                    outbox_seq: Some(pending.seq),
                    key: pending.key,
                    action,
                });
                if indexed_docs >= policy.max_docs || indexed_bytes >= policy.max_bytes {
                    stop_after_seq = Some(pending.seq);
                }
            }
        }

        let mut vector_batches = FullTextVectorMutationBatches::new();
        for mutation in &prepared {
            match &mutation.action {
                FullTextPreparedRefreshAction::Upsert { json_root, .. } => {
                    self.fulltext_collect_vector_mutations(
                        index,
                        meta,
                        &mutation.key,
                        json_root.as_ref(),
                        &mut vector_batches,
                    )?;
                }
                FullTextPreparedRefreshAction::Delete => self.fulltext_collect_vector_deletions(
                    index,
                    meta,
                    &mutation.key,
                    &mut vector_batches,
                ),
                FullTextPreparedRefreshAction::Noop => {}
            }
        }
        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            // Publish vector and text mutations under the same generation lease;
            // otherwise an EVENTUAL hybrid query could observe a new vector entry
            // before the matching Tantivy reader is published.
            self.fulltext_apply_vector_mutations(vector_batches)?;
            for mutation in prepared {
                match mutation.action {
                    FullTextPreparedRefreshAction::Upsert { document, .. } => {
                        runtime.apply_prepared_document(document)?;
                        changed = true;
                    }
                    FullTextPreparedRefreshAction::Delete => {
                        runtime.delete_hash(&mutation.key);
                        changed = true;
                    }
                    FullTextPreparedRefreshAction::Noop => {}
                }
                if let Some(seq) = mutation.outbox_seq {
                    runtime.published_outbox_seq = runtime.published_outbox_seq.max(seq);
                }
            }
            if let Some((cursor, finished)) = backfill_progress {
                runtime.published_backfill_cursor = cursor;
                runtime.backfill_complete = finished;
            }
            if processed_all {
                runtime.published_outbox_seq = runtime.published_outbox_seq.max(scanned_through);
            }
            if changed {
                let published_seq = runtime.published_outbox_seq;
                runtime.publish_through(published_seq)?;
            } else {
                runtime.last_refresh_at = Instant::now();
            }
            if durable_checkpoint {
                checkpoint_state = Some((
                    runtime.directory.clone(),
                    runtime.published_outbox_seq,
                    runtime.published_backfill_cursor.clone(),
                    runtime.backfill_complete,
                ));
            }
        }

        let Some((directory, checkpoint_seq, checkpoint_cursor, backfill_complete)) =
            checkpoint_state
        else {
            return Ok(());
        };
        let checkpointed = directory.checkpoint()?;
        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            runtime.durable_outbox_seq = runtime.durable_outbox_seq.max(checkpoint_seq);
            runtime.last_checkpoint_at = Instant::now();
            meta.indexed_docs = runtime.num_docs();
        }

        let previous_durable_seq = meta.last_indexed_outbox_seq;
        let previous_state = meta.state;
        let previous_cursor = meta.backfill_cursor.clone();
        meta.last_indexed_outbox_seq = meta.last_indexed_outbox_seq.max(checkpoint_seq);
        if matches!(
            meta.state,
            FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
        ) {
            meta.backfill_cursor = checkpoint_cursor;
            if backfill_complete {
                meta.state = FullTextIndexState::Ready;
                meta.backfill_cursor = None;
            }
        }
        if checkpointed {
            meta.indexed_bytes = self.fulltext_file_bytes(&meta.active_storage) as u64;
        }
        if checkpointed
            || meta.last_indexed_outbox_seq != previous_durable_seq
            || meta.state != previous_state
            || meta.backfill_cursor != previous_cursor
        {
            let mut batch = WriteBatch::new();
            let prefix = fulltext_outbox_prefix(self.db_index, index);
            if checkpoint_seq == u64::MAX {
                delete_prefix_to_batch(&mut batch, &self.store, &prefix)?;
            } else {
                batch
                    .delete_range(
                        &prefix,
                        &fulltext_outbox_key(self.db_index, index, checkpoint_seq + 1),
                    )
                    .map_err(|error| Error::msg(error.to_string()))?;
            }
            self.fulltext_write_meta_cas(index, expected_meta_raw, meta, &mut batch)?;
            self.fulltext_runtimes
                .clear_outbox_pending(self.db_index, index);
        }
        Ok(())
    }
}
