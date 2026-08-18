use super::*;

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

impl Db {
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
                scanned_through = scanned_through.max(seq);
                let record = decode_record::<FullTextMutationRecord>(&raw)?;
                if record.incarnation != meta.incarnation {
                    continue;
                }
                latest_by_key.insert(record.key.clone(), (seq, record));
            }
            let mut pending = latest_by_key.into_values().collect::<Vec<_>>();
            pending.sort_by_key(|(seq, _)| *seq);
            for (seq, record) in pending {
                if Instant::now() >= deadline {
                    processed_all = false;
                    break;
                }
                let action = match record.kind {
                    FullTextMutationKind::UpsertKey => {
                        if !matches!(meta.source_type, FullTextSourceType::Hash) {
                            FullTextPendingSourceAction::Noop
                        } else {
                            let fields = self.hash_get_all(&record.key)?;
                            if fields.is_empty() || !fulltext_index_filter_matches(meta, &fields)? {
                                FullTextPendingSourceAction::Delete
                            } else {
                                FullTextPendingSourceAction::Upsert {
                                    fields,
                                    expires_at_ms: self.fulltext_source_expire_ms(&record.key),
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
            }
        }

        prepared.reserve(pending_sources.len());
        {
            // Text analysis can be CPU-heavy. A read lease keeps the schema stable
            // while allowing searches to continue on the last published reader.
            let runtime = runtime
                .read()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            for pending in pending_sources {
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
                    processed_all = false;
                    break;
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
