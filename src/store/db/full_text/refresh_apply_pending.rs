use super::*;
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
        let mut vector_batches = FullTextVectorMutationBatches::new();

        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            if matches!(
                meta.state,
                FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
            ) {
                let mut published_meta = meta.clone();
                published_meta.backfill_cursor = runtime.published_backfill_cursor.clone();
                let BackfillProgress {
                    finished,
                    cursor,
                    docs,
                    bytes,
                } = self.fulltext_apply_backfill_batch(
                    index,
                    &mut runtime,
                    &published_meta,
                    policy,
                    deadline,
                )?;
                changed |= docs > 0;
                indexed_docs += docs;
                indexed_bytes += bytes;
                runtime.published_backfill_cursor = cursor;
                runtime.backfill_complete = finished;
            }

            if indexed_docs < policy.max_docs
                && indexed_bytes < policy.max_bytes
                && Instant::now() < deadline
            {
                let prefix = fulltext_outbox_prefix(self.db_index, index);
                let start = runtime
                    .published_outbox_seq
                    .checked_add(1)
                    .map(|seq| fulltext_outbox_key(self.db_index, index, seq));
                let remaining_docs = policy.max_docs.saturating_sub(indexed_docs);
                for (outbox_key, raw) in start.into_iter().flat_map(|start| {
                    self.store.scan_range_raw_limited(
                        &start,
                        prefix_exclusive_upper_bound(&prefix),
                        remaining_docs.max(1),
                    )
                }) {
                    let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key)
                    else {
                        continue;
                    };
                    let record = decode_record::<FullTextMutationRecord>(&raw)?;
                    if record.incarnation != meta.incarnation {
                        runtime.published_outbox_seq = runtime.published_outbox_seq.max(seq);
                        continue;
                    }
                    match record.kind {
                        FullTextMutationKind::UpsertKey => {
                            if !matches!(meta.source_type, FullTextSourceType::Hash) {
                                runtime.published_outbox_seq =
                                    runtime.published_outbox_seq.max(seq);
                                continue;
                            }
                            let fields = self.hash_get_all(&record.key)?;
                            if fields.is_empty() || !fulltext_index_filter_matches(meta, &fields)? {
                                runtime.delete_hash(&record.key);
                                self.fulltext_collect_vector_deletions(
                                    index,
                                    meta,
                                    &record.key,
                                    &mut vector_batches,
                                );
                            } else {
                                indexed_bytes += runtime.upsert_hash(
                                    &record.key,
                                    &fields,
                                    self.fulltext_source_expire_ms(&record.key),
                                )?;
                                self.fulltext_collect_vector_mutations(
                                    index,
                                    meta,
                                    &record.key,
                                    None,
                                    &mut vector_batches,
                                )?;
                            }
                        }
                        FullTextMutationKind::UpsertJson => {
                            if !matches!(meta.source_type, FullTextSourceType::Json) {
                                runtime.published_outbox_seq =
                                    runtime.published_outbox_seq.max(seq);
                                continue;
                            }
                            if let Some(root) = self.fulltext_json_root(&record.key)? {
                                let fields = self.fulltext_json_fields_from_root(&root, meta)?;
                                if fulltext_index_filter_matches(meta, &fields)? {
                                    indexed_bytes += runtime.upsert_fields(
                                        &record.key,
                                        &fields,
                                        self.fulltext_source_expire_ms(&record.key),
                                    )?;
                                    self.fulltext_collect_vector_mutations(
                                        index,
                                        meta,
                                        &record.key,
                                        Some(&root),
                                        &mut vector_batches,
                                    )?;
                                } else {
                                    runtime.delete_hash(&record.key);
                                    self.fulltext_collect_vector_deletions(
                                        index,
                                        meta,
                                        &record.key,
                                        &mut vector_batches,
                                    );
                                }
                            } else {
                                runtime.delete_hash(&record.key);
                                self.fulltext_collect_vector_deletions(
                                    index,
                                    meta,
                                    &record.key,
                                    &mut vector_batches,
                                );
                            }
                        }
                        FullTextMutationKind::DeleteKey => {
                            runtime.delete_hash(&record.key);
                            self.fulltext_collect_vector_deletions(
                                index,
                                meta,
                                &record.key,
                                &mut vector_batches,
                            );
                        }
                    }
                    changed = true;
                    indexed_docs += 1;
                    runtime.published_outbox_seq = runtime.published_outbox_seq.max(seq);
                    if indexed_docs >= policy.max_docs
                        || indexed_bytes >= policy.max_bytes
                        || Instant::now() >= deadline
                    {
                        break;
                    }
                }
            }

            self.fulltext_apply_vector_mutations(std::mem::take(&mut vector_batches))?;
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
