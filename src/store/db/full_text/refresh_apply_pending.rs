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
    ) -> Result<(), Error> {
        let mut changed = false;
        let mut indexed_docs = 0usize;
        let mut indexed_bytes = 0usize;
        let mut processed_outbox = Vec::new();
        let mut max_processed_seq = meta.last_indexed_outbox_seq;

        {
            let mut runtime = runtime
                .write()
                .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
            if matches!(
                meta.state,
                FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
            ) {
                let BackfillProgress {
                    finished,
                    cursor,
                    docs,
                    bytes,
                } = self.fulltext_apply_backfill_batch(
                    index,
                    &mut runtime,
                    meta,
                    policy,
                    deadline,
                )?;
                changed |= docs > 0;
                indexed_docs += docs;
                indexed_bytes += bytes;
                meta.backfill_cursor = cursor;
                if finished {
                    meta.state = FullTextIndexState::Ready;
                    meta.backfill_cursor = None;
                }
            }

            if indexed_docs < policy.max_docs
                && indexed_bytes < policy.max_bytes
                && Instant::now() < deadline
            {
                let prefix = fulltext_outbox_prefix(self.db_index, index);
                let remaining_docs = policy.max_docs.saturating_sub(indexed_docs);
                for (outbox_key, raw) in self.store.scan_range_raw_limited(
                    &prefix,
                    prefix_exclusive_upper_bound(&prefix),
                    remaining_docs.max(1),
                ) {
                    let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key)
                    else {
                        continue;
                    };
                    let record = decode_record::<FullTextMutationRecord>(&raw)?;
                    if record.incarnation != meta.incarnation {
                        processed_outbox.push(outbox_key);
                        continue;
                    }
                    match record.kind {
                        FullTextMutationKind::UpsertKey => {
                            if !matches!(meta.source_type, FullTextSourceType::Hash) {
                                processed_outbox.push(outbox_key);
                                continue;
                            }
                            let fields = self.hash_get_all(&record.key)?;
                            if fields.is_empty() || !fulltext_index_filter_matches(meta, &fields)? {
                                runtime.delete_hash(&record.key);
                                self.fulltext_delete_vectors(index, meta, &record.key)?;
                            } else {
                                indexed_bytes += runtime.upsert_hash(&record.key, &fields)?;
                                self.fulltext_upsert_vectors(
                                    index,
                                    meta,
                                    &record.key,
                                    &fields,
                                    None,
                                )?;
                            }
                        }
                        FullTextMutationKind::UpsertJson => {
                            if !matches!(meta.source_type, FullTextSourceType::Json) {
                                processed_outbox.push(outbox_key);
                                continue;
                            }
                            if let Some(root) = self.fulltext_json_root(&record.key)? {
                                let fields = self.fulltext_json_fields_from_root(&root, meta)?;
                                if fulltext_index_filter_matches(meta, &fields)? {
                                    indexed_bytes += runtime.upsert_fields(&record.key, &fields)?;
                                    self.fulltext_upsert_vectors(
                                        index,
                                        meta,
                                        &record.key,
                                        &fields,
                                        Some(&root),
                                    )?;
                                } else {
                                    runtime.delete_hash(&record.key);
                                    self.fulltext_delete_vectors(index, meta, &record.key)?;
                                }
                            } else {
                                runtime.delete_hash(&record.key);
                                self.fulltext_delete_vectors(index, meta, &record.key)?;
                            }
                        }
                        FullTextMutationKind::DeleteKey => {
                            runtime.delete_hash(&record.key);
                            self.fulltext_delete_vectors(index, meta, &record.key)?;
                        }
                    }
                    changed = true;
                    indexed_docs += 1;
                    max_processed_seq = max_processed_seq.max(seq);
                    processed_outbox.push(outbox_key);
                    if indexed_docs >= policy.max_docs
                        || indexed_bytes >= policy.max_bytes
                        || Instant::now() >= deadline
                    {
                        break;
                    }
                }
            }

            if changed {
                runtime.publish()?;
                meta.indexed_docs = runtime.num_docs();
            } else {
                runtime.last_refresh_at = Instant::now();
            }
        }

        if changed
            || !processed_outbox.is_empty()
            || max_processed_seq != meta.last_indexed_outbox_seq
        {
            if changed {
                meta.indexed_bytes = self.fulltext_file_bytes(&meta.active_storage) as u64;
            }
            meta.last_indexed_outbox_seq = max_processed_seq;
            let mut batch = WriteBatch::new();
            for key in processed_outbox {
                batch.delete(&key);
            }
            let processed = batch.count() as u64;
            self.fulltext_write_meta_cas(index, expected_meta_raw, meta, &mut batch)?;
            self.fulltext_runtimes
                .remove_outbox_pending(self.db_index, index, processed);
        }
        Ok(())
    }
}
