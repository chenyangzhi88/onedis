impl Db {
    fn fulltext_apply_backfill_batch(
        &self,
        index: &str,
        runtime: &mut FullTextRuntime,
        meta: &FullTextIndexMeta,
        policy: &FullTextRefreshPolicy,
        deadline: Instant,
    ) -> Result<BackfillProgress, Error> {
        let mut docs = 0usize;
        let mut bytes = 0usize;
        let mut cursor = meta.backfill_cursor.clone();
        if Instant::now() >= deadline {
            return Ok(BackfillProgress {
                finished: false,
                cursor,
                docs,
                bytes,
            });
        }
        let keys = self
            .fulltext_source_keys(meta)?
            .into_iter()
            .filter(|key| cursor.as_ref().is_none_or(|last| key > last))
            .collect::<Vec<_>>();
        let finished = keys.len() <= policy.max_docs;
        for key in keys.into_iter().take(policy.max_docs) {
            if Instant::now() >= deadline {
                return Ok(BackfillProgress {
                    finished: false,
                    cursor,
                    docs,
                    bytes,
                });
            }
            match meta.source_type {
                FullTextSourceType::Hash => {
                    let fields = self.hash_get_all(&key)?;
                    if !fields.is_empty() {
                        if fulltext_index_filter_matches(meta, &fields)? {
                            bytes += runtime.upsert_hash(&key, &fields)?;
                            self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        } else {
                            runtime.delete_hash(&key);
                            self.fulltext_delete_vectors(index, meta, &key)?;
                        }
                    }
                }
                FullTextSourceType::Json => {
                    if let Some(fields) = self.fulltext_json_fields(&key, meta)? {
                        if fulltext_index_filter_matches(meta, &fields)? {
                            bytes += runtime.upsert_fields(&key, &fields)?;
                            self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        } else {
                            runtime.delete_hash(&key);
                            self.fulltext_delete_vectors(index, meta, &key)?;
                        }
                    }
                }
            }
            docs += 1;
            cursor = Some(key);
            if bytes >= policy.max_bytes {
                return Ok(BackfillProgress {
                    finished: false,
                    cursor,
                    docs,
                    bytes,
                });
            }
        }
        Ok(BackfillProgress {
            finished,
            cursor,
            docs,
            bytes,
        })
    }
}
