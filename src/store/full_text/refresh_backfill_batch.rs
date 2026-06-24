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
        let mut seen = HashSet::new();
        let mut keys = Vec::new();
        for prefix in &meta.prefixes {
            for (raw_key, raw_value) in self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
            {
                if raw_key.len() < 2 {
                    continue;
                }
                let key = String::from_utf8_lossy(&raw_key[2..]).to_string();
                if !key.starts_with(prefix) || cursor.as_ref().is_some_and(|last| key <= *last) {
                    continue;
                }
                let matches_source = match meta.source_type {
                    FullTextSourceType::Hash => decode_hash_meta_checked(&raw_value).is_ok(),
                    FullTextSourceType::Json => Self::decode_json_meta(&raw_value).is_ok(),
                };
                if !matches_source || !seen.insert(key.clone()) {
                    continue;
                }
                keys.push(key);
            }
        }
        keys.sort();
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
                        bytes += runtime.upsert_hash(&key, &fields)?;
                        self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        docs += 1;
                    }
                }
                FullTextSourceType::Json => {
                    if let Some(fields) = self.fulltext_json_fields(&key, meta)? {
                        bytes += runtime.upsert_fields(&key, &fields)?;
                        self.fulltext_upsert_vectors(index, meta, &key, &fields)?;
                        docs += 1;
                    }
                }
            }
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
