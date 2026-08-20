use super::*;
impl Db {
    pub(super) fn fulltext_build_generation_from_existing(
        &self,
        index: &str,
        old_meta: &FullTextIndexMeta,
        new_meta: &FullTextIndexMeta,
        runtime: &mut FullTextRuntime,
    ) -> Result<(), Error> {
        let old_runtime = self
            .fulltext_runtimes
            .get(self.db_index, index)
            .ok_or_else(|| Error::msg("ERR fulltext index does not exist"))?;
        let old_runtime = old_runtime
            .read()
            .map_err(|_| Error::msg("ERR fulltext runtime lock poisoned"))?;
        let mut existing_vector_meta = new_meta.clone();
        existing_vector_meta.schema = old_meta.schema.clone();
        let mut docs_since_publish = 0usize;
        let mut bytes_since_publish = 0usize;
        let policy = self.fulltext_effective_refresh_policy(new_meta)?;
        old_runtime.visit_indexed_keys(|key| {
            let mut json_root = None;
            let mut fields = match old_meta.source_type {
                FullTextSourceType::Hash => self.hash_get_all(key)?,
                FullTextSourceType::Json => {
                    let Some(root) = self.fulltext_json_root(key)? else {
                        return Ok(());
                    };
                    let fields = self.fulltext_json_fields_from_root(&root, old_meta)?;
                    json_root = Some(root);
                    fields
                }
            };
            if fields.is_empty() || !fulltext_index_filter_matches(old_meta, &fields)? {
                return Ok(());
            }
            if matches!(old_meta.source_type, FullTextSourceType::Hash) {
                let mut allowed = old_meta
                    .schema
                    .iter()
                    .flat_map(|field| [field.name.as_str(), field.attribute_name()])
                    .collect::<HashSet<_>>();
                allowed.extend(
                    [
                        old_meta.index_options.language_field.as_deref(),
                        old_meta.index_options.score_field.as_deref(),
                        old_meta.index_options.payload_field.as_deref(),
                    ]
                    .into_iter()
                    .flatten(),
                );
                fields.retain(|(name, _)| allowed.contains(name.as_str()));
            }
            bytes_since_publish = bytes_since_publish.saturating_add(runtime.upsert_fields(
                key,
                &fields,
                self.fulltext_source_expire_ms(key)?,
            )?);
            match old_meta.source_type {
                FullTextSourceType::Hash => {
                    self.fulltext_upsert_vectors(index, &existing_vector_meta, key, &fields, None)?
                }
                FullTextSourceType::Json => {
                    if let Some(root) = json_root.as_ref() {
                        self.fulltext_upsert_vectors(
                            index,
                            &existing_vector_meta,
                            key,
                            &fields,
                            Some(root),
                        )?;
                    }
                }
            }
            docs_since_publish = docs_since_publish.saturating_add(1);
            if docs_since_publish >= policy.max_docs || bytes_since_publish >= policy.max_bytes {
                runtime.publish()?;
                docs_since_publish = 0;
                bytes_since_publish = 0;
            }
            Ok(())
        })?;
        if docs_since_publish > 0 || runtime.num_docs() == 0 {
            runtime.publish()?;
        }
        Ok(())
    }

    pub(super) fn fulltext_build_generation(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
        runtime: &mut FullTextRuntime,
    ) -> Result<(), Error> {
        let mut build_meta = meta.clone();
        build_meta.backfill_cursor = None;
        let policy = self.fulltext_effective_refresh_policy(meta)?;
        loop {
            let progress = self.fulltext_apply_backfill_batch(
                index,
                runtime,
                &build_meta,
                &policy,
                Instant::now()
                    .checked_add(Duration::from_secs(24 * 60 * 60))
                    .unwrap_or_else(Instant::now),
            )?;
            if progress.docs > 0 {
                runtime.publish()?;
            }
            build_meta.backfill_cursor = progress.cursor;
            if progress.finished {
                return Ok(());
            }
            if progress.docs == 0 {
                return Err(Error::msg("ERR fulltext generation build made no progress"));
            }
        }
    }

    pub(super) fn fulltext_apply_backfill_batch(
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
        if policy.max_docs == 0 || policy.max_bytes == 0 || Instant::now() >= deadline {
            return Ok(BackfillProgress {
                finished: false,
                cursor,
                docs,
            });
        }
        let (keys, has_more) =
            self.fulltext_source_keys_page(meta, cursor.as_deref(), policy.max_docs)?;
        let mut finished = !has_more;
        let mut vector_batches = FullTextVectorMutationBatches::new();
        for key in keys {
            if Instant::now() >= deadline {
                finished = false;
                break;
            }
            match meta.source_type {
                FullTextSourceType::Hash => {
                    let fields = self.hash_get_all(&key)?;
                    if !fields.is_empty() {
                        if fulltext_index_filter_matches(meta, &fields)? {
                            bytes += runtime.upsert_hash(
                                &key,
                                &fields,
                                self.fulltext_source_expire_ms(&key)?,
                            )?;
                            self.fulltext_collect_vector_mutations(
                                index,
                                meta,
                                &key,
                                None,
                                &mut vector_batches,
                            )?;
                        } else {
                            runtime.delete_hash(&key);
                            self.fulltext_collect_vector_deletions(
                                index,
                                meta,
                                &key,
                                &mut vector_batches,
                            );
                        }
                    }
                }
                FullTextSourceType::Json => {
                    if let Some(root) = self.fulltext_json_root(&key)? {
                        let fields = self.fulltext_json_fields_from_root(&root, meta)?;
                        if fulltext_index_filter_matches(meta, &fields)? {
                            bytes += runtime.upsert_fields(
                                &key,
                                &fields,
                                self.fulltext_source_expire_ms(&key)?,
                            )?;
                            self.fulltext_collect_vector_mutations(
                                index,
                                meta,
                                &key,
                                Some(&root),
                                &mut vector_batches,
                            )?;
                        } else {
                            runtime.delete_hash(&key);
                            self.fulltext_collect_vector_deletions(
                                index,
                                meta,
                                &key,
                                &mut vector_batches,
                            );
                        }
                    }
                }
            }
            docs += 1;
            cursor = Some(key);
            if bytes >= policy.max_bytes {
                finished = false;
                break;
            }
        }
        self.fulltext_apply_vector_mutations(vector_batches)?;
        Ok(BackfillProgress {
            finished,
            cursor,
            docs,
        })
    }
}
