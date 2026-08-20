use super::*;
impl Db {
    pub(in crate::store::db) fn fulltext_hash_source_is_indexed(
        &self,
        key: &str,
    ) -> Result<bool, Error> {
        Ok(!self
            .fulltext_matching_metas_for_source(key, FullTextSourceType::Hash)?
            .is_empty())
    }

    pub(super) fn fulltext_enqueue_mutation_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
        source_type: FullTextSourceType,
        kind: FullTextMutationKind,
    ) -> Result<(), Error> {
        if self.store.is_transactional() {
            return Ok(());
        }
        for (index_name, meta) in self.fulltext_matching_metas_for_source(key, source_type)? {
            let seq = self.next_fulltext_sequence();
            let record = FullTextMutationRecord {
                incarnation: meta.incarnation,
                kind,
                key: key.to_string(),
                projection: None,
            };
            batch
                .put(
                    &fulltext_outbox_key(self.db_index, &index_name, seq),
                    &encode_record(&record)?,
                )
                .map_err(|error| Error::msg(error.to_string()))?;
            batch
                .put(
                    &fulltext_outbox_latest_key(self.db_index, &index_name),
                    &seq.to_be_bytes(),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        Ok(())
    }

    /// Appends one packed outbox record per matching index and HSET batch.
    /// The source fields and durable outbox record still share one atomic KV
    /// commit, but a pipeline no longer creates one extra KV entry per document.
    pub(in crate::store::db) fn fulltext_enqueue_hash_upserts_to_batch(
        &self,
        batch: &mut WriteBatch,
        keys: &[&str],
    ) -> Result<(), Error> {
        if self.store.is_transactional() || keys.is_empty() {
            return Ok(());
        }
        let routes = self.fulltext_source_routes()?;
        for route in routes.iter().filter(|route| {
            route.source_type == FullTextSourceType::Hash
                && keys
                    .iter()
                    .any(|key| route.prefixes.iter().any(|prefix| key.starts_with(prefix)))
        }) {
            if self.fulltext_index_expired(&route.index, &route.meta)?
                || matches!(route.meta.state, FullTextIndexState::Dropping)
            {
                continue;
            }
            self.fulltext_touch_temporary_index(&route.index, &route.meta)?;
            let matching_keys = keys
                .iter()
                .copied()
                .filter(|key| route.prefixes.iter().any(|prefix| key.starts_with(prefix)))
                .collect::<Vec<_>>();
            if matching_keys.is_empty() {
                continue;
            }
            let seq = self.next_fulltext_sequence();
            batch
                .put(
                    &fulltext_outbox_key(self.db_index, &route.index, seq),
                    &encode_fulltext_mutation_batch(
                        route.meta.incarnation,
                        FullTextMutationKind::UpsertKey,
                        &matching_keys,
                    )?,
                )
                .map_err(|error| Error::msg(error.to_string()))?;
            batch
                .put(
                    &fulltext_outbox_latest_key(self.db_index, &route.index),
                    &seq.to_be_bytes(),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        Ok(())
    }

    /// Adds a compact, index-specific field projection to new-HASH pipeline
    /// outbox records. The projection is bounded and only used when it is
    /// complete (no arbitrary FILTER fields and no binary VECTOR fields).
    pub(in crate::store::db) fn fulltext_enqueue_new_hash_projections_to_batch(
        &self,
        batch: &mut WriteBatch,
        documents: &[(&str, &PackedHashFields)],
    ) -> Result<(), Error> {
        const MAX_PROJECTED_BATCH_BYTES: usize = 4 * 1024 * 1024;
        if self.store.is_transactional() || documents.is_empty() {
            return Ok(());
        }
        let routes = self.fulltext_source_routes()?;
        for route in routes.iter().filter(|route| {
            route.source_type == FullTextSourceType::Hash
                && documents
                    .iter()
                    .any(|(key, _)| route.prefixes.iter().any(|prefix| key.starts_with(prefix)))
        }) {
            if self.fulltext_index_expired(&route.index, &route.meta)?
                || matches!(route.meta.state, FullTextIndexState::Dropping)
            {
                continue;
            }
            self.fulltext_touch_temporary_index(&route.index, &route.meta)?;
            let matching = documents
                .iter()
                .filter(|(key, _)| route.prefixes.iter().any(|prefix| key.starts_with(prefix)))
                .collect::<Vec<_>>();
            if matching.is_empty() {
                continue;
            }
            let supports_projection = route.meta.index_options.filter.is_none()
                && !route
                    .meta
                    .schema
                    .iter()
                    .any(|field| matches!(field.kind, FullTextFieldKind::Vector))
                // Packed HASH values already need one bounded metadata read.
                // Duplicating their fields in the outbox is pure write
                // amplification; projection pays off for separated large HASHes.
                && matching
                    .iter()
                    .all(|(_, fields)| !hash_uses_packed_layout(fields));
            let seq = self.next_fulltext_sequence();
            let raw = if supports_projection {
                let mut field_names = route
                    .meta
                    .schema
                    .iter()
                    .map(|field| field.name.as_str())
                    .chain(route.meta.index_options.language_field.as_deref())
                    .chain(route.meta.index_options.score_field.as_deref())
                    .chain(route.meta.index_options.payload_field.as_deref())
                    .collect::<Vec<_>>();
                field_names.sort_unstable();
                field_names.dedup();
                let mutations = matching
                    .iter()
                    .map(|(key, fields)| FullTextProjectedMutation {
                        key: (*key).to_string(),
                        projection: FullTextIndexedProjection {
                            fields: field_names
                                .iter()
                                .filter_map(|field| {
                                    fields.get(*field).and_then(|value| {
                                        String::from_utf8(value.clone())
                                            .ok()
                                            .map(|value| ((*field).to_string(), value))
                                    })
                                })
                                .collect(),
                            expires_at_ms: 0,
                        },
                    })
                    .collect::<Vec<_>>();
                let projected =
                    encode_fulltext_projected_mutation_batch(route.meta.incarnation, mutations)?;
                if projected.len() <= MAX_PROJECTED_BATCH_BYTES {
                    projected
                } else {
                    encode_fulltext_mutation_batch(
                        route.meta.incarnation,
                        FullTextMutationKind::UpsertKey,
                        &matching.iter().map(|&&(key, _)| key).collect::<Vec<_>>(),
                    )?
                }
            } else {
                encode_fulltext_mutation_batch(
                    route.meta.incarnation,
                    FullTextMutationKind::UpsertKey,
                    &matching.iter().map(|&&(key, _)| key).collect::<Vec<_>>(),
                )?
            };
            batch
                .put(&fulltext_outbox_key(self.db_index, &route.index, seq), &raw)
                .map_err(|error| Error::msg(error.to_string()))?;
            batch
                .put(
                    &fulltext_outbox_latest_key(self.db_index, &route.index),
                    &seq.to_be_bytes(),
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        Ok(())
    }
}
