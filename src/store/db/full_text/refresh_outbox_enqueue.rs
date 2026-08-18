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
            if self.fulltext_index_expired(&route.index, &route.meta)
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
}
