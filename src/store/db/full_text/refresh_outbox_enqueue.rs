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
            batch.put(
                &fulltext_outbox_key(self.db_index, &index_name, seq),
                &encode_record(&record)?,
            );
        }
        Ok(())
    }
}
