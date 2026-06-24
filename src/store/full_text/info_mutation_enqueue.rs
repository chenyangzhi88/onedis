impl Db {
    pub(crate) fn fulltext_enqueue_hash_upsert_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
    ) -> Result<(), Error> {
        self.fulltext_enqueue_mutation_to_batch(
            batch,
            key,
            FullTextSourceType::Hash,
            FullTextMutationKind::UpsertKey,
        )
    }

    pub(crate) fn fulltext_enqueue_hash_delete_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
    ) -> Result<(), Error> {
        self.fulltext_enqueue_mutation_to_batch(
            batch,
            key,
            FullTextSourceType::Hash,
            FullTextMutationKind::DeleteKey,
        )
    }

    pub(crate) fn fulltext_enqueue_json_upsert_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
    ) -> Result<(), Error> {
        self.fulltext_enqueue_mutation_to_batch(
            batch,
            key,
            FullTextSourceType::Json,
            FullTextMutationKind::UpsertJson,
        )
    }

    pub(crate) fn fulltext_enqueue_json_delete_to_batch(
        &self,
        batch: &mut WriteBatch,
        key: &str,
    ) -> Result<(), Error> {
        self.fulltext_enqueue_mutation_to_batch(
            batch,
            key,
            FullTextSourceType::Json,
            FullTextMutationKind::DeleteKey,
        )
    }
}
