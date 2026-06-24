impl Db {
    fn fulltext_compact_outbox_if_needed(
        &self,
        index: &str,
        generation: u64,
        threshold: usize,
    ) -> Result<(), Error> {
        if threshold == 0 {
            return Ok(());
        }
        let entries = self
            .store
            .scan_prefix_raw(&fulltext_outbox_prefix(self.db_index, index));
        if entries.len() <= threshold {
            return Ok(());
        }
        let mut latest_by_key: HashMap<String, (u64, Vec<u8>)> = HashMap::new();
        let mut stale = Vec::new();
        for (outbox_key, raw) in entries {
            let Some(seq) = fulltext_outbox_seq_from_key(self.db_index, index, &outbox_key) else {
                stale.push(outbox_key);
                continue;
            };
            let record = decode_record::<FullTextMutationRecord>(&raw)?;
            if record.generation != generation {
                stale.push(outbox_key);
                continue;
            }
            match latest_by_key.insert(record.key.clone(), (seq, outbox_key.clone())) {
                Some((previous_seq, previous_key)) if previous_seq < seq => {
                    stale.push(previous_key)
                }
                Some((previous_seq, previous_key)) => {
                    stale.push(outbox_key);
                    latest_by_key.insert(record.key, (previous_seq, previous_key));
                }
                None => {}
            }
        }
        if stale.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for key in stale {
            batch.delete(&key);
        }
        self.write_batch_if_not_empty(&batch);
        Ok(())
    }
}
