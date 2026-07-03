impl Db {
    pub fn fulltext_observability_snapshot(
        &self,
    ) -> crate::store::db::FullTextObservabilitySnapshot {
        let mut snapshot = crate::store::db::FullTextObservabilitySnapshot::default();
        let Ok(metas) = self.read_all_fulltext_metas() else {
            return snapshot;
        };
        for (index, meta) in metas {
            match meta.state {
                FullTextIndexState::Creating => snapshot.creating += 1,
                FullTextIndexState::Backfilling => snapshot.backfilling += 1,
                FullTextIndexState::Ready => snapshot.ready += 1,
                FullTextIndexState::Dirty => snapshot.dirty += 1,
                FullTextIndexState::Rebuilding => snapshot.rebuilding += 1,
                FullTextIndexState::Dropping => snapshot.dropping += 1,
            }
            if meta.backfill_cursor.is_some()
                || matches!(
                    meta.state,
                    FullTextIndexState::Backfilling | FullTextIndexState::Rebuilding
                )
            {
                snapshot.backfill_pending += 1;
            }
            snapshot.outbox_pending += self
                .store
                .scan_prefix_raw(&fulltext_outbox_prefix(self.db_index, &index))
                .len() as u64;
        }
        snapshot
    }

    pub fn fulltext_info(&self, index: &str) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let meta = self.read_fulltext_meta_direct(&index)?;
        let pending = self
            .store
            .scan_prefix_raw(&fulltext_outbox_prefix(self.db_index, &index))
            .len();
        let source_keys = self.fulltext_source_keys(&meta).unwrap_or_default();
        let source_key_count = source_keys.len();
        let indexed_field_count = meta
            .schema
            .iter()
            .filter(|field| !field.options.noindex)
            .count();
        let vector_field_count = meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Vector))
            .count();
        let text_field_count = meta
            .schema
            .iter()
            .filter(|field| matches!(field.kind, FullTextFieldKind::Text))
            .count();
        let runtime_loaded = self.fulltext_runtimes.get(self.db_index, &index).is_some();
        let file_bytes = self.fulltext_file_bytes(&index);
        let effective_policy = self.fulltext_effective_refresh_policy(&meta)?;
        let cluster_enabled = self.fulltext_cluster_enabled()?;
        let cluster_shards = self.fulltext_cluster_shards()?;
        let cluster_shard_id = self.fulltext_cluster_shard_id()?;
        Ok(Frame::Array(vec![
            Frame::bulk_string("index_name"),
            Frame::bulk_string(index),
            Frame::bulk_string("index_definition"),
            Frame::Array(vec![
                Frame::bulk_string("key_type"),
                Frame::bulk_string(fulltext_source_type_name(meta.source_type)),
                Frame::bulk_string("prefixes"),
                Frame::Array(
                    meta.prefixes
                        .iter()
                        .cloned()
                        .map(Frame::bulk_string)
                        .collect(),
                ),
                Frame::bulk_string("filter"),
                meta.index_options
                    .filter
                    .clone()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("language"),
                meta.index_options
                    .language
                    .clone()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("language_field"),
                meta.index_options
                    .language_field
                    .clone()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("score"),
                meta.index_options
                    .score
                    .map(|score| Frame::bulk_string(score.to_string()))
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("score_field"),
                meta.index_options
                    .score_field
                    .clone()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("payload_field"),
                meta.index_options
                    .payload_field
                    .clone()
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("max_text_fields"),
                Frame::Integer(i64::from(meta.index_options.max_text_fields)),
                Frame::bulk_string("temporary_seconds"),
                meta.index_options
                    .temporary_seconds
                    .map(|seconds| Frame::Integer(seconds as i64))
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("no_offsets"),
                Frame::Integer(i64::from(meta.index_options.no_offsets)),
                Frame::bulk_string("no_hl"),
                Frame::Integer(i64::from(meta.index_options.no_hl)),
                Frame::bulk_string("no_fields"),
                Frame::Integer(i64::from(meta.index_options.no_fields)),
                Frame::bulk_string("no_freqs"),
                Frame::Integer(i64::from(meta.index_options.no_freqs)),
                Frame::bulk_string("stopwords"),
                meta.index_options
                    .stopwords
                    .clone()
                    .map(|words| Frame::Array(words.into_iter().map(Frame::bulk_string).collect()))
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("skip_initial_scan"),
                Frame::Integer(i64::from(meta.index_options.skip_initial_scan)),
                Frame::bulk_string("index_all"),
                meta.index_options
                    .index_all
                    .map(|enabled| if enabled { "ENABLE" } else { "DISABLE" })
                    .map(Frame::bulk_string)
                    .unwrap_or(Frame::Null),
                Frame::bulk_string("aliases"),
                Frame::Array(
                    meta.aliases
                        .iter()
                        .cloned()
                        .map(Frame::bulk_string)
                        .collect(),
                ),
            ]),
            Frame::bulk_string("attributes"),
            fulltext_schema_frame(&meta.schema),
            Frame::bulk_string("state"),
            Frame::bulk_string(fulltext_state_name(meta.state)),
            Frame::bulk_string("generation"),
            Frame::Integer(meta.generation as i64),
            Frame::bulk_string("backfill_cursor"),
            meta.backfill_cursor
                .map(Frame::bulk_string)
                .unwrap_or(Frame::Null),
            Frame::bulk_string("last_indexed_outbox_seq"),
            Frame::Integer(meta.last_indexed_outbox_seq as i64),
            Frame::bulk_string("pending_outbox"),
            Frame::Integer(pending as i64),
            Frame::bulk_string("refresh_interval_ms"),
            Frame::Integer(effective_policy.refresh_interval_ms as i64),
            Frame::bulk_string("refresh_max_docs"),
            Frame::Integer(effective_policy.max_docs as i64),
            Frame::bulk_string("refresh_max_bytes"),
            Frame::Integer(effective_policy.max_bytes as i64),
            Frame::bulk_string("refresh_timeout_ms"),
            Frame::Integer(self.fulltext_refresh_timeout_ms()? as i64),
            Frame::bulk_string("outbox_compact_threshold"),
            Frame::Integer(self.fulltext_outbox_compact_threshold()? as i64),
            Frame::bulk_string("repair_throttle_ms"),
            Frame::Integer(self.fulltext_repair_throttle_ms()? as i64),
            Frame::bulk_string("num_docs"),
            Frame::Integer(source_key_count as i64),
            Frame::bulk_string("num_records"),
            Frame::Integer(source_key_count as i64),
            Frame::bulk_string("num_terms"),
            Frame::Integer(0),
            Frame::bulk_string("num_records_per_doc_avg"),
            Frame::bulk_string(if source_key_count == 0 { "0" } else { "1" }),
            Frame::bulk_string("num_fields"),
            Frame::Integer(meta.schema.len() as i64),
            Frame::bulk_string("num_indexed_fields"),
            Frame::Integer(indexed_field_count as i64),
            Frame::bulk_string("num_text_fields"),
            Frame::Integer(text_field_count as i64),
            Frame::bulk_string("num_vector_fields"),
            Frame::Integer(vector_field_count as i64),
            Frame::bulk_string("inverted_sz_mb"),
            Frame::bulk_string(format!("{:.6}", file_bytes as f64 / 1024.0 / 1024.0)),
            Frame::bulk_string("vector_index_sz_mb"),
            Frame::bulk_string("0.000000"),
            Frame::bulk_string("offset_vectors_sz_mb"),
            Frame::bulk_string("0.000000"),
            Frame::bulk_string("doc_table_size_mb"),
            Frame::bulk_string("0.000000"),
            Frame::bulk_string("sortable_values_size_mb"),
            Frame::bulk_string("0.000000"),
            Frame::bulk_string("key_table_size_mb"),
            Frame::bulk_string("0.000000"),
            Frame::bulk_string("outbox_queue_length"),
            Frame::Integer(pending as i64),
            Frame::bulk_string("runtime_loaded"),
            Frame::Integer(i64::from(runtime_loaded)),
            Frame::bulk_string("memory_budget"),
            Frame::Array(vec![
                Frame::bulk_string("reader_bytes"),
                Frame::Integer(
                    self.fulltext_config_u64("MEMORY_BUDGET_READER_BYTES", 67_108_864)? as i64,
                ),
                Frame::bulk_string("writer_bytes"),
                Frame::Integer(self.fulltext_config_u64(
                    "MEMORY_BUDGET_WRITER_BYTES",
                    FULLTEXT_WRITER_HEAP_BYTES as u64,
                )? as i64),
                Frame::bulk_string("sort_bytes"),
                Frame::Integer(
                    self.fulltext_config_u64("MEMORY_BUDGET_SORT_BYTES", 16_777_216)? as i64,
                ),
                Frame::bulk_string("aggregate_cursor_bytes"),
                Frame::Integer(
                    self.fulltext_config_u64("MEMORY_BUDGET_AGGREGATE_CURSOR_BYTES", 16_777_216)?
                        as i64,
                ),
                Frame::bulk_string("vector_heap_bytes"),
                Frame::Integer(
                    self.fulltext_config_u64("MEMORY_BUDGET_VECTOR_HEAP_BYTES", 16_777_216)? as i64,
                ),
            ]),
            Frame::bulk_string("cluster"),
            Frame::Array(vec![
                Frame::bulk_string("enabled"),
                Frame::Integer(i64::from(cluster_enabled)),
                Frame::bulk_string("shards"),
                Frame::Integer(cluster_shards as i64),
                Frame::bulk_string("shard_id"),
                Frame::Integer(cluster_shard_id as i64),
                Frame::bulk_string("placement"),
                Frame::bulk_string("local"),
                Frame::bulk_string("routing"),
                Frame::bulk_string(self.fulltext_config_string("CLUSTER_ROUTING", "local")?),
                Frame::bulk_string("alias_propagation"),
                Frame::bulk_string(
                    self.fulltext_config_string("CLUSTER_ALIAS_PROPAGATION", "local")?,
                ),
                Frame::bulk_string("config_propagation"),
                Frame::bulk_string(
                    self.fulltext_config_string("CLUSTER_CONFIG_PROPAGATION", "local")?,
                ),
                Frame::bulk_string("vector_merge"),
                Frame::bulk_string(self.fulltext_config_string("CLUSTER_VECTOR_MERGE", "local")?),
                Frame::bulk_string("router_state"),
                Frame::bulk_string(if cluster_enabled && cluster_shards > 1 {
                    "unsupported"
                } else {
                    "local"
                }),
                Frame::bulk_string("merge_policy"),
                Frame::bulk_string("score_desc_key_asc"),
            ]),
            Frame::bulk_string("gc_stats"),
            Frame::Array(vec![
                Frame::bulk_string("bytes_collected"),
                Frame::Integer(0),
                Frame::bulk_string("total_ms_run"),
                Frame::Integer(0),
            ]),
            Frame::bulk_string("cursor_stats"),
            Frame::Array(vec![
                Frame::bulk_string("global_idle"),
                Frame::Integer(0),
                Frame::bulk_string("global_total"),
                Frame::Integer(0),
            ]),
            Frame::bulk_string("dialect_stats"),
            Frame::Array(vec![
                Frame::bulk_string("dialect_1"),
                Frame::Integer(0),
                Frame::bulk_string("dialect_2"),
                Frame::Integer(0),
                Frame::bulk_string("dialect_3"),
                Frame::Integer(0),
                Frame::bulk_string("dialect_4"),
                Frame::Integer(0),
            ]),
        ]))
    }

    pub async fn fulltext_info_async(&self, index: &str) -> Result<Frame, Error> {
        self.fulltext_info(index)
    }
}
