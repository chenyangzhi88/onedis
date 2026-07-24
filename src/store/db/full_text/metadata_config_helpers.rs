use super::*;
impl Db {
    pub(super) fn next_fulltext_sequence(&self) -> u64 {
        self.next_persisted_version()
    }

    pub(super) fn resolve_fulltext_index(&self, index_or_alias: &str) -> Result<String, Error> {
        if let Some(raw) = self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, index_or_alias))
        {
            let meta = decode_fulltext_meta(&raw)?;
            if matches!(meta.state, FullTextIndexState::Dropping) {
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            if self.fulltext_index_expired(index_or_alias, &meta) {
                self.fulltext_purge_index(index_or_alias, &meta)?;
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            self.fulltext_touch_temporary_index(index_or_alias, &meta);
            return Ok(index_or_alias.to_string());
        }
        if let Some(alias) = self.read_fulltext_alias(index_or_alias)? {
            let meta = self.read_fulltext_meta_direct(&alias.index)?;
            if matches!(meta.state, FullTextIndexState::Dropping) {
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            if self.fulltext_index_expired(&alias.index, &meta) {
                self.fulltext_purge_index(&alias.index, &meta)?;
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            self.fulltext_touch_temporary_index(&alias.index, &meta);
            return Ok(alias.index);
        }
        Err(Error::msg("ERR fulltext index does not exist"))
    }

    pub(super) fn read_fulltext_meta_direct(
        &self,
        index: &str,
    ) -> Result<FullTextIndexMeta, Error> {
        self.read_fulltext_meta_versioned(index)
            .map(|(meta, _)| meta)
    }

    pub(super) fn read_fulltext_meta_versioned(
        &self,
        index: &str,
    ) -> Result<(FullTextIndexMeta, Vec<u8>), Error> {
        let Some(raw) = self.store.get_raw(&fulltext_meta_key(self.db_index, index)) else {
            return Err(Error::msg("ERR fulltext index does not exist"));
        };
        Ok((decode_fulltext_meta(&raw)?, raw))
    }

    pub(super) fn fulltext_write_meta_cas(
        &self,
        index: &str,
        expected_raw: &[u8],
        meta: &mut FullTextIndexMeta,
        batch: &mut WriteBatch,
    ) -> Result<Vec<u8>, Error> {
        meta.revision = meta.revision.saturating_add(1);
        let encoded = encode_record(meta)?;
        batch.put(&fulltext_meta_key(self.db_index, index), &encoded);
        self.fulltext_compare_and_write(index, Some(expected_raw), batch)?;
        Ok(encoded)
    }

    pub(super) fn fulltext_compare_and_write(
        &self,
        index: &str,
        expected_raw: Option<&[u8]>,
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        let key = fulltext_meta_key(self.db_index, index);
        let condition = match expected_raw {
            Some(raw) => CompareCondition::exists_with(&key, raw),
            None => CompareCondition::absent(&key),
        };
        match self.store.compare_and_write_batch(&[condition], batch) {
            Ok(()) => Ok(()),
            Err(Status::ConditionFailed(_)) => {
                Err(Error::msg("ERR fulltext index changed concurrently"))
            }
            Err(error) => Err(Error::msg(error.to_string())),
        }
    }

    pub(super) fn fulltext_compare_conditions(
        &self,
        conditions: &[CompareCondition],
        batch: &WriteBatch,
    ) -> Result<(), Error> {
        match self.store.compare_and_write_batch(conditions, batch) {
            Ok(()) => Ok(()),
            Err(Status::ConditionFailed(_)) => {
                Err(Error::msg("ERR fulltext metadata changed concurrently"))
            }
            Err(error) => Err(Error::msg(error.to_string())),
        }
    }

    pub(super) fn read_fulltext_alias(
        &self,
        alias: &str,
    ) -> Result<Option<FullTextAliasMeta>, Error> {
        self.store
            .get_raw(&fulltext_alias_key(self.db_index, alias))
            .map(|raw| decode_record::<FullTextAliasMeta>(&raw))
            .transpose()
    }

    pub(super) fn fulltext_matching_metas_for_source(
        &self,
        key: &str,
        source_type: FullTextSourceType,
    ) -> Result<Vec<(String, FullTextIndexMeta)>, Error> {
        let mut matches = Vec::new();
        let routes = self.fulltext_source_routes()?;
        for route in routes.iter().filter(|route| {
            route.source_type == source_type
                && route.prefixes.iter().any(|prefix| key.starts_with(prefix))
        }) {
            let Ok(meta) = self.read_fulltext_meta_direct(&route.index) else {
                continue;
            };
            if self.fulltext_index_expired(&route.index, &meta)
                || matches!(meta.state, FullTextIndexState::Dropping)
            {
                continue;
            }
            self.fulltext_touch_temporary_index(&route.index, &meta);
            matches.push((route.index.clone(), meta));
        }
        Ok(matches)
    }

    pub(super) fn fulltext_source_routes(&self) -> Result<Arc<Vec<FullTextSourceRoute>>, Error> {
        if let Some(routes) = self.fulltext_runtimes.source_routes(self.db_index) {
            return Ok(routes);
        }
        let routes = self
            .read_all_fulltext_metas()?
            .into_iter()
            .filter(|(_, meta)| !matches!(meta.state, FullTextIndexState::Dropping))
            .map(|(index, meta)| FullTextSourceRoute {
                index,
                source_type: meta.source_type,
                prefixes: meta.prefixes,
            })
            .collect::<Vec<_>>();
        self.fulltext_runtimes
            .set_source_routes(self.db_index, routes);
        self.fulltext_runtimes
            .source_routes(self.db_index)
            .ok_or_else(|| Error::msg("ERR failed to initialize fulltext source routes"))
    }

    pub(super) fn fulltext_invalidate_source_routes(&self) {
        self.fulltext_runtimes
            .invalidate_source_routes(self.db_index);
    }

    pub(super) fn fulltext_pending_outbox_count(&self, index: &str) -> u64 {
        if let Some(pending) = self.fulltext_runtimes.outbox_pending(self.db_index, index) {
            return pending;
        }
        let pending = self.store.scan_range_raw_visit(
            &fulltext_outbox_prefix(self.db_index, index),
            prefix_exclusive_upper_bound(&fulltext_outbox_prefix(self.db_index, index)),
            usize::MAX,
            |_, _| true,
        ) as u64;
        self.fulltext_runtimes
            .set_outbox_pending(self.db_index, index, pending);
        pending
    }

    pub(super) fn fulltext_source_keys_page(
        &self,
        meta: &FullTextIndexMeta,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, bool), Error> {
        let source_type_tag = match meta.source_type {
            FullTextSourceType::Hash => TYPE_HASH,
            FullTextSourceType::Json => TYPE_JSON,
        };
        self.fulltext_source_keys_page_for_type(meta, source_type_tag, after, limit)
    }

    pub(super) fn fulltext_source_keys_page_for_type(
        &self,
        meta: &FullTextIndexMeta,
        source_type_tag: u8,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, bool), Error> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let target = limit.saturating_add(1);
        let mut keys = BTreeSet::new();
        let mut any_prefix_has_more = false;
        let mut overflowed = false;
        for prefix in &meta.prefixes {
            let (page, has_more) =
                self.fulltext_source_prefix_page(prefix, source_type_tag, after, target)?;
            for key in page {
                if keys.insert(key) && keys.len() > target {
                    keys.pop_last();
                    overflowed = true;
                }
            }
            any_prefix_has_more |= has_more;
        }
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        let has_more = keys.len() > limit || any_prefix_has_more || overflowed;
        keys.truncate(limit);
        Ok((keys, has_more))
    }

    pub(super) fn fulltext_source_prefix_page(
        &self,
        prefix: &str,
        source_type_tag: u8,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, bool), Error> {
        const RAW_SCAN_CHUNK: usize = 256;
        let storage_base = self.mk("");
        let prefix_start = self.mk(prefix);
        let upper = prefix_exclusive_upper_bound(&prefix_start);
        let mut lower = after
            .map(|cursor| {
                let mut lower = self.mk(cursor);
                lower.push(0);
                lower
            })
            .filter(|cursor| cursor.as_slice() > prefix_start.as_slice())
            .unwrap_or(prefix_start);
        let mut keys = Vec::new();
        let mut exhausted = false;
        while keys.len() < limit && !exhausted {
            let entries = self
                .store
                .scan_range_raw_limited(&lower, upper.clone(), RAW_SCAN_CHUNK);
            if entries.is_empty() {
                exhausted = true;
                break;
            }
            let entry_count = entries.len();
            let mut last_raw_key = None;
            let mut stopped_at_page_limit = false;
            for (raw_key, raw_value) in entries {
                last_raw_key = Some(raw_key.clone());
                let Some(encoded_key) =
                    logical_main_key_from_raw_key(self.key_layout, self.db_index, &raw_key)
                else {
                    continue;
                };
                let Some(logical_key) = encoded_key.strip_prefix(storage_base.as_slice()) else {
                    continue;
                };
                let Ok(key) = String::from_utf8(logical_key.to_vec()) else {
                    continue;
                };
                if !key.starts_with(prefix) {
                    continue;
                }
                let Some(header) = decode_meta_header(&raw_value) else {
                    continue;
                };
                if header.type_tag != source_type_tag {
                    continue;
                }
                if header.expire_ms > 0 && current_fulltext_millis() >= header.expire_ms {
                    self.expire_if_needed(&key);
                    continue;
                }
                if after.is_none_or(|cursor| key.as_str() > cursor) {
                    keys.push(key);
                    if keys.len() >= limit {
                        stopped_at_page_limit = true;
                        break;
                    }
                }
            }
            exhausted = !stopped_at_page_limit && entry_count < RAW_SCAN_CHUNK;
            if let Some(mut last_raw_key) = last_raw_key {
                last_raw_key.push(0);
                if upper
                    .as_ref()
                    .is_some_and(|upper| last_raw_key.as_slice() >= upper.as_slice())
                {
                    exhausted = true;
                } else {
                    lower = last_raw_key;
                }
            } else {
                exhausted = true;
            }
        }
        Ok((keys, !exhausted))
    }

    pub(super) fn fulltext_aliases_for_index(&self, index: &str) -> Result<Vec<String>, Error> {
        let mut aliases = Vec::new();
        for (key, raw) in self
            .store
            .scan_prefix_raw(&fulltext_alias_prefix(self.db_index))
        {
            let Some(alias) = fulltext_alias_from_key(self.db_index, &key) else {
                continue;
            };
            let meta = decode_record::<FullTextAliasMeta>(&raw)?;
            if meta.index == index {
                aliases.push(alias);
            }
        }
        Ok(aliases)
    }

    pub(super) fn delete_fulltext_index_storage_to_batch(
        &self,
        batch: &mut WriteBatch,
        index: &str,
    ) {
        self.delete_fulltext_storage_to_batch(batch, index);
        delete_prefix_to_batch(
            batch,
            &self.store,
            &fulltext_outbox_prefix(self.db_index, index),
        );
    }

    pub(super) fn delete_fulltext_storage_to_batch(
        &self,
        batch: &mut WriteBatch,
        storage_name: &str,
    ) {
        delete_prefix_to_batch(
            batch,
            &self.store,
            &fulltext_file_prefix(self.db_index, storage_name),
        );
    }

    pub(super) fn fulltext_active_storage_name(
        &self,
        index: &str,
        meta: &FullTextIndexMeta,
    ) -> String {
        let _ = index;
        meta.active_storage.clone()
    }

    pub(super) fn fulltext_config_value(&self, name: &str) -> Result<Option<String>, Error> {
        self.store
            .get_raw(&fulltext_config_key(self.db_index, name))
            .map(|raw| {
                String::from_utf8(raw)
                    .map_err(|_| Error::msg("ERR failed to decode fulltext config"))
            })
            .transpose()
    }

    pub(super) fn fulltext_effective_search_options(
        &self,
        mut options: FullTextSearchOptions,
    ) -> Result<FullTextSearchOptions, Error> {
        if !options.dialect_explicit {
            let dialect = self
                .fulltext_config_value("DEFAULT_DIALECT")?
                .unwrap_or_else(|| {
                    fulltext_default_config_value("DEFAULT_DIALECT")
                        .unwrap_or("2")
                        .to_string()
                })
                .parse::<u8>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?;
            if (1..=4).contains(&dialect) {
                options.dialect = dialect;
            }
        }
        if let Some(language) = options.language.as_deref() {
            options.language = Some(normalize_fulltext_language(language)?);
        }
        if options.timeout_ms.is_none() {
            options.timeout_ms = Some(self.fulltext_config_u64("TIMEOUT", 500)?);
        }
        let max_results = self.fulltext_config_usize("MAXSEARCHRESULTS", 10_000)?;
        if options.offset.saturating_add(options.limit) > max_results {
            return Err(Error::msg("ERR fulltext search result limit exceeded"));
        }
        let max_expansions = self.fulltext_config_usize("MAXEXPANSIONS", 200)?;
        if options.filters.len() > max_expansions
            || options.geo_filters.len() > max_expansions
            || options
                .in_keys
                .as_ref()
                .is_some_and(|keys| keys.len() > max_expansions)
            || options
                .in_fields
                .as_ref()
                .is_some_and(|fields| fields.len() > max_expansions)
        {
            return Err(Error::msg("ERR fulltext query expansion limit exceeded"));
        }
        Ok(options)
    }

    pub(super) fn fulltext_runtime_config(&self) -> Result<FullTextRuntimeConfig, Error> {
        Ok(FullTextRuntimeConfig {
            writer_heap_bytes: self
                .fulltext_config_usize("MEMORY_BUDGET_WRITER_BYTES", FULLTEXT_WRITER_HEAP_BYTES)?,
            min_prefix: self.fulltext_config_usize("MINPREFIX", 2)?,
            max_expansions: self.fulltext_config_usize("MAXEXPANSIONS", 200)?,
            max_prefix_expansions: self
                .fulltext_config_u64("MAXPREFIXEXPANSIONS", 200)?
                .try_into()
                .map_err(|_| Error::msg("ERR invalid fulltext config value"))?,
        })
    }

    pub(super) fn fulltext_effective_refresh_policy(
        &self,
        meta: &FullTextIndexMeta,
    ) -> Result<FullTextRefreshPolicy, Error> {
        Ok(FullTextRefreshPolicy {
            max_docs: self
                .fulltext_config_usize("REFRESH_MAX_DOCS", meta.refresh_policy.max_docs)?,
            max_bytes: self
                .fulltext_config_usize("REFRESH_MAX_BYTES", meta.refresh_policy.max_bytes)?,
            refresh_interval_ms: self.fulltext_config_u64(
                "REFRESH_INTERVAL_MS",
                meta.refresh_policy.refresh_interval_ms,
            )?,
        })
    }

    pub(super) fn fulltext_refresh_timeout_ms(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("REFRESH_TIMEOUT_MS", DEFAULT_REFRESH_TIMEOUT_MS)
    }

    pub(super) fn fulltext_search_refresh_timeout_ms(
        &self,
        search_timeout_ms: u64,
    ) -> Result<u64, Error> {
        match self.fulltext_config_value("REFRESH_TIMEOUT_MS")? {
            Some(value) => value
                .parse::<u64>()
                .map_err(|_| Error::msg("ERR invalid fulltext config value")),
            None => Ok(DEFAULT_REFRESH_TIMEOUT_MS.max(search_timeout_ms)),
        }
    }

    pub(super) fn fulltext_outbox_compact_threshold(&self) -> Result<usize, Error> {
        self.fulltext_config_usize("OUTBOX_COMPACT_THRESHOLD", DEFAULT_OUTBOX_COMPACT_THRESHOLD)
    }

    pub(super) fn fulltext_repair_throttle_ms(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("REPAIR_THROTTLE_MS", DEFAULT_REPAIR_THROTTLE_MS)
    }

    pub(super) fn fulltext_cluster_enabled(&self) -> Result<bool, Error> {
        self.fulltext_config_bool("CLUSTER_ENABLED", false)
    }

    pub(super) fn fulltext_cluster_shards(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("CLUSTER_SHARDS", 1)
    }

    pub(super) fn fulltext_cluster_shard_id(&self) -> Result<u64, Error> {
        let shard_id = self.fulltext_config_u64("CLUSTER_SHARD_ID", 0)?;
        let shards = self.fulltext_cluster_shards()?;
        if shard_id < shards {
            Ok(shard_id)
        } else {
            Err(Error::msg("ERR invalid fulltext cluster shard id"))
        }
    }

    pub(super) fn fulltext_reject_cluster_multi_shard(&self, command: &str) -> Result<(), Error> {
        self.fulltext_cluster_shard_id()?;
        if self.fulltext_cluster_enabled()? && self.fulltext_cluster_shards()? > 1 {
            return Err(Error::msg(format!(
                "ERR {command} is not supported with multiple fulltext shards"
            )));
        }
        Ok(())
    }

    pub(super) fn fulltext_config_u64(&self, name: &str, default: u64) -> Result<u64, Error> {
        self.fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string())
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR invalid fulltext config value"))
    }

    pub(super) fn fulltext_config_usize(&self, name: &str, default: usize) -> Result<usize, Error> {
        self.fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string())
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR invalid fulltext config value"))
    }

    pub(super) fn fulltext_config_bool(&self, name: &str, default: bool) -> Result<bool, Error> {
        let value = self
            .fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string());
        match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(Error::msg("ERR invalid fulltext config value")),
        }
    }

    pub(super) fn fulltext_config_string(
        &self,
        name: &str,
        default: &str,
    ) -> Result<String, Error> {
        Ok(self
            .fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string()))
    }

    pub(super) fn fulltext_index_expired(&self, index: &str, meta: &FullTextIndexMeta) -> bool {
        let Some(seconds) = meta.index_options.temporary_seconds else {
            return false;
        };
        let last_activity_ms = self
            .store
            .get_raw(&fulltext_temporary_activity_key(self.db_index, index))
            .and_then(|raw| raw.try_into().ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        current_fulltext_millis() >= last_activity_ms.saturating_add(seconds.saturating_mul(1_000))
    }

    pub(super) fn fulltext_touch_temporary_index(&self, index: &str, meta: &FullTextIndexMeta) {
        if meta.index_options.temporary_seconds.is_none() {
            return;
        }
        let mut batch = WriteBatch::new();
        batch.put(
            &fulltext_temporary_activity_key(self.db_index, index),
            &current_fulltext_millis().to_be_bytes(),
        );
        self.write_batch_if_not_empty(&batch);
    }

    pub(super) fn fulltext_file_bytes(&self, index: &str) -> usize {
        KvTantivyDirectory::storage_bytes(&self.store, self.db_index, index)
    }

    pub(super) fn read_all_fulltext_metas(
        &self,
    ) -> Result<Vec<(String, FullTextIndexMeta)>, Error> {
        let mut metas = Vec::new();
        for (key, raw) in self
            .store
            .scan_prefix_raw(&fulltext_meta_prefix(self.db_index))
        {
            let Some(index) = fulltext_index_from_meta_key(self.db_index, &key) else {
                continue;
            };
            metas.push((index, decode_fulltext_meta(&raw)?));
        }
        Ok(metas)
    }

    pub(super) fn fulltext_dict_terms(&self, dict: &str) -> Result<HashSet<String>, Error> {
        Ok(self
            .store
            .scan_prefix_raw(&fulltext_dict_prefix(self.db_index, dict))
            .into_iter()
            .filter_map(|(key, _)| fulltext_dict_term_from_key(self.db_index, dict, &key))
            .collect())
    }

    pub(super) fn fulltext_index_vocabulary(&self, index: &str) -> Result<HashSet<String>, Error> {
        let meta = self.read_fulltext_meta_direct(index)?;
        let mut out = HashSet::new();
        let reader_budget = self.fulltext_config_usize("MEMORY_BUDGET_READER_BYTES", 67_108_864)?;
        let mut used = 0usize;
        let mut cursor = None;
        loop {
            let (keys, has_more) = self.fulltext_source_keys_page(&meta, cursor.as_deref(), 256)?;
            for key in keys {
                cursor = Some(key.clone());
                let values = match meta.source_type {
                    FullTextSourceType::Hash => {
                        let mut values = Vec::new();
                        for (field, value) in self.hash_get_all(&key)? {
                            if meta.schema.iter().any(|schema| {
                                matches!(schema.kind, FullTextFieldKind::Text)
                                    && (schema.name == field || schema.attribute_name() == field)
                            }) {
                                values.push(value);
                            }
                        }
                        values
                    }
                    FullTextSourceType::Json => self
                        .fulltext_json_fields(&key, &meta)?
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(_, value)| value)
                        .collect(),
                };
                for value in values {
                    for token in fulltext_tokenize(&value) {
                        if out.contains(&token) {
                            continue;
                        }
                        used = used.saturating_add(
                            std::mem::size_of::<String>()
                                .saturating_add(token.len())
                                .saturating_add(2 * std::mem::size_of::<usize>()),
                        );
                        if used > reader_budget {
                            return Err(Error::msg("ERR fulltext reader memory limit exceeded"));
                        }
                        out.insert(token);
                    }
                }
            }
            if !has_more {
                break;
            }
            if cursor.is_none() {
                return Err(Error::msg("ERR fulltext source scan made no progress"));
            }
        }
        Ok(out)
    }
}
