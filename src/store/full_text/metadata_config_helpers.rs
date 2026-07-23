impl Db {
    fn resolve_fulltext_index(&self, index_or_alias: &str) -> Result<String, Error> {
        if let Some(raw) = self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, index_or_alias))
        {
            let meta = decode_fulltext_meta(&raw)?;
            if self.fulltext_index_expired(index_or_alias, &meta) {
                self.fulltext_purge_index(index_or_alias, &meta)?;
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            self.fulltext_touch_temporary_index(index_or_alias, &meta);
            return Ok(index_or_alias.to_string());
        }
        if let Some(alias) = self.read_fulltext_alias(index_or_alias)? {
            let meta = self.read_fulltext_meta_direct(&alias.index)?;
            if self.fulltext_index_expired(&alias.index, &meta) {
                self.fulltext_purge_index(&alias.index, &meta)?;
                return Err(Error::msg("ERR fulltext index does not exist"));
            }
            self.fulltext_touch_temporary_index(&alias.index, &meta);
            return Ok(alias.index);
        }
        Err(Error::msg("ERR fulltext index does not exist"))
    }

    fn read_fulltext_meta_direct(&self, index: &str) -> Result<FullTextIndexMeta, Error> {
        let Some(raw) = self.store.get_raw(&fulltext_meta_key(self.db_index, index)) else {
            return Err(Error::msg("ERR fulltext index does not exist"));
        };
        decode_fulltext_meta(&raw)
    }

    fn read_fulltext_alias(&self, alias: &str) -> Result<Option<FullTextAliasMeta>, Error> {
        self.store
            .get_raw(&fulltext_alias_key(self.db_index, alias))
            .map(|raw| decode_record::<FullTextAliasMeta>(&raw))
            .transpose()
    }

    fn fulltext_matching_metas_for_source(
        &self,
        key: &str,
        source_type: FullTextSourceType,
    ) -> Result<Vec<(String, FullTextIndexMeta)>, Error> {
        let mut matches = Vec::new();
        for (index, meta) in self.read_all_fulltext_metas()? {
            if self.fulltext_index_expired(&index, &meta)
                || meta.source_type != source_type
                || !meta.prefixes.iter().any(|prefix| key.starts_with(prefix))
            {
                continue;
            }
            self.fulltext_touch_temporary_index(&index, &meta);
            matches.push((index, meta));
        }
        Ok(matches)
    }

    fn fulltext_matching_hash_keys(&self, meta: &FullTextIndexMeta) -> Result<Vec<String>, Error> {
        self.fulltext_matching_source_keys(meta, TYPE_HASH)
    }

    fn fulltext_matching_source_keys(
        &self,
        meta: &FullTextIndexMeta,
        source_type_tag: u8,
    ) -> Result<Vec<String>, Error> {
        let mut keys = HashSet::new();
        let storage_base = self.mk("");
        for prefix in &meta.prefixes {
            for (raw_key, raw_value) in self.store.scan_prefix_raw(&self.mk(prefix)) {
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
                keys.insert(key);
            }
        }
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }

    fn fulltext_aliases_for_index(&self, index: &str) -> Result<Vec<String>, Error> {
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

    fn delete_fulltext_index_storage_to_batch(&self, batch: &mut WriteBatch, index: &str) {
        delete_prefix_to_batch(
            batch,
            &self.store,
            &fulltext_file_prefix(self.db_index, index),
        );
        delete_prefix_to_batch(
            batch,
            &self.store,
            &fulltext_legacy_file_prefix(self.db_index, index),
        );
        delete_prefix_to_batch(
            batch,
            &self.store,
            &fulltext_outbox_prefix(self.db_index, index),
        );
    }

    fn fulltext_config_value(&self, name: &str) -> Result<Option<String>, Error> {
        self.store
            .get_raw(&fulltext_config_key(self.db_index, name))
            .map(|raw| {
                String::from_utf8(raw)
                    .map_err(|_| Error::msg("ERR failed to decode fulltext config"))
            })
            .transpose()
    }

    fn fulltext_effective_search_options(
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
        Ok(options)
    }

    fn fulltext_effective_refresh_policy(
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

    fn fulltext_refresh_timeout_ms(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("REFRESH_TIMEOUT_MS", DEFAULT_REFRESH_TIMEOUT_MS)
    }

    fn fulltext_outbox_compact_threshold(&self) -> Result<usize, Error> {
        self.fulltext_config_usize("OUTBOX_COMPACT_THRESHOLD", DEFAULT_OUTBOX_COMPACT_THRESHOLD)
    }

    fn fulltext_repair_throttle_ms(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("REPAIR_THROTTLE_MS", DEFAULT_REPAIR_THROTTLE_MS)
    }

    fn fulltext_cluster_enabled(&self) -> Result<bool, Error> {
        self.fulltext_config_bool("CLUSTER_ENABLED", false)
    }

    fn fulltext_cluster_shards(&self) -> Result<u64, Error> {
        self.fulltext_config_u64("CLUSTER_SHARDS", 1)
    }

    fn fulltext_cluster_shard_id(&self) -> Result<u64, Error> {
        let shard_id = self.fulltext_config_u64("CLUSTER_SHARD_ID", 0)?;
        let shards = self.fulltext_cluster_shards()?;
        if shard_id < shards {
            Ok(shard_id)
        } else {
            Err(Error::msg("ERR invalid fulltext cluster shard id"))
        }
    }

    fn fulltext_reject_cluster_multi_shard(&self, command: &str) -> Result<(), Error> {
        let _ = command;
        self.fulltext_cluster_shard_id()?;
        Ok(())
    }

    fn fulltext_config_u64(&self, name: &str, default: u64) -> Result<u64, Error> {
        self.fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string())
            .parse::<u64>()
            .map_err(|_| Error::msg("ERR invalid fulltext config value"))
    }

    fn fulltext_config_usize(&self, name: &str, default: usize) -> Result<usize, Error> {
        self.fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string())
            .parse::<usize>()
            .map_err(|_| Error::msg("ERR invalid fulltext config value"))
    }

    fn fulltext_config_bool(&self, name: &str, default: bool) -> Result<bool, Error> {
        let value = self
            .fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string());
        match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(Error::msg("ERR invalid fulltext config value")),
        }
    }

    fn fulltext_config_string(&self, name: &str, default: &str) -> Result<String, Error> {
        Ok(self
            .fulltext_config_value(name)?
            .unwrap_or_else(|| default.to_string()))
    }

    fn fulltext_index_expired(&self, index: &str, meta: &FullTextIndexMeta) -> bool {
        let Some(seconds) = meta.index_options.temporary_seconds else {
            return false;
        };
        let last_activity_ms = self
            .store
            .get_raw(&fulltext_temporary_activity_key(self.db_index, index))
            .and_then(|raw| raw.try_into().ok())
            .map(u64::from_be_bytes)
            .unwrap_or(meta.generation >> 16);
        current_fulltext_millis()
            >= last_activity_ms.saturating_add(seconds.saturating_mul(1_000))
    }

    fn fulltext_touch_temporary_index(&self, index: &str, meta: &FullTextIndexMeta) {
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

    fn fulltext_file_bytes(&self, index: &str) -> usize {
        let current = self
            .store
            .scan_prefix_raw(&fulltext_file_prefix(self.db_index, index))
            .into_iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>();
        let legacy = self
            .store
            .scan_prefix_raw(&fulltext_legacy_file_prefix(self.db_index, index))
            .into_iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>();
        current + legacy
    }

    fn read_all_fulltext_metas(&self) -> Result<Vec<(String, FullTextIndexMeta)>, Error> {
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

    fn fulltext_dict_terms(&self, dict: &str) -> Result<HashSet<String>, Error> {
        Ok(self
            .store
            .scan_prefix_raw(&fulltext_dict_prefix(self.db_index, dict))
            .into_iter()
            .filter_map(|(key, _)| fulltext_dict_term_from_key(self.db_index, dict, &key))
            .collect())
    }

    fn fulltext_all_dict_terms(&self) -> Result<HashSet<String>, Error> {
        Ok(self
            .store
            .scan_prefix_raw(&fulltext_dict_root_prefix(self.db_index))
            .into_iter()
            .filter_map(|(key, _)| fulltext_any_dict_term_from_key(self.db_index, &key))
            .collect())
    }

    fn fulltext_index_vocabulary(&self, index: &str) -> Result<HashSet<String>, Error> {
        let meta = self.read_fulltext_meta_direct(index)?;
        let mut out = HashSet::new();
        match meta.source_type {
            FullTextSourceType::Hash => {
                for key in self.fulltext_matching_hash_keys(&meta)? {
                    for (field, value) in self.hash_get_all(&key)? {
                        if meta.schema.iter().any(|schema| {
                            matches!(schema.kind, FullTextFieldKind::Text)
                                && (schema.name == field || schema.attribute_name() == field)
                        }) {
                            out.extend(fulltext_tokenize(&value));
                        }
                    }
                }
            }
            FullTextSourceType::Json => {
                for key in self.fulltext_source_keys(&meta)? {
                    if let Some(fields) = self.fulltext_json_fields(&key, &meta)? {
                        for (_, value) in fields {
                            out.extend(fulltext_tokenize(&value));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

}
