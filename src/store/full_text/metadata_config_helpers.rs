impl Db {
    fn resolve_fulltext_index(&self, index_or_alias: &str) -> Result<String, Error> {
        if self
            .store
            .get_raw(&fulltext_meta_key(self.db_index, index_or_alias))
            .is_some()
        {
            return Ok(index_or_alias.to_string());
        }
        if let Some(alias) = self.read_fulltext_alias(index_or_alias)? {
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
        Ok(self
            .read_all_fulltext_metas()?
            .into_iter()
            .filter(|(_, meta)| {
                meta.source_type == source_type
                    && meta.prefixes.iter().any(|prefix| key.starts_with(prefix))
            })
            .collect())
    }

    fn fulltext_matching_hash_keys(&self, meta: &FullTextIndexMeta) -> Result<Vec<String>, Error> {
        let mut keys = HashSet::new();
        for prefix in &meta.prefixes {
            for (raw_key, raw_value) in self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
            {
                if raw_key.len() < 2 || decode_hash_meta_checked(&raw_value).is_err() {
                    continue;
                }
                let key = String::from_utf8_lossy(&raw_key[2..]).to_string();
                if key.starts_with(prefix) {
                    keys.insert(key);
                }
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
        if self.fulltext_cluster_enabled()? && self.fulltext_cluster_shards()? > 1 {
            Err(Error::msg(format!(
                "ERR {command} requires fulltext cluster routing, which is not implemented"
            )))
        } else {
            Ok(())
        }
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

    fn fulltext_file_bytes(&self, index: &str) -> usize {
        self.store
            .scan_prefix_raw(&fulltext_file_prefix(self.db_index, index))
            .into_iter()
            .map(|(key, value)| key.len() + value.len())
            .sum()
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
                for prefix in &meta.prefixes {
                    for (raw_key, raw_value) in
                        self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
                    {
                        if raw_key.len() < 2 || Self::decode_json_meta(&raw_value).is_err() {
                            continue;
                        }
                        let key = String::from_utf8_lossy(&raw_key[2..]).to_string();
                        if let Some(fields) = self.fulltext_json_fields(&key, &meta)? {
                            for (_, value) in fields {
                                out.extend(fulltext_tokenize(&value));
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

}
