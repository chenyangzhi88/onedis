impl Db {
    fn fulltext_exact_filter_hits(
        &self,
        meta: &FullTextIndexMeta,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
    ) -> Result<Vec<FullTextLiveHit>, Error> {
        let keys = self.fulltext_source_keys(meta)?;
        let mut live = Vec::new();
        for key in keys {
            if options
                .in_keys
                .as_ref()
                .is_some_and(|keys| !keys.contains(&key))
            {
                continue;
            }
            let Some(hit) = self.fulltext_live_hit_from_source(meta, options, key, 1.0)? else {
                continue;
            };
            if fulltext_eval_ast_against_fields(ast, &hit.fields, meta, options)? {
                live.push(hit);
            }
        }
        Ok(live)
    }

    fn fulltext_source_keys(&self, meta: &FullTextIndexMeta) -> Result<Vec<String>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => self.fulltext_matching_hash_keys(meta),
            FullTextSourceType::Json => {
                let mut keys = HashSet::new();
                for prefix in &meta.prefixes {
                    for (raw_key, raw_value) in
                        self.store.scan_prefix_raw(&main_key(self.db_index, prefix))
                    {
                        if raw_key.len() < 2 || Self::decode_json_meta(&raw_value).is_err() {
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
        }
    }

    fn fulltext_live_hit_from_source(
        &self,
        meta: &FullTextIndexMeta,
        options: &FullTextSearchOptions,
        key: String,
        score: f32,
    ) -> Result<Option<FullTextLiveHit>, Error> {
        match meta.source_type {
            FullTextSourceType::Hash => {
                let fields = self.hash_get_all(&key)?;
                if fields.is_empty()
                    || !fulltext_fields_match_filters(&fields, &options.filters)
                    || !fulltext_fields_match_geo_filters(&fields, &options.geo_filters)?
                {
                    return Ok(None);
                }
                let sort_key = options
                    .sort_by
                    .as_ref()
                    .and_then(|sort_by| fulltext_field_value(&fields, &sort_by.field));
                Ok(Some(FullTextLiveHit {
                    key,
                    score,
                    fields,
                    sort_key,
                }))
            }
            FullTextSourceType::Json => {
                let Some(filter_fields) = self.fulltext_json_fields(&key, meta)? else {
                    return Ok(None);
                };
                if !fulltext_fields_match_filters(&filter_fields, &options.filters) {
                    return Ok(None);
                }
                if !fulltext_fields_match_geo_filters(&filter_fields, &options.geo_filters)? {
                    return Ok(None);
                }
                let sort_key = options
                    .sort_by
                    .as_ref()
                    .and_then(|sort_by| fulltext_field_value(&filter_fields, &sort_by.field));
                let fields = self.fulltext_json_return_fields(&key, meta, options.dialect)?;
                Ok(Some(FullTextLiveHit {
                    key,
                    score,
                    fields,
                    sort_key,
                }))
            }
        }
    }
}
