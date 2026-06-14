impl Db {
    pub fn fulltext_dict_add(&self, dict: &str, terms: Vec<String>) -> Result<Frame, Error> {
        let mut inserted = 0i64;
        let mut batch = WriteBatch::new();
        for term in terms {
            let normalized = term.to_lowercase();
            let key = fulltext_dict_term_key(self.db_index, dict, &normalized);
            if self.store.get_raw(&key).is_none() {
                inserted += 1;
            }
            batch.put(&key, normalized.as_bytes());
        }
        self.write_batch_if_not_empty(&batch);
        Ok(Frame::Integer(inserted))
    }

    pub async fn fulltext_dict_add_async(
        &self,
        dict: &str,
        terms: Vec<String>,
    ) -> Result<Frame, Error> {
        self.fulltext_dict_add(dict, terms)
    }

    pub fn fulltext_dict_del(&self, dict: &str, terms: Vec<String>) -> Result<Frame, Error> {
        let mut deleted = 0i64;
        let mut batch = WriteBatch::new();
        for term in terms {
            let key = fulltext_dict_term_key(self.db_index, dict, &term.to_lowercase());
            if self.store.get_raw(&key).is_some() {
                deleted += 1;
                batch.delete(&key);
            }
        }
        self.write_batch_if_not_empty(&batch);
        Ok(Frame::Integer(deleted))
    }

    pub async fn fulltext_dict_del_async(
        &self,
        dict: &str,
        terms: Vec<String>,
    ) -> Result<Frame, Error> {
        self.fulltext_dict_del(dict, terms)
    }

    pub fn fulltext_dict_dump(&self, dict: &str) -> Result<Frame, Error> {
        Ok(Frame::Array(
            self.fulltext_dict_terms(dict)?
                .into_iter()
                .map(Frame::bulk_string)
                .collect(),
        ))
    }

    pub async fn fulltext_dict_dump_async(&self, dict: &str) -> Result<Frame, Error> {
        self.fulltext_dict_dump(dict)
    }

    pub fn fulltext_spellcheck(
        &self,
        index: &str,
        query: &str,
        distance: usize,
        include: Vec<String>,
        exclude: Vec<String>,
    ) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let mut vocabulary = self.fulltext_index_vocabulary(&index)?;
        if include.is_empty() {
            vocabulary.extend(self.fulltext_all_dict_terms()?);
        } else {
            for dict in include {
                vocabulary.extend(self.fulltext_dict_terms(&dict)?);
            }
        }
        for dict in exclude {
            for term in self.fulltext_dict_terms(&dict)? {
                vocabulary.remove(&term);
            }
        }
        let mut out = Vec::new();
        for token in fulltext_tokenize(query) {
            if vocabulary.contains(&token) {
                continue;
            }
            let mut suggestions = vocabulary
                .iter()
                .filter_map(|candidate| {
                    let dist = fulltext_edit_distance(&token, candidate);
                    if dist <= distance {
                        Some((dist, candidate.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            suggestions
                .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
            suggestions.truncate(5);
            if suggestions.is_empty() {
                continue;
            }
            out.push(Frame::Array(vec![
                Frame::bulk_string(token),
                Frame::Array(
                    suggestions
                        .into_iter()
                        .map(|(dist, term)| {
                            Frame::Array(vec![
                                Frame::bulk_string(format_fulltext_spellcheck_score(dist)),
                                Frame::bulk_string(term),
                            ])
                        })
                        .collect(),
                ),
            ]));
        }
        Ok(Frame::Array(out))
    }

    pub async fn fulltext_spellcheck_async(
        &self,
        index: &str,
        query: &str,
        distance: usize,
        include: Vec<String>,
        exclude: Vec<String>,
    ) -> Result<Frame, Error> {
        self.fulltext_spellcheck(index, query, distance, include, exclude)
    }

    pub fn fulltext_sugadd(
        &self,
        key: &str,
        string: &str,
        score: f64,
        incr: bool,
        payload: Option<String>,
    ) -> Result<Frame, Error> {
        if !score.is_finite() {
            return Err(Error::msg("ERR invalid suggestion score"));
        }
        let storage_key = fulltext_suggest_key(self.db_index, key, string);
        let existed = self.store.get_raw(&storage_key);
        let old = existed
            .as_ref()
            .map(|raw| decode_record::<FullTextSuggestRecord>(raw))
            .transpose()?;
        let record = FullTextSuggestRecord {
            score: if incr {
                old.as_ref().map(|record| record.score).unwrap_or(0.0) + score
            } else {
                score
            },
            payload: payload.or_else(|| old.and_then(|record| record.payload)),
        };
        let mut batch = WriteBatch::new();
        batch.put(&storage_key, &encode_record(&record)?);
        self.write_batch_if_not_empty(&batch);
        Ok(Frame::Integer(if existed.is_some() { 0 } else { 1 }))
    }

    pub async fn fulltext_sugadd_async(
        &self,
        key: &str,
        string: &str,
        score: f64,
        incr: bool,
        payload: Option<String>,
    ) -> Result<Frame, Error> {
        self.fulltext_sugadd(key, string, score, incr, payload)
    }

    pub fn fulltext_sugget(
        &self,
        key: &str,
        prefix: &str,
        fuzzy: bool,
        with_scores: bool,
        with_payloads: bool,
        max: usize,
    ) -> Result<Frame, Error> {
        let prefix_norm = prefix.to_lowercase();
        let mut entries = Vec::new();
        for (raw_key, raw) in self
            .store
            .scan_prefix_raw(&fulltext_suggest_prefix(self.db_index, key))
        {
            let Some(string) = fulltext_suggest_string_from_key(self.db_index, key, &raw_key)
            else {
                continue;
            };
            let string_norm = string.to_lowercase();
            if !string_norm.starts_with(&prefix_norm)
                && !(fuzzy && fulltext_edit_distance(&prefix_norm, &string_norm) <= 1)
            {
                continue;
            }
            entries.push((string, decode_record::<FullTextSuggestRecord>(&raw)?));
        }
        entries.sort_by(|left, right| {
            right
                .1
                .score
                .partial_cmp(&left.1.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut out = Vec::new();
        for (string, record) in entries.into_iter().take(max.max(1)) {
            out.push(Frame::bulk_string(string));
            if with_scores {
                out.push(Frame::bulk_string(format_fulltext_suggestion_score(
                    record.score,
                )));
            }
            if with_payloads {
                out.push(
                    record
                        .payload
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                );
            }
        }
        Ok(Frame::Array(out))
    }

    pub async fn fulltext_sugget_async(
        &self,
        key: &str,
        prefix: &str,
        fuzzy: bool,
        with_scores: bool,
        with_payloads: bool,
        max: usize,
    ) -> Result<Frame, Error> {
        self.fulltext_sugget(key, prefix, fuzzy, with_scores, with_payloads, max)
    }

    pub fn fulltext_sugdel(&self, key: &str, string: &str) -> Result<Frame, Error> {
        let storage_key = fulltext_suggest_key(self.db_index, key, string);
        let existed = self.store.get_raw(&storage_key).is_some();
        if existed {
            let mut batch = WriteBatch::new();
            batch.delete(&storage_key);
            self.write_batch_if_not_empty(&batch);
        }
        Ok(Frame::Integer(i64::from(existed)))
    }

    pub async fn fulltext_sugdel_async(&self, key: &str, string: &str) -> Result<Frame, Error> {
        self.fulltext_sugdel(key, string)
    }

    pub fn fulltext_suglen(&self, key: &str) -> Result<Frame, Error> {
        Ok(Frame::Integer(
            self.store
                .scan_prefix_raw(&fulltext_suggest_prefix(self.db_index, key))
                .len() as i64,
        ))
    }

    pub async fn fulltext_suglen_async(&self, key: &str) -> Result<Frame, Error> {
        self.fulltext_suglen(key)
    }

    pub fn fulltext_synupdate(
        &self,
        index: &str,
        group: &str,
        terms: Vec<String>,
    ) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        if terms.is_empty() {
            return Err(Error::msg("ERR SYNUPDATE requires terms"));
        }
        let record = FullTextSynonymGroup {
            terms: terms.into_iter().map(|term| term.to_lowercase()).collect(),
        };
        let mut batch = WriteBatch::new();
        batch.put(
            &fulltext_syn_key(self.db_index, &index, group),
            &encode_record(&record)?,
        );
        self.write_batch_if_not_empty(&batch);
        self.fulltext_runtimes.remove(self.db_index, &index);
        Ok(Frame::Ok)
    }

    pub async fn fulltext_synupdate_async(
        &self,
        index: &str,
        group: &str,
        terms: Vec<String>,
    ) -> Result<Frame, Error> {
        let _ = group;
        self.fulltext_synupdate(index, group, terms)
    }

    pub fn fulltext_syndump(&self, index: &str) -> Result<Frame, Error> {
        let index = self.resolve_fulltext_index(index)?;
        let mut groups = Vec::new();
        for (raw_key, raw) in self
            .store
            .scan_prefix_raw(&fulltext_syn_prefix(self.db_index, &index))
        {
            let Some(group) = fulltext_syn_group_from_key(self.db_index, &index, &raw_key) else {
                continue;
            };
            let record = decode_record::<FullTextSynonymGroup>(&raw)?;
            groups.push((group, record.terms));
        }
        groups.sort_by(|left, right| left.0.cmp(&right.0));
        let mut out = Vec::new();
        for (group, terms) in groups {
            out.push(Frame::bulk_string(group));
            out.push(Frame::Array(
                terms.into_iter().map(Frame::bulk_string).collect(),
            ));
        }
        Ok(Frame::Array(out))
    }

    pub async fn fulltext_syndump_async(&self, index: &str) -> Result<Frame, Error> {
        self.fulltext_syndump(index)
    }


}
