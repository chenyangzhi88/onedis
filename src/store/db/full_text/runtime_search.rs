use super::*;
impl FullTextRuntime {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn search(
        &self,
        query_text: &str,
        options: &FullTextSearchOptions,
        fetch_limit: Option<usize>,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let searcher = self.reader.searcher();
        let query_text = substitute_fulltext_params(query_text, &options.params)?;
        let query = self.build_query(&query_text, options)?;
        self.search_query(query, &searcher, fetch_limit, deadline)
    }

    pub(super) fn search_ast(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        fetch_limit: usize,
        deadline: FullTextSearchDeadline,
    ) -> Result<Vec<FullTextSearchHit>, Error> {
        let result = self.search_ast_hits(ast, options, Some(fetch_limit), deadline)?;
        if result.timed_out && deadline.fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        Ok(result.hits)
    }

    pub(super) fn search_ast_hits(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        fetch_limit: Option<usize>,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let searcher = self.reader.searcher();
        let query = self.plan_query(ast, options.in_fields.as_deref(), options)?;
        let query = self.apply_search_filters(query, options)?;
        self.search_query(query, &searcher, fetch_limit, deadline)
    }

    pub(super) fn search_ast_page_hits(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        fetch_limit: usize,
        key_offset: usize,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let searcher = self.reader.searcher();
        let query = self.plan_query(ast, options.in_fields.as_deref(), options)?;
        let query = self.apply_search_filters(query, options)?;
        self.search_query_window(query, &searcher, fetch_limit, key_offset, deadline)
    }

    pub(super) fn search_sorted_ast(
        &self,
        ast: &FullTextQueryAst,
        options: &FullTextSearchOptions,
        fetch_limit: usize,
        key_offset: usize,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let query = self.plan_query(ast, options.in_fields.as_deref(), options)?;
        let query = self.apply_search_filters(query, options)?;
        self.search_sorted_query(query, options, fetch_limit, key_offset, deadline)
    }

    fn search_sorted_query(
        &self,
        query: Box<dyn Query>,
        options: &FullTextSearchOptions,
        fetch_limit: usize,
        key_offset: usize,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let sort_by = options
            .sort_by
            .as_ref()
            .ok_or_else(|| Error::msg("ERR missing SORTBY"))?;
        let (field, kind) = self
            .sortable_fields
            .get(&sort_by.field)
            .ok_or_else(|| Error::msg("ERR SORTBY field is not SORTABLE"))?;
        let searcher = self.reader.searcher();
        let query = self.with_live_documents_query(query);
        if Instant::now() >= deadline.at {
            if deadline.fail_on_timeout {
                return Err(Error::msg("Timeout limit was reached"));
            }
            return Ok(FullTextSearchHits {
                total: 0,
                hits: Vec::new(),
                timed_out: true,
            });
        }
        let order = if sort_by.asc { Order::Asc } else { Order::Desc };
        let field_name = self.index.schema().get_field_name(*field).to_string();
        let (addresses, total) = match kind {
            FullTextFieldKind::Numeric => {
                let (top, total) = searcher.search(
                    query.as_ref(),
                    &(
                        TopDocs::with_limit(fetch_limit).order_by((
                            (SortByStaticFastValue::<f64>::for_field(&field_name), order),
                            (SortByString::for_field(FULLTEXT_KEY_FIELD), Order::Asc),
                        )),
                        Count,
                    ),
                )?;
                (
                    top.into_iter()
                        .map(|(_, address)| address)
                        .collect::<Vec<_>>(),
                    total,
                )
            }
            FullTextFieldKind::Text | FullTextFieldKind::Tag => {
                let (top, total) = searcher.search(
                    query.as_ref(),
                    &(
                        TopDocs::with_limit(fetch_limit).order_by((
                            (SortByString::for_field(&field_name), order),
                            (SortByString::for_field(FULLTEXT_KEY_FIELD), Order::Asc),
                        )),
                        Count,
                    ),
                )?;
                (
                    top.into_iter()
                        .map(|(_, address)| address)
                        .collect::<Vec<_>>(),
                    total,
                )
            }
            _ => return Err(Error::msg("ERR SORTBY field is not SORTABLE")),
        };
        let timed_out = Instant::now() >= deadline.at;
        if timed_out && deadline.fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        let mut hits = Vec::with_capacity(addresses.len().saturating_sub(key_offset));
        for address in addresses.into_iter().skip(key_offset) {
            if let Some(key) = fulltext_fast_key(&searcher, address)? {
                hits.push(FullTextSearchHit {
                    key,
                    score: 1.0,
                    address,
                });
            }
        }
        Ok(FullTextSearchHits {
            total,
            hits,
            timed_out,
        })
    }

    pub(super) fn search_query(
        &self,
        query: Box<dyn Query>,
        searcher: &tantivy::Searcher,
        fetch_limit: Option<usize>,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        self.search_query_window(
            query,
            searcher,
            fetch_limit.unwrap_or(usize::MAX),
            0,
            deadline,
        )
    }

    fn search_query_window(
        &self,
        query: Box<dyn Query>,
        searcher: &tantivy::Searcher,
        fetch_limit: usize,
        key_offset: usize,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let query = self.with_live_documents_query(query);
        let result = searcher.search(
            query.as_ref(),
            &FullTextDeadlineCollector {
                limit: fetch_limit,
                deadline: deadline.at,
            },
        )?;
        if result.timed_out && deadline.fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        let mut hits = Vec::new();
        for scored in result.top_docs.into_iter().skip(key_offset) {
            let score = scored.score;
            let address = scored.address;
            let Some(key) = fulltext_fast_key(searcher, address)? else {
                continue;
            };
            hits.push(FullTextSearchHit {
                key,
                score,
                address,
            });
        }
        // Tantivy's TopK heap uses the document address as its internal tie
        // breaker. Keep the externally visible RedisSearch ordering stable
        // across refreshes and segment merges by resolving equal scores by key.
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(FullTextSearchHits {
            total: result.total,
            hits,
            timed_out: result.timed_out,
        })
    }

    fn with_live_documents_query(&self, query: Box<dyn Query>) -> Box<dyn Query> {
        // Keep the common no-TTL path as the original query. Wrapping every query in
        // a dynamic range filter prevents Tantivy from using its specialized
        // Block-WAND scorers for score-sorted TopK collection.
        if !self.has_expiring_documents {
            return query;
        }
        // Source TTL is indexed with every published document. Applying the
        // current clock as part of the Tantivy query keeps both `total` and
        // the selected page free of expired documents, without materializing
        // every candidate in the source store.
        let live_query = Box::new(BooleanQuery::new(vec![
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_u64(self.expires_at_field, 0),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            ),
            (
                Occur::Should,
                Box::new(RangeQuery::new(
                    Bound::Excluded(Term::from_field_u64(
                        self.expires_at_field,
                        current_fulltext_millis(),
                    )),
                    Bound::Unbounded,
                )) as Box<dyn Query>,
            ),
        ]));
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::Must, live_query),
        ]))
    }

    pub(super) fn fast_geo_matches(
        &self,
        ast: &FullTextQueryAst,
        address: DocAddress,
    ) -> Result<Option<bool>, Error> {
        let searcher = self.reader.searcher();
        let segment = &searcher.segment_readers()[address.segment_ord as usize];
        self.fast_geo_ast_matches(ast, segment, address.doc_id)
    }

    fn fast_geo_ast_matches(
        &self,
        ast: &FullTextQueryAst,
        segment: &SegmentReader,
        doc: DocId,
    ) -> Result<Option<bool>, Error> {
        match ast {
            FullTextQueryAst::Geo {
                field,
                lon,
                lat,
                radius,
                unit,
            } => {
                let Some((lon_field, lat_field)) = self.geo_fields.get(field) else {
                    return Ok(None);
                };
                let schema = self.index.schema();
                let lon_column = segment
                    .fast_fields()
                    .f64(schema.get_field_name(*lon_field))?;
                let lat_column = segment
                    .fast_fields()
                    .f64(schema.get_field_name(*lat_field))?;
                let radius_meters = radius * fulltext_geo_unit_meters(unit)?;
                Ok(Some(
                    lon_column
                        .values_for_doc(doc)
                        .zip(lat_column.values_for_doc(doc))
                        .any(|(value_lon, value_lat)| {
                            fulltext_haversine_meters(*lat, *lon, value_lat, value_lon)
                                <= radius_meters
                        }),
                ))
            }
            FullTextQueryAst::And(children) => {
                for child in children {
                    if self.fast_geo_ast_matches(child, segment, doc)? == Some(false) {
                        return Ok(Some(false));
                    }
                }
                Ok(Some(true))
            }
            FullTextQueryAst::Field { expr, .. } | FullTextQueryAst::Attributed { expr, .. } => {
                self.fast_geo_ast_matches(expr, segment, doc)
            }
            FullTextQueryAst::GeoShape { .. }
            | FullTextQueryAst::Or(_)
            | FullTextQueryAst::Not(_)
            | FullTextQueryAst::Optional(_) => Ok(None),
            FullTextQueryAst::All
            | FullTextQueryAst::Text(_)
            | FullTextQueryAst::Phrase(_)
            | FullTextQueryAst::Prefix(_)
            | FullTextQueryAst::Wildcard(_)
            | FullTextQueryAst::Fuzzy(_)
            | FullTextQueryAst::Tag { .. }
            | FullTextQueryAst::Numeric { .. }
            | FullTextQueryAst::Missing { .. }
            | FullTextQueryAst::VectorKnn { .. }
            | FullTextQueryAst::VectorRange { .. } => Ok(Some(true)),
        }
    }
}

fn fulltext_fast_key(
    searcher: &tantivy::Searcher,
    address: DocAddress,
) -> Result<Option<String>, Error> {
    let segment = &searcher.segment_readers()[address.segment_ord as usize];
    let Some(column) = segment.fast_fields().str(FULLTEXT_KEY_FIELD)? else {
        return Ok(None);
    };
    let Some(ordinal) = column.term_ords(address.doc_id).next() else {
        return Ok(None);
    };
    let mut key = String::new();
    Ok(column.ord_to_str(ordinal, &mut key)?.then_some(key))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FullTextScoredDoc {
    pub(super) score: Score,
    pub(super) address: DocAddress,
}

impl PartialEq for FullTextScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.address == other.address
    }
}

impl Eq for FullTextScoredDoc {}

impl PartialOrd for FullTextScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FullTextScoredDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.address.cmp(&self.address))
    }
}

pub(super) struct FullTextCollectorFruit {
    pub(super) total: usize,
    pub(super) top_docs: Vec<FullTextScoredDoc>,
    pub(super) timed_out: bool,
}

pub(super) struct FullTextDeadlineCollector {
    pub(super) limit: usize,
    pub(super) deadline: Instant,
}

pub(super) struct FullTextUnusedSegmentCollector;

impl SegmentCollector for FullTextUnusedSegmentCollector {
    type Fruit = FullTextCollectorFruit;

    fn collect(&mut self, _doc: DocId, _score: Score) {}

    fn harvest(self) -> Self::Fruit {
        FullTextCollectorFruit {
            total: 0,
            top_docs: Vec::new(),
            timed_out: false,
        }
    }
}

impl Collector for FullTextDeadlineCollector {
    type Fruit = FullTextCollectorFruit;
    type Child = FullTextUnusedSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(FullTextUnusedSegmentCollector)
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> tantivy::Result<FullTextCollectorFruit> {
        if Instant::now() >= self.deadline {
            return Ok(FullTextCollectorFruit {
                total: 0,
                top_docs: Vec::new(),
                timed_out: true,
            });
        }
        // Counting through Weight::count avoids score computation. The second pass
        // deliberately uses for_each_pruning so Tantivy can select its Block-WAND
        // implementation for eligible Boolean/term queries.
        let total = weight.count(reader)? as usize;
        let mut heap = BinaryHeap::new();
        let mut timed_out = Instant::now() >= self.deadline;
        let mut checked = 0usize;
        if self.limit > 0 && !timed_out {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if checked == 0 && Instant::now() >= self.deadline {
                    timed_out = true;
                    return Score::MAX;
                }
                checked = (checked + 1) & 127;
                if reader
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                {
                    let scored = FullTextScoredDoc {
                        score,
                        address: DocAddress::new(segment_ord, doc),
                    };
                    if heap.len() < self.limit {
                        heap.push(Reverse(scored));
                    } else if heap.peek().is_some_and(|worst| scored > worst.0) {
                        heap.pop();
                        heap.push(Reverse(scored));
                    }
                }
                if timed_out {
                    Score::MAX
                } else if heap.len() < self.limit {
                    Score::MIN
                } else {
                    heap.peek().map(|worst| worst.0.score).unwrap_or(Score::MIN)
                }
            })?;
            timed_out |= Instant::now() >= self.deadline;
        }
        Ok(FullTextCollectorFruit {
            total,
            top_docs: heap.into_iter().map(|entry| entry.0).collect(),
            timed_out,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<FullTextCollectorFruit>,
    ) -> tantivy::Result<FullTextCollectorFruit> {
        let mut total = 0usize;
        let mut timed_out = false;
        let mut heap = BinaryHeap::new();
        for fruit in segment_fruits {
            total = total.saturating_add(fruit.total);
            timed_out |= fruit.timed_out;
            for scored in fruit.top_docs {
                if self.limit == 0 {
                    continue;
                }
                if heap.len() < self.limit {
                    heap.push(Reverse(scored));
                } else if heap.peek().is_some_and(|worst| scored > worst.0) {
                    heap.pop();
                    heap.push(Reverse(scored));
                }
            }
        }
        let mut top_docs = heap.into_iter().map(|entry| entry.0).collect::<Vec<_>>();
        top_docs.sort_by(|left, right| right.cmp(left));
        Ok(FullTextCollectorFruit {
            total,
            top_docs,
            timed_out,
        })
    }
}
