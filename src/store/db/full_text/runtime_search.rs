use super::*;
impl FullTextRuntime {
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
        let searcher = self.reader.searcher();
        let query = self.plan_query(ast, options.in_fields.as_deref(), options)?;
        let result = self.search_query(query, &searcher, Some(fetch_limit), deadline)?;
        if result.timed_out && deadline.fail_on_timeout {
            return Err(Error::msg("Timeout limit was reached"));
        }
        Ok(result.hits)
    }

    pub(super) fn search_query(
        &self,
        query: Box<dyn Query>,
        searcher: &tantivy::Searcher,
        fetch_limit: Option<usize>,
        deadline: FullTextSearchDeadline,
    ) -> Result<FullTextSearchHits, Error> {
        let fetch_limit = fetch_limit.unwrap_or(usize::MAX);
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
        for scored in result.top_docs {
            let score = scored.score;
            let address = scored.address;
            let doc: TantivyDocument = searcher.doc(address)?;
            let Some(key) = doc
                .get_first(self.key_field)
                .and_then(|value| value.as_str())
            else {
                continue;
            };
            hits.push(FullTextSearchHit {
                key: key.to_string(),
                score,
            });
        }
        Ok(FullTextSearchHits {
            total: result.total,
            hits,
            timed_out: result.timed_out,
        })
    }
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
        let mut scorer = weight.scorer(reader, 1.0)?;
        let mut total = 0usize;
        let mut heap = BinaryHeap::new();
        let mut timed_out = false;
        let mut checked = 0usize;
        loop {
            let doc = scorer.doc();
            if doc == TERMINATED {
                break;
            }
            if checked == 0 && Instant::now() >= self.deadline {
                timed_out = true;
                break;
            }
            checked = (checked + 1) & 127;
            if reader
                .alive_bitset()
                .is_none_or(|alive| alive.is_alive(doc))
            {
                total = total.saturating_add(1);
                if self.limit > 0 {
                    let scored = FullTextScoredDoc {
                        score: scorer.score(),
                        address: DocAddress::new(segment_ord, doc),
                    };
                    if heap.len() < self.limit {
                        heap.push(Reverse(scored));
                    } else if heap.peek().is_some_and(|worst| scored > worst.0) {
                        heap.pop();
                        heap.push(Reverse(scored));
                    }
                }
            }
            scorer.advance();
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
