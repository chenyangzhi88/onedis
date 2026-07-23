impl Db {
    fn fulltext_search_frame(
        &self,
        mut collected: FullTextCollectedHits,
        options: &FullTextSearchOptions,
        display_terms: &[String],
    ) -> Result<Frame, Error> {
        let live = &mut collected.hits;
        if let Some(sort_by) = &options.sort_by {
            live.sort_by(|left, right| compare_fulltext_sort_keys(left, right, sort_by.asc));
        }
        let mut out = Vec::new();
        out.push(Frame::Integer(collected.total as i64));
        if options.limit == 0 {
            return Ok(Frame::Array(out));
        }
        for hit in collected
            .hits
            .into_iter()
            .skip(options.offset)
            .take(options.limit)
        {
            out.push(Frame::bulk_string(hit.key));
            if options.with_scores {
                out.push(Frame::bulk_string(format_fulltext_score(hit.score)));
                if options.explain_score {
                    let mut explanation = vec![
                        Frame::bulk_string("score"),
                        Frame::bulk_string(format_fulltext_score(hit.score)),
                        Frame::bulk_string("scorer"),
                        Frame::bulk_string(match options.scorer {
                            FullTextScorer::Bm25 => "BM25",
                            FullTextScorer::Bm25Std => "BM25STD",
                            FullTextScorer::DisMax => "DISMAX",
                            FullTextScorer::DocScore => "DOCSCORE",
                        }),
                    ];
                    if let Some(payload) = &options.payload {
                        explanation.push(Frame::bulk_string("query_payload"));
                        explanation.push(Frame::bulk_string(payload.clone()));
                    }
                    out.push(Frame::Array(explanation));
                }
            }
            if options.with_payloads {
                out.push(
                    hit.payload
                        .clone()
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                );
            }
            if options.with_sort_keys {
                out.push(
                    hit.sort_key
                        .clone()
                        .map(Frame::bulk_string)
                        .unwrap_or(Frame::Null),
                );
            }
            if !options.no_content {
                out.push(fulltext_fields_frame(
                    hit.fields,
                    options.return_fields.as_deref(),
                    options,
                    display_terms,
                ));
            }
        }
        Ok(Frame::Array(out))
    }
}
