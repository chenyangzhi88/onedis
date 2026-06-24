impl Db {
    fn fulltext_search_frame(
        &self,
        mut live: Vec<FullTextLiveHit>,
        options: &FullTextSearchOptions,
        display_terms: &[String],
    ) -> Result<Frame, Error> {
        if let Some(sort_by) = &options.sort_by {
            live.sort_by(|left, right| compare_fulltext_sort_keys(left, right, sort_by.asc));
        }
        let total = live.len();
        let mut out = Vec::new();
        out.push(Frame::Integer(total as i64));
        if options.limit == 0 {
            return Ok(Frame::Array(out));
        }
        for hit in live.into_iter().skip(options.offset).take(options.limit) {
            out.push(Frame::bulk_string(hit.key));
            if options.with_scores {
                out.push(Frame::bulk_string(format_fulltext_score(hit.score)));
                if options.explain_score {
                    out.push(Frame::Array(vec![
                        Frame::bulk_string("score"),
                        Frame::bulk_string(format_fulltext_score(hit.score)),
                    ]));
                }
            }
            if options.with_payloads {
                out.push(Frame::Null);
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
