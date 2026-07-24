    use super::*;

    fn frame(args: &[&str]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        )
    }

    fn bytes_frame(args: &[&[u8]]) -> Frame {
        Frame::Array(
            args.iter()
                .map(|arg| Frame::BulkString(arg.to_vec()))
                .collect(),
        )
    }

    fn assert_error_frame(frame: Frame, expected: &str) {
        match frame {
            Frame::Error(value) => assert!(value.contains(expected)),
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn full_text_create_parser_covers_index_options_schema_kinds_and_errors() {
        let create = FtCreate::parse_from_frame(frame(&[
            "FT.CREATE",
            "idx",
            "ON",
            "JSON",
            "PREFIX",
            "2",
            "doc:",
            "item:",
            "SKIPINITIALSCAN",
            "FILTER",
            "@active == 1",
            "LANGUAGE",
            "english",
            "LANGUAGE_FIELD",
            "lang",
            "SCORE",
            "0.5",
            "SCORE_FIELD",
            "score",
            "PAYLOAD_FIELD",
            "payload",
            "MAXTEXTFIELDS",
            "TEMPORARY",
            "60",
            "NOOFFSETS",
            "NOHL",
            "NOFIELDS",
            "NOFREQS",
            "STOPWORDS",
            "2",
            "a",
            "the",
            "INDEXALL",
            "ENABLE",
            "SCHEMA",
            "$.title",
            "AS",
            "title",
            "TEXT",
            "WEIGHT",
            "2.5",
            "SORTABLE",
            "UNF",
            "NOSTEM",
            "PHONETIC",
            "dm:en",
            "WITHSUFFIXTRIE",
            "$.tags",
            "TAG",
            "SEPARATOR",
            "|",
            "CASESENSITIVE",
            "INDEXEMPTY",
            "INDEXMISSING",
            "$.price",
            "NUMERIC",
            "SORTABLE",
            "$.loc",
            "GEO",
            "$.shape",
            "GEOSHAPE",
            "FLAT",
            "$.vec",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "COSINE",
            "NOINDEX",
        ]))
        .unwrap();

        assert_eq!(create.index, "idx");
        assert_eq!(create.options.source_type, FullTextSourceType::Json);
        assert_eq!(create.options.prefixes, vec!["doc:", "item:"]);
        assert!(create.options.index_options.skip_initial_scan);
        assert_eq!(create.options.index_options.score, Some(0.5));
        assert_eq!(create.options.index_options.index_all, Some(true));
        assert_eq!(create.options.schema.len(), 6);
        assert!(matches!(
            create.options.schema[0].kind,
            FullTextFieldKind::Text
        ));
        assert!(matches!(
            create.options.schema[5].kind,
            FullTextFieldKind::Vector
        ));
        assert!(matches!(
            create.options.schema[5]
                .options
                .vector
                .as_ref()
                .unwrap()
                .algorithm,
            FullTextVectorAlgorithm::Hnsw
        ));

        let create = FtCreate::parse_from_frame(frame(&[
            "FT.CREATE",
            "idx",
            "INDEXALL",
            "DISABLE",
            "SCHEMA",
            "body",
            "TEXT",
        ]))
        .unwrap();
        assert_eq!(create.options.index_options.index_all, Some(false));

        for args in [
            vec!["FT.CREATE", "idx", "ON", "BAD", "SCHEMA", "body", "TEXT"],
            vec![
                "FT.CREATE",
                "idx",
                "PREFIX",
                "2",
                "doc:",
                "SCHEMA",
                "body",
                "TEXT",
            ],
            vec![
                "FT.CREATE",
                "idx",
                "INDEXALL",
                "MAYBE",
                "SCHEMA",
                "body",
                "TEXT",
            ],
            vec!["FT.CREATE", "idx", "SCHEMA"],
            vec!["FT.CREATE", "idx", "SCHEMA", "shape", "GEOSHAPE", "BAD"],
            vec![
                "FT.CREATE",
                "idx",
                "SCHEMA",
                "vec",
                "VECTOR",
                "BAD",
                "2",
                "DIM",
                "3",
            ],
            vec![
                "FT.CREATE",
                "idx",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "3",
                "DIM",
                "3",
                "TYPE",
            ],
            vec!["FT.CREATE", "idx", "SCHEMA", "body", "TEXT", "WEIGHT", "0"],
            vec!["FT.CREATE", "idx", "SCHEMA", "body"],
        ] {
            assert!(
                FtCreate::parse_from_frame(frame(&args)).is_err(),
                "{args:?}"
            );
        }
    }

    #[test]
    fn full_text_search_aggregate_profile_and_explain_parsers_cover_options() {
        let search = FtSearch::parse_from_frame(frame(&[
            "FT.SEARCH",
            "idx",
            "@title:hello",
            "LIMIT",
            "1",
            "5",
            "NOCONTENT",
            "WITHSCORES",
            "WITHPAYLOADS",
            "WITHSORTKEYS",
            "RETURN",
            "2",
            "title",
            "AS",
            "t",
            "price",
            "FILTER",
            "price",
            "(1",
            "+inf",
            "GEOFILTER",
            "loc",
            "1",
            "2",
            "10",
            "km",
            "INKEYS",
            "2",
            "doc:1",
            "doc:2",
            "INFIELDS",
            "2",
            "title",
            "body",
            "SORTBY",
            "price",
            "DESC",
            "SUMMARIZE",
            "FIELDS",
            "1",
            "title",
            "FRAGS",
            "2",
            "LEN",
            "10",
            "SEPARATOR",
            "...",
            "HIGHLIGHT",
            "FIELDS",
            "1",
            "title",
            "TAGS",
            "<b>",
            "</b>",
            "SLOP",
            "2",
            "TIMEOUT",
            "100",
            "INORDER",
            "LANGUAGE",
            "english",
            "EXPANDER",
            "DEFAULT",
            "SCORER",
            "BM25",
            "EXPLAINSCORE",
            "PAYLOAD",
            "payload",
            "PARAMS",
            "2",
            "q",
            "hello",
            "DIALECT",
            "4",
        ]))
        .unwrap();
        assert_eq!(search.index, "idx");
        assert_eq!(search.options.offset, 1);
        assert_eq!(search.options.limit, 5);
        assert!(search.options.no_content);
        assert!(search.options.with_scores);
        assert!(search.options.with_payloads);
        assert!(search.options.with_sort_keys);
        assert_eq!(search.options.return_fields.as_ref().unwrap().len(), 2);
        assert_eq!(search.options.filters.len(), 1);
        assert!(matches!(
            search.options.filters[0].min,
            FullTextSearchBound::Exclusive(1.0)
        ));
        assert_eq!(search.options.geo_filters.len(), 1);
        assert_eq!(search.options.in_keys.as_ref().unwrap().len(), 2);
        assert_eq!(search.options.in_fields.as_ref().unwrap().len(), 2);
        assert!(!search.options.sort_by.as_ref().unwrap().asc);
        let summarize = search.options.summarize.as_ref().unwrap();
        assert_eq!(summarize.frags, 2);
        assert_eq!(summarize.len, 10);
        assert_eq!(summarize.separator, "...");
        assert_eq!(
            summarize.fields.as_ref().unwrap(),
            &HashSet::from(["title".to_string()])
        );
        let highlight = search.options.highlight.as_ref().unwrap();
        assert_eq!(highlight.open_tag, "<b>");
        assert_eq!(highlight.close_tag, "</b>");
        assert!(search.options.inorder);
        assert!(search.options.explain_score);
        assert_eq!(search.options.params.len(), 1);
        assert_eq!(search.options.dialect, 4);
        assert!(search.options.dialect_explicit);

        for args in [
            vec!["FT.SEARCH", "idx"],
            vec!["FT.SEARCH", "idx", "*", "RETURN", "2", "title"],
            vec!["FT.SEARCH", "idx", "*", "PARAMS", "3", "a", "b", "c"],
            vec!["FT.SEARCH", "idx", "*", "DIALECT", "9"],
            vec!["FT.SEARCH", "idx", "*", "EXPANDER", "CUSTOM"],
            vec!["FT.SEARCH", "idx", "*", "SCORER", "CUSTOM"],
            vec!["FT.SEARCH", "idx", "*", "FILTER", "price", "nan", "1"],
        ] {
            assert!(
                FtSearch::parse_from_frame(frame(&args)).is_err(),
                "{args:?}"
            );
        }

        let aggregate = FtAggregate::parse_from_frame(frame(&[
            "FT.AGGREGATE",
            "idx",
            "*",
            "LOAD",
            "4",
            "@title",
            "AS",
            "title",
            "@price",
            "APPLY",
            "@price * 2",
            "AS",
            "double",
            "FILTER",
            "@price > 0",
            "GROUPBY",
            "1",
            "@tag",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "n",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
            "REDUCE",
            "AVG",
            "1",
            "@price",
            "REDUCE",
            "MIN",
            "1",
            "@price",
            "REDUCE",
            "MAX",
            "1",
            "@price",
            "REDUCE",
            "FIRST_VALUE",
            "1",
            "@title",
            "REDUCE",
            "TOLIST",
            "1",
            "@title",
            "SORTBY",
            "4",
            "@price",
            "DESC",
            "@title",
            "ASC",
            "LIMIT",
            "2",
            "3",
            "WITHCURSOR",
            "COUNT",
            "7",
            "MAXIDLE",
            "123",
            "PARAMS",
            "2",
            "min",
            "1",
            "DIALECT",
            "3",
        ]))
        .unwrap();
        assert_eq!(aggregate.options.load.as_ref().unwrap().len(), 2);
        assert_eq!(aggregate.options.steps.len(), 3);
        assert_eq!(aggregate.options.sort_by.len(), 2);
        assert_eq!(aggregate.options.offset, 2);
        assert_eq!(aggregate.options.limit, 3);
        assert_eq!(aggregate.options.cursor_count, Some(7));
        assert_eq!(aggregate.options.cursor_max_idle_ms, Some(123));

        let load_all =
            FtAggregate::parse_from_frame(frame(&["FT.AGGREGATE", "idx", "*", "LOAD", "*"]))
                .unwrap();
        assert_eq!(load_all.options.load.as_ref().unwrap()[0].identifier, "*");

        for args in [
            vec!["FT.AGGREGATE", "idx"],
            vec!["FT.AGGREGATE", "idx", "*", "LOAD"],
            vec!["FT.AGGREGATE", "idx", "*", "LOAD", "2", "@a"],
            vec!["FT.AGGREGATE", "idx", "*", "APPLY", "1", "BAD", "x"],
            vec!["FT.AGGREGATE", "idx", "*", "GROUPBY", "2", "@a"],
            vec![
                "FT.AGGREGATE",
                "idx",
                "*",
                "GROUPBY",
                "0",
                "REDUCE",
                "BAD",
                "0",
            ],
            vec!["FT.AGGREGATE", "idx", "*", "SORTBY", "3", "@a"],
            vec!["FT.AGGREGATE", "idx", "*", "PARAMS", "1", "a"],
            vec!["FT.AGGREGATE", "idx", "*", "DIALECT", "0"],
        ] {
            assert!(
                FtAggregate::parse_from_frame(frame(&args)).is_err(),
                "{args:?}"
            );
        }

        let profile = FtProfile::parse_from_frame(frame(&[
            "FT.PROFILE",
            "idx",
            "SEARCH",
            "LIMITED",
            "QUERY",
            "*",
            "LIMIT",
            "0",
            "1",
        ]))
        .unwrap();
        assert!(matches!(profile.target, FtProfileTarget::Search(_)));
        let profile = FtProfile::parse_from_frame(frame(&[
            "FT.PROFILE",
            "idx",
            "AGGREGATE",
            "QUERY",
            "*",
            "LOAD",
            "*",
        ]))
        .unwrap();
        assert!(matches!(profile.target, FtProfileTarget::Aggregate(_)));
        assert!(
            FtProfile::parse_from_frame(frame(&["FT.PROFILE", "idx", "BAD", "QUERY", "*"]))
                .is_err()
        );
        assert!(
            FtProfile::parse_from_frame(frame(&["FT.PROFILE", "idx", "SEARCH", "BAD", "*"]))
                .is_err()
        );

        let explain = FtExplain::parse_from_frame(frame(&[
            "FT.EXPLAINCLI",
            "idx",
            "@title:$q",
            "PARAMS",
            "2",
            "q",
            "hello",
            "DIALECT",
            "4",
        ]))
        .unwrap();
        assert!(explain.cli);
        assert_eq!(explain.options.params.len(), 1);
        assert!(
            FtExplain::parse_from_frame(frame(&["FT.EXPLAIN", "idx", "*", "PARAMS", "1", "q"]))
                .is_err()
        );
        assert!(
            FtExplain::parse_from_frame(frame(&["FT.EXPLAIN", "idx", "*", "DIALECT", "5"]))
                .is_err()
        );
    }

    #[test]
    fn full_text_misc_parsers_cover_alias_cursor_dict_sug_syn_and_unsupported() {
        assert!(FtList::parse_from_frame(frame(&["FT._LIST"])).is_ok());
        assert!(FtList::parse_from_frame(frame(&["FT._LIST", "extra"])).is_err());

        assert!(
            !FtDropIndex::parse_from_frame(frame(&["FT.DROPINDEX", "idx"]))
                .unwrap()
                .delete_documents
        );
        assert!(
            FtDropIndex::parse_from_frame(frame(&["FT.DROPINDEX", "idx", "DD"]))
                .unwrap()
                .delete_documents
        );
        assert!(FtDropIndex::parse_from_frame(frame(&["FT.DROPINDEX", "idx", "BAD"])).is_err());

        assert_eq!(
            FtAlter::parse_from_frame(frame(&[
                "FT.ALTER",
                "idx",
                "SCHEMA",
                "ADD",
                "new_field",
                "TAG",
            ]))
            .unwrap()
            .fields
            .len(),
            1
        );
        let alter_skip = FtAlter::parse_from_frame(frame(&[
            "FT.ALTER",
            "idx",
            "SKIPINITIALSCAN",
            "SCHEMA",
            "ADD",
            "new_field",
            "TEXT",
        ]))
        .unwrap();
        assert!(alter_skip.skip_initial_scan);
        assert_eq!(alter_skip.fields.len(), 1);
        assert!(
            FtAlter::parse_from_frame(frame(&[
                "FT.ALTER", "idx", "SCHEMA", "DROP", "field", "TAG",
            ]))
            .is_err()
        );

        assert_eq!(
            FtAliasAdd::parse_from_frame(frame(&["FT.ALIASADD", "alias", "idx"]))
                .unwrap()
                .alias,
            "alias"
        );
        assert_eq!(
            FtAliasUpdate::parse_from_frame(frame(&["FT.ALIASUPDATE", "alias", "idx"]))
                .unwrap()
                .index,
            "idx"
        );
        assert_eq!(
            FtAliasDel::parse_from_frame(frame(&["FT.ALIASDEL", "alias"]))
                .unwrap()
                .alias,
            "alias"
        );
        assert!(FtAliasAdd::parse_from_frame(frame(&["FT.ALIASADD", "alias"])).is_err());
        assert!(FtAliasDel::parse_from_frame(frame(&["FT.ALIASDEL"])).is_err());

        assert!(matches!(
            FtConfig::parse_from_frame(frame(&["FT.CONFIG", "GET", "DEFAULT_DIALECT"])).unwrap(),
            FtConfig::Get { .. }
        ));
        assert!(matches!(
            FtConfig::parse_from_frame(frame(&["FT.CONFIG", "SET", "DEFAULT_DIALECT", "4"]))
                .unwrap(),
            FtConfig::Set { .. }
        ));
        assert!(FtConfig::parse_from_frame(frame(&["FT.CONFIG", "BAD", "x"])).is_err());

        assert!(matches!(
            FtCursor::parse_from_frame(frame(&["FT.CURSOR", "READ", "idx", "1", "COUNT", "5"]))
                .unwrap(),
            FtCursor::Read { count: 5, .. }
        ));
        assert!(matches!(
            FtCursor::parse_from_frame(frame(&["FT.CURSOR", "DEL", "idx", "1"])).unwrap(),
            FtCursor::Del { cursor_id: 1, .. }
        ));
        assert!(FtCursor::parse_from_frame(frame(&["FT.CURSOR", "READ", "idx", "bad"])).is_err());
        assert!(
            FtCursor::parse_from_frame(frame(&["FT.CURSOR", "READ", "idx", "1", "BAD"])).is_err()
        );

        assert_eq!(
            FtTagVals::parse_from_frame(frame(&["FT.TAGVALS", "idx", "tag"]))
                .unwrap()
                .field,
            "tag"
        );
        assert_eq!(
            FtInfo::parse_from_frame(frame(&["FT.INFO", "idx"]))
                .unwrap()
                .index,
            "idx"
        );
        assert!(FtTagVals::parse_from_frame(frame(&["FT.TAGVALS", "idx"])).is_err());
        assert!(FtInfo::parse_from_frame(frame(&["FT.INFO"])).is_err());

        assert!(matches!(
            FtDict::parse_from_frame(frame(&["FT.DICTADD", "dict", "term1", "term2"])).unwrap(),
            FtDict::Add { .. }
        ));
        assert!(matches!(
            FtDict::parse_from_frame(frame(&["FT.DICTDEL", "dict", "term1"])).unwrap(),
            FtDict::Del { .. }
        ));
        assert!(matches!(
            FtDict::parse_from_frame(frame(&["FT.DICTDUMP", "dict"])).unwrap(),
            FtDict::Dump { .. }
        ));
        assert!(FtDict::parse_from_frame(frame(&["FT.DICTADD", "dict"])).is_err());
        assert!(FtDict::parse_from_frame(frame(&["FT.DICTDUMP", "dict", "extra"])).is_err());

        let spell = FtSpellCheck::parse_from_frame(frame(&[
            "FT.SPELLCHECK",
            "idx",
            "helo",
            "DISTANCE",
            "2",
            "TERMS",
            "INCLUDE",
            "dict1",
            "TERMS",
            "EXCLUDE",
            "dict2",
        ]))
        .unwrap();
        assert_eq!(spell.distance, 2);
        assert_eq!(
            spell.include,
            vec![FullTextSpellcheckDictionary {
                name: "dict1".to_string(),
                terms: Vec::new(),
            }]
        );
        assert_eq!(
            spell.exclude,
            vec![FullTextSpellcheckDictionary {
                name: "dict2".to_string(),
                terms: Vec::new(),
            }]
        );
        assert!(FtSpellCheck::parse_from_frame(frame(&["FT.SPELLCHECK", "idx"])).is_err());
        assert!(
            FtSpellCheck::parse_from_frame(frame(&[
                "FT.SPELLCHECK",
                "idx",
                "q",
                "TERMS",
                "BAD",
                "d"
            ]))
            .is_err()
        );

        assert!(matches!(
            FtSug::parse_from_frame(frame(&[
                "FT.SUGADD",
                "sug",
                "hello",
                "1.5",
                "INCR",
                "PAYLOAD",
                "p",
            ]))
            .unwrap(),
            FtSug::Add {
                incr: true,
                payload: Some(_),
                ..
            }
        ));
        assert!(matches!(
            FtSug::parse_from_frame(frame(&[
                "FT.SUGGET",
                "sug",
                "he",
                "FUZZY",
                "WITHSCORES",
                "WITHPAYLOADS",
                "MAX",
                "2",
            ]))
            .unwrap(),
            FtSug::Get {
                fuzzy: true,
                with_scores: true,
                with_payloads: true,
                max: 2,
                ..
            }
        ));
        assert!(matches!(
            FtSug::parse_from_frame(frame(&["FT.SUGDEL", "sug", "hello"])).unwrap(),
            FtSug::Del { .. }
        ));
        assert!(matches!(
            FtSug::parse_from_frame(frame(&["FT.SUGLEN", "sug"])).unwrap(),
            FtSug::Len { .. }
        ));
        assert!(FtSug::parse_from_frame(frame(&["FT.SUGADD", "sug", "hello", "bad"])).is_err());
        assert!(FtSug::parse_from_frame(frame(&["FT.SUGGET", "sug", "he", "MAX", "bad"])).is_err());
        assert!(FtSug::parse_from_frame(frame(&["FT.SUGDEL", "sug"])).is_err());

        assert!(matches!(
            FtSyn::parse_from_frame(frame(&[
                "FT.SYNUPDATE",
                "idx",
                "group",
                "SKIPINITIALSCAN",
                "fast",
                "quick",
            ]))
            .unwrap(),
            FtSyn::Update { .. }
        ));
        assert!(matches!(
            FtSyn::parse_from_frame(frame(&["FT.SYNDUMP", "idx"])).unwrap(),
            FtSyn::Dump { .. }
        ));
        assert!(FtSyn::parse_from_frame(frame(&["FT.SYNUPDATE", "idx", "group"])).is_err());
        assert!(FtSyn::parse_from_frame(frame(&["FT.SYNDUMP", "idx", "extra"])).is_err());

        let unsupported = FtUnsupported::parse_from_frame(frame(&["FT.DEBUG", "idx"])).unwrap();
        assert_error_frame(unsupported.apply().unwrap(), "FT.DEBUG");
    }

    #[test]
    fn full_text_parsers_reject_unsafe_counts_and_invalid_search_combinations() {
        let huge = usize::MAX.to_string();
        let owned_frame = |args: Vec<&str>| {
            Frame::Array(
                args.into_iter()
                    .map(|arg| Frame::bulk_string(arg.to_string()))
                    .collect(),
            )
        };
        for args in [
            vec!["FT.SEARCH", "idx", "*", "RETURN", huge.as_str()],
            vec!["FT.SEARCH", "idx", "*", "INKEYS", huge.as_str()],
            vec!["FT.SEARCH", "idx", "*", "INFIELDS", huge.as_str()],
            vec!["FT.SEARCH", "idx", "*", "PARAMS", huge.as_str()],
            vec![
                "FT.SEARCH",
                "idx",
                "*",
                "SUMMARIZE",
                "FIELDS",
                huge.as_str(),
            ],
            vec!["FT.CREATE", "idx", "PREFIX", huge.as_str(), "SCHEMA", "t", "TEXT"],
            vec![
                "FT.CREATE",
                "idx",
                "STOPWORDS",
                huge.as_str(),
                "SCHEMA",
                "t",
                "TEXT",
            ],
            vec![
                "FT.CREATE",
                "idx",
                "SCHEMA",
                "v",
                "VECTOR",
                "FLAT",
                huge.as_str(),
            ],
            vec!["FT.AGGREGATE", "idx", "*", "GROUPBY", huge.as_str()],
            vec![
                "FT.AGGREGATE",
                "idx",
                "*",
                "GROUPBY",
                "0",
                "REDUCE",
                "COUNT",
                huge.as_str(),
            ],
            vec!["FT.AGGREGATE", "idx", "*", "SORTBY", huge.as_str()],
            vec!["FT.AGGREGATE", "idx", "*", "PARAMS", huge.as_str()],
            vec!["FT.EXPLAIN", "idx", "*", "PARAMS", huge.as_str()],
        ] {
            let result = match args[0] {
                "FT.SEARCH" => FtSearch::parse_from_frame(owned_frame(args)).map(|_| ()),
                "FT.CREATE" => FtCreate::parse_from_frame(owned_frame(args)).map(|_| ()),
                "FT.AGGREGATE" => FtAggregate::parse_from_frame(owned_frame(args)).map(|_| ()),
                "FT.EXPLAIN" => FtExplain::parse_from_frame(owned_frame(args)).map(|_| ()),
                _ => unreachable!(),
            };
            assert!(result.is_err());
        }
        for args in [
            vec![
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "*",
                "VSIM",
                "@v",
                "$vec",
                "KNN",
                huge.as_str(),
            ],
            vec![
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "*",
                "VSIM",
                "@v",
                "$vec",
                "KNN",
                "2",
                "K",
                "1",
                "COMBINE",
                "RRF",
                huge.as_str(),
            ],
            vec![
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "*",
                "VSIM",
                "@v",
                "$vec",
                "KNN",
                "2",
                "K",
                "1",
                "LOAD",
                huge.as_str(),
            ],
        ] {
            assert!(FtHybrid::parse_from_frame(owned_frame(args)).is_err());
        }

        let return_zero =
            FtSearch::parse_from_frame(frame(&["FT.SEARCH", "idx", "*", "RETURN", "0"]))
                .unwrap();
        assert!(return_zero.options.no_content);
        assert!(return_zero.options.return_fields.unwrap().is_empty());
        assert!(
            FtSearch::parse_from_frame(frame(&["FT.SEARCH", "idx", "*", "INKEYS", "0"]))
                .is_err()
        );
        assert!(
            FtSearch::parse_from_frame(frame(&["FT.SEARCH", "idx", "*", "INFIELDS", "0"]))
                .is_err()
        );
        assert!(
            FtSearch::parse_from_frame(frame(&["FT.SEARCH", "idx", "*", "EXPLAINSCORE"]))
                .is_err()
        );
        assert!(
            FtSearch::parse_from_frame(frame(&[
                "FT.SEARCH",
                "idx",
                "*",
                "SLOP",
                "4294967296",
            ]))
            .is_err()
        );
        assert!(
            FtSearch::parse_from_frame(frame(&[
                "FT.SEARCH",
                "idx",
                "*",
                "HIGHLIGHT",
                "TAGS",
                "<i>",
            ]))
            .is_err()
        );

        let binary = FtSearch::parse_from_frame(bytes_frame(&[
            b"FT.SEARCH",
            b"idx",
            b"*",
            b"PAYLOAD",
            &[0xff, 0x00, 0x01],
        ]))
        .unwrap();
        assert_eq!(binary.options.payload, Some(vec![0xff, 0x00, 0x01]));
    }

    #[test]
    fn full_text_profile_spell_synonym_and_hybrid_options_are_preserved() {
        let profile =
            FtProfile::parse_from_frame(frame(&["FT.PROFILE", "idx", "SEARCH", "QUERY", "*"]))
                .unwrap();
        assert!(!profile.limited);
        assert!(matches!(profile.target, FtProfileTarget::Search(_)));
        let profile = FtProfile::parse_from_frame(frame(&[
            "FT.PROFILE",
            "idx",
            "SEARCH",
            "LIMITED",
            "QUERY",
            "*",
        ]))
        .unwrap();
        assert!(profile.limited);

        let spell = FtSpellCheck::parse_from_frame(frame(&[
            "FT.SPELLCHECK",
            "idx",
            "helo",
            "TERMS",
            "INCLUDE",
            "dict",
            "hello",
            "help",
            "DIALECT",
            "4",
        ]))
        .unwrap();
        assert_eq!(spell.include[0].terms, vec!["hello", "help"]);
        assert_eq!(spell.dialect, Some(4));
        assert!(
            FtSpellCheck::parse_from_frame(frame(&[
                "FT.SPELLCHECK",
                "idx",
                "helo",
                "DISTANCE",
                "5",
            ]))
            .is_err()
        );

        let syn = FtSyn::parse_from_frame(frame(&[
            "FT.SYNUPDATE",
            "idx",
            "group",
            "SKIPINITIALSCAN",
            "fast",
            "quick",
        ]))
        .unwrap();
        assert!(matches!(
            syn,
            FtSyn::Update {
                skip_initial_scan: true,
                ..
            }
        ));

        let hybrid = FtHybrid::parse_from_frame(frame(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "fox",
            "YIELD_SCORE_AS",
            "text_score",
            "VSIM",
            "@embedding",
            "$vec",
            "KNN",
            "4",
            "K",
            "5",
            "YIELD_DISTANCE_AS",
            "distance",
            "COMBINE",
            "RRF",
            "6",
            "WINDOW",
            "7",
            "CONSTANT",
            "20",
            "YIELD_SCORE_AS",
            "combined",
            "PARAMS",
            "2",
            "vec",
            "[1,0]",
            "LIMIT",
            "1",
            "3",
            "WITHSCORES",
        ]))
        .unwrap();
        assert_eq!(hybrid.index, "idx");
        assert!(hybrid.vector_query.contains("KNN 5 @embedding $vec"));
        assert_eq!(
            hybrid.options.search.params.get("vec").unwrap(),
            b"[1,0]"
        );
        assert_eq!(
            hybrid.options.search_score_alias.as_deref(),
            Some("text_score")
        );
        assert_eq!(
            hybrid.options.vector_score_alias.as_deref(),
            Some("distance")
        );
        assert_eq!(
            hybrid.options.combined_score_alias.as_deref(),
            Some("combined")
        );
        assert!(matches!(
            hybrid.options.combine,
            FullTextHybridCombine::Rrf {
                window: 7,
                constant: 20.0,
            }
        ));
        assert!(FtHybrid::parse_from_frame(frame(&["FT.HYBRID", "idx", "fox"])).is_err());

        let suggestion = FtSug::parse_from_frame(bytes_frame(&[
            b"FT.SUGADD",
            b"sug",
            b"hello",
            b"1",
            b"PAYLOAD",
            &[0xff, 0x00],
        ]))
        .unwrap();
        assert!(matches!(
            suggestion,
            FtSug::Add {
                payload: Some(payload),
                ..
            } if payload == vec![0xff, 0x00]
        ));
    }
