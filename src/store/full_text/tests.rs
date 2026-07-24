use super::*;
use crate::store::kv_store::KvStore;

fn test_store(label: &str) -> KvStore {
    let unique = format!(
        "onedis-fulltext-test-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique);
    let db_path = base.join("db");
    let wal_dir = base.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    KvStore::new(db_path, wal_dir, 1)
}

fn text_field(name: &str) -> FullTextFieldSchema {
    FullTextFieldSchema {
        name: name.to_string(),
        kind: FullTextFieldKind::Text,
        options: FullTextFieldOptions::default(),
    }
}

fn field(name: &str, kind: FullTextFieldKind) -> FullTextFieldSchema {
    FullTextFieldSchema {
        name: name.to_string(),
        kind,
        options: FullTextFieldOptions::default(),
    }
}

fn vector_options() -> FullTextVectorOptions {
    FullTextVectorOptions {
        algorithm: FullTextVectorAlgorithm::Hnsw,
        attributes: vec![
            ("TYPE".to_string(), "FLOAT32".to_string()),
            ("DIM".to_string(), "3".to_string()),
            ("DISTANCE_METRIC".to_string(), "COSINE".to_string()),
            ("M".to_string(), "16".to_string()),
            ("EF_CONSTRUCTION".to_string(), "200".to_string()),
        ],
    }
}

fn search_options() -> FullTextSearchOptions {
    FullTextSearchOptions {
        offset: 0,
        limit: 10,
        return_fields: None,
        no_content: false,
        with_scores: false,
        with_payloads: false,
        with_sort_keys: false,
        filters: Vec::new(),
        geo_filters: Vec::new(),
        in_keys: None,
        in_fields: None,
        sort_by: None,
        timeout_ms: None,
        slop: None,
        inorder: false,
        language: None,
        payload: None,
        scorer: FullTextScorer::Bm25Std,
        summarize: false,
        highlight: false,
        explain_score: false,
        params: HashMap::new(),
        dialect: 2,
        dialect_explicit: false,
    }
}

fn meta(schema: Vec<FullTextFieldSchema>) -> FullTextIndexMeta {
    FullTextIndexMeta {
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema,
        aliases: Vec::new(),
        index_options: FullTextIndexOptions::default(),
        state: FullTextIndexState::Ready,
        generation: 1,
        backfill_cursor: None,
        last_indexed_outbox_seq: 0,
        refresh_policy: FullTextRefreshPolicy::default(),
    }
}

fn row(key: &str, score: f64, fields: &[(&str, &str)]) -> FullTextAggregateRow {
    let hit = FullTextLiveHit {
        key: key.to_string(),
        score: score as f32,
        fields: fields
            .iter()
            .map(|(field, value)| ((*field).to_string(), (*value).to_string()))
            .collect(),
        sort_key: None,
        payload: None,
    };
    fulltext_aggregate_row_from_hit(hit, None).unwrap()
}

fn number_value(value: &FullTextAggregateValue) -> f64 {
    match value {
        FullTextAggregateValue::Number(value) => *value,
        _ => panic!("expected numeric aggregate value"),
    }
}

fn string_value(value: &FullTextAggregateValue) -> &str {
    match value {
        FullTextAggregateValue::String(value) => value,
        _ => panic!("expected string aggregate value"),
    }
}

#[test]
fn vector_parsing_distance_and_query_params_cover_success_and_errors() {
    let binary = [1.0f32.to_le_bytes(), 2.5f32.to_le_bytes()].concat();
    assert_eq!(
        parse_fulltext_vector_bytes(&binary).unwrap(),
        vec![1.0, 2.5]
    );
    assert_eq!(
        parse_fulltext_vector_bytes(b"[1.0, 2.0, 3.5]").unwrap(),
        vec![1.0, 2.0, 3.5]
    );
    assert_eq!(
        parse_fulltext_vector_text("1 2,3").unwrap(),
        vec![1.0, 2.0, 3.0]
    );
    assert_eq!(
        parse_fulltext_vector_json_value(&serde_json::json!([1, 2.5])).unwrap(),
        vec![1.0, 2.5]
    );
    assert_eq!(
        parse_fulltext_vector_json_value(&serde_json::json!("4,5")).unwrap(),
        vec![4.0, 5.0]
    );

    let mut params = HashMap::new();
    params.insert("q".to_string(), b"9 8 7".to_vec());
    assert_eq!(
        parse_fulltext_vector_param(&params, "q").unwrap(),
        vec![9.0, 8.0, 7.0]
    );
    assert!(parse_fulltext_vector_param(&params, "missing").is_err());

    assert!((fulltext_vector_distance("L2", &[1.0, 2.0], &[3.0, 4.0]).unwrap() - 8.0).abs() < 1e-6);
    assert!(
        (fulltext_vector_distance("IP", &[1.0, 2.0], &[3.0, 4.0]).unwrap() + 11.0).abs() < 1e-6
    );
    assert!(
        fulltext_vector_distance("COSINE", &[1.0, 0.0], &[1.0, 0.0])
            .unwrap()
            .abs()
            < 1e-6
    );
    assert!(fulltext_vector_distance("COSINE", &[0.0, 0.0], &[1.0, 0.0]).is_err());
    assert!(fulltext_vector_distance("BAD", &[1.0], &[1.0]).is_err());
    assert!(fulltext_vector_distance("L2", &[1.0], &[1.0, 2.0]).is_err());
    assert!(parse_fulltext_vector_bytes(&[1, 2, 3]).is_err());
    assert!(parse_fulltext_vector_text("not-a-number").is_err());
    assert!(parse_fulltext_vector_json_value(&serde_json::json!({"x": 1})).is_err());
}

#[test]
fn query_parser_attributes_vectors_geo_numeric_and_helpers_are_covered() {
    assert!(matches!(
        FullTextQueryParser::new("", 2).parse().unwrap(),
        FullTextQueryAst::All
    ));
    assert!(matches!(
        FullTextQueryParser::new("hello*", 2).parse().unwrap(),
        FullTextQueryAst::Prefix(prefix) if prefix == "hello"
    ));
    assert!(matches!(
        FullTextQueryParser::new("h?llo", 2).parse().unwrap(),
        FullTextQueryAst::Wildcard(pattern) if pattern == "h?llo"
    ));
    assert!(matches!(
        FullTextQueryParser::new("%helo%", 2).parse().unwrap(),
        FullTextQueryAst::Fuzzy(term) if term == "helo"
    ));
    assert!(matches!(
        FullTextQueryParser::new("\"hello world\"", 2).parse().unwrap(),
        FullTextQueryAst::Phrase(phrase) if phrase == "hello world"
    ));
    assert!(matches!(
        FullTextQueryParser::new("@tag:{foo\\|bar|baz}", 2)
            .parse()
            .unwrap(),
        FullTextQueryAst::Tag { field, values }
            if field == "tag" && values == vec!["foo|bar".to_string(), "baz".to_string()]
    ));
    assert!(matches!(
        FullTextQueryParser::new("@price:[(10 +inf]", 2)
            .parse()
            .unwrap(),
        FullTextQueryAst::Numeric { field, min: FullTextNumericBound::Exclusive(10.0), max: FullTextNumericBound::PosInf }
            if field == "price"
    ));
    assert!(matches!(
        FullTextQueryParser::new("@loc:[-122.0 37.0 10 km]", 2)
            .parse()
            .unwrap(),
        FullTextQueryAst::Geo { field, unit, .. } if field == "loc" && unit == "km"
    ));
    assert!(matches!(
        FullTextQueryParser::new("@shape:[WITHIN POINT(1 2)]", 2)
            .parse()
            .unwrap(),
        FullTextQueryAst::GeoShape { field, relation, shape }
            if field == "shape" && relation == "WITHIN" && shape == "POINT(1 2)"
    ));

    let vector_ast = FullTextQueryParser::new("(@title:hello)=>[KNN 5 @vec $blob]", 2)
        .parse()
        .unwrap();
    assert!(contains_fulltext_vector_query(&vector_ast));
    let plan = fulltext_vector_plan(&vector_ast).unwrap();
    assert_eq!(plan.field, "vec");
    assert_eq!(plan.blob_param, "blob");
    assert!(matches!(plan.kind, FullTextVectorPlanKind::Knn { k: 5 }));
    assert!(plan.filter.is_some());

    let range_ast = FullTextQueryParser::new("@vec:[VECTOR_RANGE 0.75 $blob]", 2)
        .parse()
        .unwrap();
    assert!(matches!(
        fulltext_vector_plan(&range_ast).unwrap().kind,
        FullTextVectorPlanKind::Range { radius } if (radius - 0.75).abs() < 1e-6
    ));

    let weighted = FullTextQueryParser::new("hello=>{$weight: 2.5}", 2)
        .parse()
        .unwrap();
    assert!(matches!(
        weighted,
        FullTextQueryAst::Attributed { weight: Some(weight), .. }
            if (weight - 2.5).abs() < 1e-6
    ));
    assert_eq!(
        parse_query_attribute_weight("x $weight=3 ;").unwrap(),
        Some(3.0)
    );
    assert!(parse_query_attribute_weight("$weight: -1").is_err());
    assert_eq!(unescape_query_token(r"hello\ world"), "hello world");
    assert_eq!(
        split_tag_values(r"one|two\|too| three "),
        vec!["one", "two|too", "three"]
    );
    assert_eq!(fulltext_wildcard_to_regex("a.b?c*"), r"a\.b.c.*");
    assert!(parse_f64_token("nan", "ERR bad").unwrap().is_nan());
    assert!(FullTextQueryParser::new("@bad", 2).parse().is_err());
    assert!(FullTextQueryParser::new("%", 2).parse().is_err());
    assert!(FullTextQueryParser::new("(hello", 2).parse().is_err());
}

#[test]
fn schema_vector_and_config_validation_cover_redissearch_edges() {
    let mut vector_field = field("vec", FullTextFieldKind::Vector);
    vector_field.options.vector = Some(vector_options());
    let mut geoshape = field("shape", FullTextFieldKind::GeoShape);
    geoshape.options.geoshape_coordinate_system = Some(FullTextGeoShapeCoordinateSystem::Flat);
    let valid = FullTextCreateOptions {
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema: vec![
            text_field("title"),
            field("tag", FullTextFieldKind::Tag),
            field("price", FullTextFieldKind::Numeric),
            field("loc", FullTextFieldKind::Geo),
            geoshape.clone(),
            vector_field.clone(),
        ],
        index_options: FullTextIndexOptions {
            score: Some(1.0),
            stopwords: Some(vec!["a".to_string()]),
            ..FullTextIndexOptions::default()
        },
    };
    validate_fulltext_create(&valid).unwrap();

    let mut empty_prefix = valid.clone();
    empty_prefix.prefixes.clear();
    assert!(validate_fulltext_create(&empty_prefix).is_err());

    let mut duplicate = valid.clone();
    duplicate.schema.push(text_field("title"));
    assert!(validate_fulltext_create(&duplicate).is_err());

    let mut duplicate_alias = valid.clone();
    duplicate_alias.schema[0].options.alias = Some("same".to_string());
    duplicate_alias.schema[1].options.alias = Some("same".to_string());
    assert!(validate_fulltext_create(&duplicate_alias).is_err());

    let mut json = valid.clone();
    json.source_type = FullTextSourceType::Json;
    json.schema = vec![text_field("$.title")];
    validate_fulltext_create(&json).unwrap();
    json.schema = vec![text_field("$.")];
    assert!(validate_fulltext_create(&json).is_err());

    let mut bad_tag = field("tag", FullTextFieldKind::Tag);
    bad_tag.options.separator = Some("too-long".to_string());
    assert!(validate_fulltext_field(&bad_tag).is_err());
    let mut bad_numeric = field("n", FullTextFieldKind::Numeric);
    bad_numeric.options.case_sensitive = true;
    assert!(validate_fulltext_field(&bad_numeric).is_err());
    let mut missing_geoshape_system = field("shape", FullTextFieldKind::GeoShape);
    assert!(validate_fulltext_field(&missing_geoshape_system).is_err());
    missing_geoshape_system.options.geoshape_coordinate_system =
        Some(FullTextGeoShapeCoordinateSystem::Spherical);
    validate_fulltext_field(&missing_geoshape_system).unwrap();

    validate_fulltext_vector_options(&vector_options()).unwrap();
    let mut duplicated_attr = vector_options();
    duplicated_attr
        .attributes
        .push(("dim".to_string(), "3".to_string()));
    assert!(validate_fulltext_vector_options(&duplicated_attr).is_err());
    for bad_attrs in [
        vec![
            ("TYPE".to_string(), "BAD".to_string()),
            ("DIM".to_string(), "3".to_string()),
            ("DISTANCE_METRIC".to_string(), "L2".to_string()),
        ],
        vec![
            ("TYPE".to_string(), "FLOAT32".to_string()),
            ("DIM".to_string(), "0".to_string()),
            ("DISTANCE_METRIC".to_string(), "L2".to_string()),
        ],
        vec![
            ("TYPE".to_string(), "FLOAT32".to_string()),
            ("DIM".to_string(), "3".to_string()),
            ("DISTANCE_METRIC".to_string(), "BAD".to_string()),
        ],
        vec![("TYPE".to_string(), "FLOAT32".to_string())],
        vec![
            ("TYPE".to_string(), "FLOAT32".to_string()),
            ("DIM".to_string(), "3".to_string()),
            ("DISTANCE_METRIC".to_string(), "L2".to_string()),
            ("UNKNOWN".to_string(), "1".to_string()),
        ],
    ] {
        assert!(
            validate_fulltext_vector_options(&FullTextVectorOptions {
                algorithm: FullTextVectorAlgorithm::Flat,
                attributes: bad_attrs,
            })
            .is_err()
        );
    }

    let vector_create = fulltext_vector_create_options(&vector_field).unwrap();
    assert_eq!(vector_create.dim, 3);
    assert_eq!(vector_create.distance, "COSINE");
    assert_eq!(vector_create.m, Some(16));
    assert_eq!(
        fulltext_vector_attr(&vector_options(), "distance_metric").unwrap(),
        "COSINE"
    );
    assert!(fulltext_vector_attr(&vector_options(), "missing").is_err());

    assert_eq!(fulltext_source_type_name(FullTextSourceType::Hash), "HASH");
    assert_eq!(fulltext_source_type_name(FullTextSourceType::Json), "JSON");
    assert_eq!(fulltext_state_name(FullTextIndexState::Dirty), "dirty");
    assert_eq!(
        fulltext_geoshape_coordinate_system_name(FullTextGeoShapeCoordinateSystem::Flat),
        "FLAT"
    );
    assert_eq!(
        fulltext_vector_algorithm_name(FullTextVectorAlgorithm::Hnsw),
        "HNSW"
    );

    assert!(fulltext_supported_config_names().contains(&"DEFAULT_DIALECT"));
    assert_eq!(fulltext_default_config_value("default_dialect"), Some("2"));
    validate_fulltext_config_value("DEFAULT_DIALECT", "4").unwrap();
    validate_fulltext_config_value("MINPREFIX", "1").unwrap();
    validate_fulltext_config_value("CLUSTER_SHARDS", "1").unwrap();
    validate_fulltext_config_value("NOGC", "yes").unwrap();
    validate_fulltext_config_value("CLUSTER_ENABLED", "0").unwrap();
    validate_fulltext_config_value("ON_TIMEOUT", "FAIL").unwrap();
    validate_fulltext_config_value("CLUSTER_ROUTING", "local").unwrap();
    assert!(validate_fulltext_config_value("DEFAULT_DIALECT", "9").is_err());
    assert!(validate_fulltext_config_value("MINPREFIX", "0").is_err());
    assert!(validate_fulltext_config_value("CLUSTER_SHARDS", "0").is_err());
    assert!(validate_fulltext_config_value("NOGC", "maybe").is_err());
    assert!(validate_fulltext_config_value("ON_TIMEOUT", "WAIT").is_err());
    assert!(validate_fulltext_config_value("CLUSTER_ROUTING", "remote").is_err());
    assert!(validate_fulltext_config_value("UNKNOWN", "1").is_err());
}

#[test]
fn text_materialization_display_and_matching_cover_stemming_suffix_phonetic() {
    let settings = FullTextTextFieldSettings {
        nostem: false,
        phonetic: true,
        with_suffix_trie: true,
        stopwords: HashSet::from(["the".to_string()]),
        language: "english".to_string(),
        weight: 1.0,
    };
    let materialized = fulltext_materialize_text("The running boxes Robert", &settings);
    assert!(!materialized.contains("the"));
    assert!(materialized.contains("running"));
    assert!(materialized.contains("run"));
    assert!(materialized.contains("box"));
    assert!(materialized.contains("phon"));
    assert!(materialized.contains("unning"));

    let mut synonyms = HashMap::new();
    synonyms.insert(
        "car".to_string(),
        HashSet::from(["automobile".to_string(), "vehicle".to_string()]),
    );
    let variants = fulltext_query_term_variants("car", Some(&settings), &synonyms);
    assert!(variants.contains(&"car".to_string()));
    assert!(variants.contains(&"automobile".to_string()));
    assert!(variants.contains(&"vehicle".to_string()));
    assert_eq!(
        fulltext_query_term_variants("the", Some(&settings), &HashMap::new()),
        vec!["the"]
    );

    assert_eq!(fulltext_simple_query_term("plain"), Some("plain"));
    assert_eq!(fulltext_simple_query_term("two words"), None);
    assert_eq!(fulltext_stem("stories", "english"), "stori");
    assert_eq!(fulltext_stem("running", "english"), "run");
    assert_eq!(fulltext_soundex("Robert").unwrap(), "R163");
    assert!(fulltext_soundex("123").is_none());
    assert_eq!(
        fulltext_suffix_tokens("search"),
        vec!["earch", "arch", "rch", "ch"]
    );
    assert_eq!(fulltext_edit_distance("kitten", "sitting"), 3);
    assert_eq!(format_fulltext_spellcheck_score(0), "1");
    assert_eq!(format_fulltext_spellcheck_score(3), "0.7");
    assert_eq!(format_fulltext_suggestion_score(3.0), "3");
    assert_eq!(format_fulltext_suggestion_score(3.25), "3.25");

    let mut options = search_options();
    options.summarize = true;
    options.highlight = true;
    let display_terms = fulltext_display_terms("needle");
    let long_text = format!("{} needle {}", "a".repeat(90), "b".repeat(90));
    let displayed = fulltext_display_value(&long_text, &options, &display_terms);
    assert!(displayed.contains("<b>needle</b>"));
    assert!(displayed.starts_with("...") || displayed.ends_with("..."));
    assert_eq!(
        fulltext_highlight_value("Needle needle", &display_terms),
        "<b>Needle</b> <b>needle</b>"
    );

    let fields_frame = fulltext_fields_frame(
        vec![
            ("title".to_string(), "needle text".to_string()),
            ("body".to_string(), "body".to_string()),
        ],
        Some(&[FullTextReturnField {
            identifier: "title".to_string(),
            alias: Some("t".to_string()),
        }]),
        &options,
        &display_terms,
    );
    assert!(fields_frame.to_string().contains("t"));
    assert!(fields_frame.to_string().contains("<b>needle</b>"));
    assert_eq!(
        fulltext_field_value(&[("x".to_string(), "1".to_string())], "x").unwrap(),
        "1"
    );
}

#[test]
fn aggregate_expressions_reducers_sort_and_frames_cover_success_and_errors() {
    let mut first = row(
        "doc:1",
        2.0,
        &[("category", "books"), ("price", "10"), ("title", "Rust")],
    );
    fulltext_aggregate_set_output(
        &mut first,
        "computed".to_string(),
        FullTextAggregateValue::Number(12.0),
    );
    fulltext_aggregate_set_output(
        &mut first,
        "computed".to_string(),
        FullTextAggregateValue::Number(14.0),
    );
    assert_eq!(number_value(first.values.get("__score").unwrap()), 2.0);
    assert_eq!(first.output.len(), 1);

    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("(@price + 5) * 2", &first).unwrap()),
        30.0
    );
    assert_eq!(
        string_value(&eval_fulltext_aggregate_expression("upper(@title)", &first).unwrap()),
        "RUST"
    );
    assert_eq!(
        string_value(&eval_fulltext_aggregate_expression("lower('RUST')", &first).unwrap()),
        "rust"
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("sqrt(9)", &first).unwrap()),
        3.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("ceil(1.2)", &first).unwrap()),
        2.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("floor(1.8)", &first).unwrap()),
        1.0
    );
    assert_eq!(
        number_value(&eval_fulltext_aggregate_expression("abs(-3)", &first).unwrap()),
        3.0
    );
    assert!(eval_fulltext_aggregate_expression("", &first).is_err());
    assert!(eval_fulltext_aggregate_expression("bad(@price)", &first).is_err());
    assert!(eval_fulltext_aggregate_filter("@price >= 10", &first).unwrap());
    assert!(eval_fulltext_aggregate_filter("@title != 'Go'", &first).unwrap());
    assert!(eval_fulltext_aggregate_filter("@title", &first).unwrap());
    assert!(!fulltext_aggregate_value_truthy(
        &FullTextAggregateValue::String("0".to_string())
    ));
    assert!(fulltext_aggregate_value_to_number(&FullTextAggregateValue::List(Vec::new())).is_err());
    assert_eq!(
        fulltext_aggregate_value_to_string(&FullTextAggregateValue::List(vec![
            FullTextAggregateValue::String("a".to_string()),
            FullTextAggregateValue::Number(2.0),
        ])),
        "a,2"
    );

    let rows = vec![
        first.clone(),
        row(
            "doc:2",
            1.0,
            &[("category", "books"), ("price", "20"), ("title", "Go")],
        ),
        row(
            "doc:3",
            3.0,
            &[("category", "games"), ("price", "5"), ("title", "Chess")],
        ),
    ];
    let reducers = vec![
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Count,
            args: Vec::new(),
            alias: Some("n".to_string()),
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Sum,
            args: vec!["@price".to_string()],
            alias: Some("sum_price".to_string()),
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Avg,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Min,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::Max,
            args: vec!["@price".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::CountDistinct,
            args: vec!["@title".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::FirstValue,
            args: vec!["@title".to_string()],
            alias: None,
        },
        FullTextAggregateReducer {
            kind: FullTextAggregateReducerKind::ToList,
            args: vec!["@title".to_string()],
            alias: None,
        },
    ];
    let grouped = fulltext_aggregate_group(rows, &["@category".to_string()], &reducers).unwrap();
    let books = grouped
        .iter()
        .find(|row| string_value(row.values.get("category").unwrap()) == "books")
        .unwrap();
    assert_eq!(number_value(books.values.get("n").unwrap()), 2.0);
    assert_eq!(number_value(books.values.get("sum_price").unwrap()), 30.0);
    assert_eq!(number_value(books.values.get("avg").unwrap()), 15.0);
    assert_eq!(number_value(books.values.get("min").unwrap()), 10.0);
    assert_eq!(number_value(books.values.get("max").unwrap()), 20.0);
    assert_eq!(
        number_value(books.values.get("count_distinct").unwrap()),
        2.0
    );
    assert_eq!(
        string_value(books.values.get("first_value").unwrap()),
        "Rust"
    );
    assert_eq!(
        fulltext_aggregate_value_to_string(books.values.get("tolist").unwrap()),
        "Rust,Go"
    );

    let missing_arg = FullTextAggregateReducer {
        kind: FullTextAggregateReducerKind::Sum,
        args: Vec::new(),
        alias: None,
    };
    assert!(fulltext_aggregate_reduce(&missing_arg, &[first.clone()]).is_err());

    let mut sorted = [row("doc:2", 1.0, &[("price", "2")]),
        row("doc:1", 1.0, &[("price", "10")])];
    sorted.sort_by(|left, right| {
        compare_fulltext_aggregate_rows(
            left,
            right,
            &[FullTextAggregateSortBy {
                field: "@price".to_string(),
                asc: true,
            }],
        )
    });
    assert_eq!(
        fulltext_aggregate_value_to_string(sorted[0].values.get("__key").unwrap()),
        "doc:2"
    );

    let frame = fulltext_aggregate_frame(1, vec![books.clone()]);
    assert!(frame.to_string().contains("sum_price"));
    assert!(matches!(
        fulltext_aggregate_value_frame(FullTextAggregateValue::Null),
        Frame::Null
    ));
    assert_eq!(normalize_fulltext_aggregate_field("@price"), "price");
}

#[test]
fn aggregate_cursors_enforce_idle_and_memory_limits() {
    let cursor_id = register_fulltext_aggregate_cursor(
        0,
        "idx",
        vec![row("doc:1", 1.0, &[("title", "rust")])],
        1,
        usize::MAX,
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert!(read_fulltext_aggregate_cursor(0, "idx", cursor_id, 1).is_err());

    assert!(
        register_fulltext_aggregate_cursor(
            0,
            "idx",
            vec![row("doc:2", 1.0, &[("title", "rust")])],
            300_000,
            1,
        )
        .is_err()
    );
}

#[test]
fn geo_geoshape_numeric_filter_and_ast_matching_cover_edges() {
    assert!(fulltext_geo_value_within("-122.0,37.0", -122.0, 37.0, 1.0, "m").unwrap());
    assert!(fulltext_geo_value_within("-122.0 37.0", -122.1, 37.0, 20.0, "km").unwrap());
    assert!(fulltext_geo_value_within("-122.0 37.0", -122.1, 37.0, 10.0, "ft").is_ok());
    assert!(fulltext_geo_value_within("-122.0 37.0", -122.1, 37.0, 10.0, "mi").is_ok());
    assert!(fulltext_geo_value_within("-122.0 37.0", -122.1, 37.0, -1.0, "m").is_err());
    assert!(parse_fulltext_geo_value("bad").is_err());
    assert!(fulltext_geo_unit_meters("bad").is_err());
    assert!(fulltext_haversine_meters(0.0, 0.0, 0.0, 0.0).abs() < 1e-6);

    let point = parse_fulltext_wkt("POINT(1 1)").unwrap();
    let poly = parse_fulltext_wkt("POLYGON((0 0,0 2,2 2,2 0,0 0))").unwrap();
    assert!(fulltext_geometry_within(&point, &poly));
    assert!(fulltext_geometry_contains(&poly, &point));
    assert!(
        fulltext_geoshape_relation_matches(
            "POINT(1 1)",
            "WITHIN",
            "POLYGON((0 0,0 2,2 2,2 0,0 0))"
        )
        .unwrap()
    );
    assert!(parse_fulltext_wkt("LINESTRING(0 0,1 1)").is_err());
    assert!(parse_fulltext_wkt("POLYGON((0 0,1 1,0 0))").is_err());
    assert!(parse_fulltext_wkt_point("1").is_err());
    assert!(fulltext_geoshape_relation_matches("POINT(1 1)", "BAD", "POINT(1 1)").is_err());

    assert!(fulltext_numeric_bound_allows(
        5.0,
        FullTextNumericBound::Inclusive(5.0),
        true
    ));
    assert!(!fulltext_numeric_bound_allows(
        5.0,
        FullTextNumericBound::Exclusive(5.0),
        true
    ));
    assert!(fulltext_bound_allows(
        5.0,
        FullTextSearchBound::Inclusive(5.0),
        false
    ));
    assert!(!fulltext_bound_allows(
        5.0,
        FullTextSearchBound::Exclusive(5.0),
        false
    ));

    let schema_meta = meta(vec![
        text_field("title"),
        field("tag", FullTextFieldKind::Tag),
        field("price", FullTextFieldKind::Numeric),
        field("loc", FullTextFieldKind::Geo),
        {
            let mut field = field("shape", FullTextFieldKind::GeoShape);
            field.options.geoshape_coordinate_system = Some(FullTextGeoShapeCoordinateSystem::Flat);
            field
        },
    ]);
    let fields = vec![
        ("title".to_string(), "running rust search".to_string()),
        ("tag".to_string(), "book,tech".to_string()),
        ("price".to_string(), "10".to_string()),
        ("loc".to_string(), "-122.0,37.0".to_string()),
        ("shape".to_string(), "POINT(1 1)".to_string()),
    ];
    let options = search_options();
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Text("run".to_string()),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Phrase("rust search".to_string()),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Prefix("ru".to_string()),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Wildcard("r*st".to_string()),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Fuzzy("serch".to_string()),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Tag {
                field: "tag".to_string(),
                values: vec!["tech".to_string()],
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Numeric {
                field: "price".to_string(),
                min: FullTextNumericBound::Inclusive(5.0),
                max: FullTextNumericBound::Exclusive(11.0),
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Geo {
                field: "loc".to_string(),
                lon: -122.0,
                lat: 37.0,
                radius: 1.0,
                unit: "m".to_string(),
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::GeoShape {
                field: "shape".to_string(),
                relation: "WITHIN".to_string(),
                shape: "POLYGON((0 0,0 2,2 2,2 0,0 0))".to_string(),
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::And(vec![
                FullTextQueryAst::Text("rust".to_string()),
                FullTextQueryAst::Not(Box::new(FullTextQueryAst::Text("java".to_string()))),
            ]),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Field {
                fields: vec!["title".to_string()],
                expr: Box::new(FullTextQueryAst::Text("rust".to_string())),
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        fulltext_eval_ast_against_fields(
            &FullTextQueryAst::Optional(Box::new(FullTextQueryAst::Text("missing".to_string()))),
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );
    assert!(
        !fulltext_eval_ast_against_fields(
            &FullTextQueryAst::VectorRange {
                field: "vec".to_string(),
                radius: 1.0,
                blob_param: "q".to_string(),
            },
            &fields,
            &schema_meta,
            &options
        )
        .unwrap()
    );

    assert!(fulltext_fields_match_filters(
        &fields,
        &[FullTextSearchNumericFilter {
            field: "price".to_string(),
            min: FullTextSearchBound::Inclusive(1.0),
            max: FullTextSearchBound::PosInf,
        }]
    ));
    assert!(
        fulltext_fields_match_geo_filters(
            &fields,
            &[FullTextSearchGeoFilter {
                field: "loc".to_string(),
                lon: -122.0,
                lat: 37.0,
                radius: 1.0,
                unit: "m".to_string(),
            }]
        )
        .unwrap()
    );
    fulltext_validate_search_geo_filters(
        &schema_meta,
        &[FullTextSearchGeoFilter {
            field: "loc".to_string(),
            lon: -122.0,
            lat: 37.0,
            radius: 1.0,
            unit: "km".to_string(),
        }],
    )
    .unwrap();
    assert!(
        fulltext_validate_search_geo_filters(
            &schema_meta,
            &[FullTextSearchGeoFilter {
                field: "title".to_string(),
                lon: -122.0,
                lat: 37.0,
                radius: 1.0,
                unit: "km".to_string(),
            }],
        )
        .is_err()
    );
    fulltext_validate_geo_query_ast(
        &schema_meta,
        &FullTextQueryAst::GeoShape {
            field: "shape".to_string(),
            relation: "WITHIN".to_string(),
            shape: "POINT(1 1)".to_string(),
        },
    )
    .unwrap();
    assert!(contains_fulltext_geo_query(&FullTextQueryAst::Geo {
        field: "loc".to_string(),
        lon: 0.0,
        lat: 0.0,
        radius: 1.0,
        unit: "m".to_string(),
    }));
}

#[test]
fn json_paths_indexing_encoding_and_legacy_decode_are_covered() {
    let value = serde_json::json!({
        "items": [
            {"name": "book", "price": 10, "tags": ["a", true]},
            {"name": "pen", "price": "2.5"}
        ],
        "flag": false
    });
    let tokens = parse_fulltext_json_path("$.items[*].name").unwrap();
    assert_eq!(
        fulltext_json_path_values(&value, &tokens)
            .into_iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["book", "pen"]
    );
    assert_eq!(parse_fulltext_json_path("$").unwrap(), Vec::new());
    assert_eq!(parse_fulltext_json_path(".").unwrap(), Vec::new());
    assert!(parse_fulltext_json_path("$.").is_err());
    assert!(parse_fulltext_json_path("$[x]").is_err());
    assert!(parse_fulltext_json_path("$[*x]").is_err());

    assert_eq!(
        json_index_strings(&serde_json::json!(["x", 1, true, {"skip": 1}])),
        vec!["x", "1", "true"]
    );
    assert_eq!(
        json_index_tag_values(&serde_json::json!(["x", 1, false])),
        vec!["x", "1", "false"]
    );
    assert_eq!(
        json_index_numeric_values(&serde_json::json!(["2.5", 4, "bad", false])),
        vec!["2.5", "4"]
    );

    let schema = vec![
        text_field("title"),
        {
            let mut tag = field("tag", FullTextFieldKind::Tag);
            tag.options.alias = Some("t".to_string());
            tag
        },
        {
            let mut vector = field("vec", FullTextFieldKind::Vector);
            vector.options.vector = Some(vector_options());
            vector
        },
    ];
    let schema_frame = fulltext_schema_frame(&schema);
    let schema_text = schema_frame.to_string();
    assert!(schema_text.contains("title"));
    assert!(schema_text.contains("VECTOR"));
    assert!(schema_text.contains("HNSW"));

    let meta_record = meta(schema.clone());
    let encoded = encode_record(&meta_record).unwrap();
    let decoded = decode_fulltext_meta(&encoded).unwrap();
    assert_eq!(decoded.schema.len(), 3);

    let legacy = LegacyFullTextIndexMeta {
        prefixes: vec!["doc:".to_string()],
        schema: vec![LegacyFullTextFieldSchema {
            name: "legacy".to_string(),
            kind: FullTextFieldKind::Text,
        }],
        state: FullTextIndexState::Ready,
        generation: 7,
        backfill_cursor: None,
        last_indexed_outbox_seq: 9,
        refresh_policy: FullTextRefreshPolicy::default(),
    };
    let decoded_legacy = decode_fulltext_meta(&encode_record(&legacy).unwrap()).unwrap();
    assert_eq!(decoded_legacy.source_type, FullTextSourceType::Hash);
    assert_eq!(decoded_legacy.schema[0].name, "legacy");

    let legacy_phase2 = LegacyPhase2FullTextIndexMeta {
        source_type: FullTextSourceType::Json,
        prefixes: vec!["json:".to_string()],
        schema: vec![LegacyPhase2FullTextFieldSchema {
            name: "$.title".to_string(),
            kind: FullTextFieldKind::Text,
            options: LegacyPhase2FullTextFieldOptions {
                alias: Some("title".to_string()),
                sortable: true,
                noindex: false,
                weight: Some(2.0),
            },
        }],
        aliases: vec!["alias".to_string()],
        index_options: LegacyPhase2FullTextIndexOptions {
            skip_initial_scan: true,
        },
        state: FullTextIndexState::Backfilling,
        generation: 3,
        backfill_cursor: Some("cursor".to_string()),
        last_indexed_outbox_seq: 4,
        refresh_policy: FullTextRefreshPolicy::default(),
    };
    let decoded_phase2 = decode_fulltext_meta(&encode_record(&legacy_phase2).unwrap()).unwrap();
    assert_eq!(decoded_phase2.source_type, FullTextSourceType::Json);
    assert_eq!(decoded_phase2.schema[0].attribute_name(), "title");
    assert!(decode_fulltext_meta(b"not-bincode").is_err());
}

#[test]
fn storage_key_helpers_round_trip_and_reject_bad_suffixes() {
    assert_eq!(
        fulltext_alias_from_key(3, &fulltext_alias_key(3, "alias")).unwrap(),
        "alias"
    );
    assert_eq!(
        fulltext_alias_from_key(4, &fulltext_alias_key(3, "alias")).unwrap(),
        "alias"
    );
    assert_eq!(
        fulltext_dict_term_from_key(1, "dict", &fulltext_dict_term_key(1, "dict", "term")).unwrap(),
        "term"
    );
    assert_eq!(
        fulltext_any_dict_term_from_key(1, &fulltext_dict_term_key(1, "dict", "term")).unwrap(),
        "term"
    );
    assert_eq!(
        fulltext_suggest_string_from_key(2, "sug", &fulltext_suggest_key(2, "sug", "value"))
            .unwrap(),
        "value"
    );
    assert_eq!(
        fulltext_syn_group_from_key(2, "idx", &fulltext_syn_key(2, "idx", "group")).unwrap(),
        "group"
    );
    assert_eq!(
        fulltext_index_from_meta_key(5, &fulltext_meta_key(5, "idx")).unwrap(),
        "idx"
    );
    let mut bad_meta_key = fulltext_meta_key(5, "idx");
    bad_meta_key.pop();
    assert!(fulltext_index_from_meta_key(5, &bad_meta_key).is_none());
    assert_eq!(
        fulltext_outbox_seq_from_key(7, "idx", &fulltext_outbox_key(7, "idx", 42)).unwrap(),
        42
    );
    let mut bad_outbox = fulltext_outbox_key(7, "idx", 42);
    bad_outbox.push(0);
    assert!(fulltext_outbox_seq_from_key(7, "idx", &bad_outbox).is_none());
    assert!(fulltext_file_prefix(1, "idx").starts_with(&internal_prefix(1)));
    assert!(fulltext_repair_marker_key(1, "idx").starts_with(&fulltext_meta_prefix(1)));
    assert!(fulltext_config_key(1, "DEFAULT_DIALECT").starts_with(&fulltext_meta_prefix(1)));

    let first = new_fulltext_sequence();
    let second = new_fulltext_sequence();
    assert!(second >= first);
    assert!(current_fulltext_millis() > 0);
}

#[test]
fn runtime_indexes_searches_deletes_synonyms_and_registry_paths() {
    let store = test_store("runtime");
    store.put_raw(
        &fulltext_syn_key(0, "idx", "g1"),
        &encode_record(&FullTextSynonymGroup {
            terms: vec!["car".to_string(), "automobile".to_string()],
        })
        .unwrap(),
    );

    let meta = meta(vec![
        {
            let mut title = text_field("title");
            title.options.alias = Some("t".to_string());
            title.options.phonetic = Some("dm:en".to_string());
            title.options.with_suffix_trie = true;
            title
        },
        field("tag", FullTextFieldKind::Tag),
        field("price", FullTextFieldKind::Numeric),
        {
            let mut ignored = text_field("ignored");
            ignored.options.noindex = true;
            ignored
        },
    ]);
    let mut runtime = FullTextRuntime::new(store.clone(), 0, "idx", "idx", &meta).unwrap();
    assert_eq!(runtime.synonyms.get("car").unwrap().len(), 1);
    assert!(runtime.refresh_due(&FullTextRefreshPolicy {
        refresh_interval_ms: 0,
        ..FullTextRefreshPolicy::default()
    }));
    assert!(!runtime.refresh_due(&FullTextRefreshPolicy {
        refresh_interval_ms: 60_000,
        ..FullTextRefreshPolicy::default()
    }));

    runtime
        .upsert_hash(
            "doc:1",
            &[
                ("title".to_string(), "fast automobile".to_string()),
                ("tag".to_string(), "vehicle".to_string()),
                ("price".to_string(), "10".to_string()),
                ("ignored".to_string(), "secret".to_string()),
            ],
        )
        .unwrap();
    runtime
        .upsert_fields(
            "doc:2",
            &[
                ("title".to_string(), "slow train".to_string()),
                ("tag".to_string(), "rail".to_string()),
                ("price".to_string(), "25".to_string()),
            ],
        )
        .unwrap();
    runtime.publish().unwrap();

    let options = search_options();
    assert_eq!(runtime.search("*", &options, None).unwrap().hits.len(), 2);
    assert_eq!(
        runtime.search("car", &options, None).unwrap().hits[0].key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search_ast(
                &FullTextQueryAst::Tag {
                    field: "tag".to_string(),
                    values: vec!["rail".to_string()],
                },
                &options
            )
            .unwrap()[0]
            .key,
        "doc:2"
    );
    assert_eq!(
        runtime
            .search_ast(
                &FullTextQueryAst::Numeric {
                    field: "price".to_string(),
                    min: FullTextNumericBound::Inclusive(9.0),
                    max: FullTextNumericBound::Exclusive(11.0),
                },
                &options,
            )
            .unwrap()[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime.search("@t:fast", &options, None).unwrap().hits[0].key,
        "doc:1"
    );
    assert_eq!(
        runtime.search("auto*", &options, None).unwrap().hits[0].key,
        "doc:1"
    );
    assert_eq!(
        runtime.search("a?tomobile", &options, None).unwrap().hits[0].key,
        "doc:1"
    );
    assert_eq!(
        runtime.search("%automobiel%", &options, None).unwrap().hits[0].key,
        "doc:1"
    );
    assert!(runtime.build_query("\"fast automobile\"", &options).is_ok());

    let scoped = FullTextQueryAst::Field {
        fields: vec!["t".to_string()],
        expr: Box::new(FullTextQueryAst::Text("train".to_string())),
    };
    assert_eq!(
        runtime.search_ast(&scoped, &options).unwrap()[0].key,
        "doc:2"
    );
    assert!(
        runtime
            .plan_text_query("x", Some(&["price".to_string()]), &options)
            .is_err()
    );
    assert!(
        runtime
            .plan_tag_query("missing", &["x".to_string()])
            .is_err()
    );
    assert!(
        runtime
            .plan_numeric_query(
                "tag",
                FullTextNumericBound::NegInf,
                FullTextNumericBound::PosInf,
            )
            .is_err()
    );
    assert!(
        runtime
            .plan_boolean(&[], Occur::Must, None, &options)
            .is_ok()
    );
    assert!(
        runtime
            .plan_query(
                &FullTextQueryAst::Geo {
                    field: "loc".to_string(),
                    lon: 0.0,
                    lat: 0.0,
                    radius: 1.0,
                    unit: "m".to_string(),
                },
                None,
                &options,
            )
            .is_err()
    );
    assert!(
        runtime
            .plan_query(
                &FullTextQueryAst::VectorRange {
                    field: "vec".to_string(),
                    radius: 1.0,
                    blob_param: "q".to_string(),
                },
                None,
                &options,
            )
            .is_err()
    );

    runtime.delete_hash("doc:1");
    runtime.publish().unwrap();
    assert!(
        runtime
            .search("car", &options, None)
            .unwrap()
            .hits
            .is_empty()
    );

    let registry = FullTextRuntimeRegistry::default();
    registry.insert(0, "idx", runtime);
    assert!(registry.get(0, "idx").is_some());
    registry.remove(0, "idx");
    assert!(registry.get(0, "idx").is_none());

    let runtime = FullTextRuntime::new(store, 0, "idx2", "idx2", &meta).unwrap();
    registry.insert(0, "idx2", runtime);
    assert!(registry.get(0, "idx2").is_some());
    registry.remove_db(0);
    assert!(registry.get(0, "idx2").is_none());
}

#[test]
fn alter_runtime_failure_rolls_back_schema_generation_and_runtime() {
    let store = test_store("alter-rollback");
    let version_counter = Arc::new(crate::store::ttl::VersionCounter::new());
    let ttl_manager =
        crate::store::ttl::TtlManager::new(store.clone(), crate::store::ttl::TtlConfig::default());
    let db = Db::new(0, store, version_counter, ttl_manager);
    db.fulltext_create(
        "idx",
        FullTextCreateOptions {
            source_type: FullTextSourceType::Hash,
            prefixes: vec!["doc:".to_string()],
            schema: vec![text_field("title")],
            index_options: FullTextIndexOptions::default(),
        },
    )
    .unwrap();
    db.hash_set("doc:1", "title", "alpha").unwrap();
    let before = db.read_fulltext_meta_direct("idx").unwrap();

    FULLTEXT_ALTER_FAIL_AFTER_SWAP.store(true, AtomicOrdering::SeqCst);
    let error = match db.fulltext_alter("idx", vec![text_field("body")]) {
        Ok(_) => panic!("injected FT.ALTER failure should roll back"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("injected FT.ALTER runtime failure")
    );

    let after = db.read_fulltext_meta_direct("idx").unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.schema.len(), 1);
    assert_eq!(after.schema[0].name, "title");
    assert_eq!(
        fulltext_state_name(after.state),
        fulltext_state_name(before.state)
    );
    assert_eq!(db.fulltext_active_storage_name("idx", &after), "idx");
    assert!(db.fulltext_runtimes.get(0, "idx").is_some());
}
