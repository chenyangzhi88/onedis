use super::super::*;
use super::support::*;

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
    validate_fulltext_config_value("CONSISTENCY", "EVENTUAL").unwrap();
    validate_fulltext_config_value("MERGE_DELETE_RATIO", "0.25").unwrap();
    validate_fulltext_config_value("CLUSTER_ROUTING", "local").unwrap();
    assert!(validate_fulltext_config_value("DEFAULT_DIALECT", "9").is_err());
    assert!(validate_fulltext_config_value("MINPREFIX", "0").is_err());
    assert!(validate_fulltext_config_value("CLUSTER_SHARDS", "0").is_err());
    assert!(validate_fulltext_config_value("NOGC", "maybe").is_err());
    assert!(validate_fulltext_config_value("ON_TIMEOUT", "WAIT").is_err());
    assert!(validate_fulltext_config_value("CONSISTENCY", "STALE").is_err());
    assert!(validate_fulltext_config_value("MERGE_DELETE_RATIO", "0").is_err());
    assert!(validate_fulltext_config_value("CLUSTER_ROUTING", "remote").is_err());
    assert!(validate_fulltext_config_value("UNKNOWN", "1").is_err());
}
