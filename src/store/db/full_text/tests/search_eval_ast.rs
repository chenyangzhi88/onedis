use super::super::*;
use super::support::*;

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

    let strict_poly = parse_fulltext_wkt("POLYGON((0 0,4 0,4 4,0 4,0 0))").unwrap();
    for boundary in [(0.0, 1.0), (1.0, 0.0), (4.0, 2.0), (2.0, 4.0)] {
        assert!(!fulltext_geometry_within(
            &FullTextGeometry::Point(boundary),
            &strict_poly
        ));
    }
    assert!(fulltext_geometry_within(
        &FullTextGeometry::Point((0.1, 0.1)),
        &strict_poly
    ));

    let point_cells = fulltext_geoshape_cells((1.0, 1.0, 1.0, 1.0)).unwrap();
    assert_eq!(point_cells, vec!["4:4"]);
    let local_cells = fulltext_geoshape_cells((0.0, 0.5, 0.0, 0.5)).unwrap();
    assert_eq!(local_cells.len(), 9);
    assert!(local_cells.iter().any(|cell| cell == "0:0"));
    assert!(local_cells.iter().any(|cell| cell == "2:2"));
    assert!(fulltext_geoshape_cells((-180.0, 180.0, -90.0, 90.0)).is_none());

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
