use super::super::*;

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
