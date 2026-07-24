use super::super::*;

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
