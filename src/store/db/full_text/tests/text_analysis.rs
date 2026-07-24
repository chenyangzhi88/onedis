use super::super::*;
use super::support::*;

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
    let (source, variants) = fulltext_materialize_text("The running boxes Robert", &settings);
    assert!(source.split_whitespace().any(|token| token == "the"));
    assert!(!variants.split_whitespace().any(|token| token == "the"));
    assert!(source.contains("running"));
    assert!(variants.contains("run"));
    assert!(variants.contains("box"));
    assert!(variants.contains("phon"));
    assert!(variants.contains("unning"));

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
    options.summarize = Some(FullTextSummarizeOptions::default());
    options.highlight = Some(FullTextHighlightOptions::default());
    let display_terms = fulltext_display_terms("needle");
    let long_text = format!("{} needle {}", "a".repeat(90), "b".repeat(90));
    let displayed = fulltext_display_value("title", &long_text, &options, &display_terms);
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
