fn contains_fulltext_vector_query(ast: &FullTextQueryAst) -> bool {
    match ast {
        FullTextQueryAst::VectorKnn { .. } | FullTextQueryAst::VectorRange { .. } => true,
        FullTextQueryAst::Field { expr, .. }
        | FullTextQueryAst::Not(expr)
        | FullTextQueryAst::Optional(expr)
        | FullTextQueryAst::Attributed { expr, .. } => contains_fulltext_vector_query(expr),
        FullTextQueryAst::And(children) | FullTextQueryAst::Or(children) => {
            children.iter().any(contains_fulltext_vector_query)
        }
        FullTextQueryAst::All
        | FullTextQueryAst::Text(_)
        | FullTextQueryAst::Phrase(_)
        | FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::Geo { .. }
        | FullTextQueryAst::GeoShape { .. } => false,
    }
}

fn contains_fulltext_geo_query(ast: &FullTextQueryAst) -> bool {
    match ast {
        FullTextQueryAst::Geo { .. } | FullTextQueryAst::GeoShape { .. } => true,
        FullTextQueryAst::Field { expr, .. }
        | FullTextQueryAst::Not(expr)
        | FullTextQueryAst::Optional(expr)
        | FullTextQueryAst::Attributed { expr, .. } => contains_fulltext_geo_query(expr),
        FullTextQueryAst::And(children) | FullTextQueryAst::Or(children) => {
            children.iter().any(contains_fulltext_geo_query)
        }
        FullTextQueryAst::All
        | FullTextQueryAst::Text(_)
        | FullTextQueryAst::Phrase(_)
        | FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::VectorKnn { .. }
        | FullTextQueryAst::VectorRange { .. } => false,
    }
}

fn fulltext_query_has_vector_syntax(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    upper.contains("KNN") || upper.contains("VECTOR_RANGE")
}
