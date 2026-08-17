use super::*;
pub(super) fn contains_fulltext_vector_query(ast: &FullTextQueryAst) -> bool {
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
        | FullTextQueryAst::Missing { .. }
        | FullTextQueryAst::Geo { .. }
        | FullTextQueryAst::GeoShape { .. } => false,
    }
}

pub(super) fn contains_fulltext_geo_query(ast: &FullTextQueryAst) -> bool {
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
        | FullTextQueryAst::Missing { .. }
        | FullTextQueryAst::VectorKnn { .. }
        | FullTextQueryAst::VectorRange { .. } => false,
    }
}

pub(super) fn fulltext_query_requires_source_validation(
    ast: &FullTextQueryAst,
    options: &FullTextSearchOptions,
) -> bool {
    if options.inorder || options.slop.is_some() {
        return true;
    }
    match ast {
        FullTextQueryAst::Attributed {
            expr,
            slop,
            inorder,
            ..
        } => {
            slop.is_some()
                || inorder.is_some()
                || fulltext_query_requires_source_validation(expr, options)
        }
        FullTextQueryAst::Field { expr, .. }
        | FullTextQueryAst::Not(expr)
        | FullTextQueryAst::Optional(expr) => {
            fulltext_query_requires_source_validation(expr, options)
        }
        FullTextQueryAst::And(children) | FullTextQueryAst::Or(children) => children
            .iter()
            .any(|child| fulltext_query_requires_source_validation(child, options)),
        FullTextQueryAst::All
        | FullTextQueryAst::Text(_)
        | FullTextQueryAst::Phrase(_)
        | FullTextQueryAst::Prefix(_)
        | FullTextQueryAst::Wildcard(_)
        | FullTextQueryAst::Fuzzy(_)
        | FullTextQueryAst::Tag { .. }
        | FullTextQueryAst::Numeric { .. }
        | FullTextQueryAst::Missing { .. }
        | FullTextQueryAst::Geo { .. }
        | FullTextQueryAst::GeoShape { .. }
        | FullTextQueryAst::VectorKnn { .. }
        | FullTextQueryAst::VectorRange { .. } => false,
    }
}

pub(super) fn fulltext_query_has_vector_syntax(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    upper.contains("KNN") || upper.contains("VECTOR_RANGE")
}
