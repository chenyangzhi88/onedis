fn fulltext_explain_ast_lines(ast: &FullTextQueryAst) -> Vec<String> {
    let mut lines = Vec::new();
    fulltext_explain_ast_into(ast, 0, &mut lines);
    lines
}

fn fulltext_explain_ast_into(ast: &FullTextQueryAst, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    match ast {
        FullTextQueryAst::All => lines.push(format!("{indent}ALL")),
        FullTextQueryAst::Text(term) => lines.push(format!("{indent}TEXT \"{term}\"")),
        FullTextQueryAst::Phrase(phrase) => lines.push(format!("{indent}PHRASE \"{phrase}\"")),
        FullTextQueryAst::Prefix(prefix) => lines.push(format!("{indent}PREFIX \"{prefix}\"")),
        FullTextQueryAst::Wildcard(pattern) => {
            lines.push(format!("{indent}WILDCARD \"{pattern}\""))
        }
        FullTextQueryAst::Fuzzy(term) => lines.push(format!("{indent}FUZZY \"{term}\"")),
        FullTextQueryAst::Tag { field, values } => {
            lines.push(format!("{indent}TAG @{field} {{{}}}", values.join("|")))
        }
        FullTextQueryAst::Numeric { field, min, max } => lines.push(format!(
            "{indent}NUMERIC @{field} [{} {}]",
            fulltext_explain_numeric_bound(*min),
            fulltext_explain_numeric_bound(*max)
        )),
        FullTextQueryAst::Geo {
            field,
            lon,
            lat,
            radius,
            unit,
        } => lines.push(format!(
            "{indent}GEO @{field} [{lon} {lat} {radius} {unit}]"
        )),
        FullTextQueryAst::GeoShape {
            field,
            relation,
            shape,
        } => lines.push(format!("{indent}GEOSHAPE @{field} [{relation} {shape}]")),
        FullTextQueryAst::VectorKnn {
            filter,
            k,
            field,
            blob_param,
        } => {
            lines.push(format!(
                "{indent}VECTOR_KNN @{field} K={k} PARAM=${blob_param}"
            ));
            fulltext_explain_ast_into(filter, depth + 1, lines);
        }
        FullTextQueryAst::VectorRange {
            field,
            radius,
            blob_param,
        } => lines.push(format!(
            "{indent}VECTOR_RANGE @{field} RADIUS={radius} PARAM=${blob_param}"
        )),
        FullTextQueryAst::Field { fields, expr } => {
            lines.push(format!("{indent}FIELD {}", fields.join("|")));
            fulltext_explain_ast_into(expr, depth + 1, lines);
        }
        FullTextQueryAst::And(children) => {
            lines.push(format!("{indent}INTERSECT"));
            for child in children {
                fulltext_explain_ast_into(child, depth + 1, lines);
            }
        }
        FullTextQueryAst::Or(children) => {
            lines.push(format!("{indent}UNION"));
            for child in children {
                fulltext_explain_ast_into(child, depth + 1, lines);
            }
        }
        FullTextQueryAst::Not(child) => {
            lines.push(format!("{indent}NOT"));
            fulltext_explain_ast_into(child, depth + 1, lines);
        }
        FullTextQueryAst::Optional(child) => {
            lines.push(format!("{indent}OPTIONAL"));
            fulltext_explain_ast_into(child, depth + 1, lines);
        }
        FullTextQueryAst::Attributed { expr, weight } => {
            lines.push(format!(
                "{indent}ATTRIBUTES weight={}",
                weight
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "1".to_string())
            ));
            fulltext_explain_ast_into(expr, depth + 1, lines);
        }
    }
}

fn fulltext_explain_numeric_bound(bound: FullTextNumericBound) -> String {
    match bound {
        FullTextNumericBound::NegInf => "-inf".to_string(),
        FullTextNumericBound::PosInf => "+inf".to_string(),
        FullTextNumericBound::Inclusive(value) => value.to_string(),
        FullTextNumericBound::Exclusive(value) => format!("({value}"),
    }
}
