fn fulltext_numeric_bound_allows(value: f64, bound: FullTextNumericBound, lower: bool) -> bool {
    match (bound, lower) {
        (FullTextNumericBound::NegInf, true) | (FullTextNumericBound::PosInf, false) => true,
        (FullTextNumericBound::NegInf, false) => false,
        (FullTextNumericBound::PosInf, true) => false,
        (FullTextNumericBound::Inclusive(bound), true) => value >= bound,
        (FullTextNumericBound::Inclusive(bound), false) => value <= bound,
        (FullTextNumericBound::Exclusive(bound), true) => value > bound,
        (FullTextNumericBound::Exclusive(bound), false) => value < bound,
    }
}

fn fulltext_bound_allows(value: f64, bound: FullTextSearchBound, lower: bool) -> bool {
    match (bound, lower) {
        (FullTextSearchBound::NegInf, true) | (FullTextSearchBound::PosInf, false) => true,
        (FullTextSearchBound::NegInf, false) => false,
        (FullTextSearchBound::PosInf, true) => false,
        (FullTextSearchBound::Inclusive(bound), true) => value >= bound,
        (FullTextSearchBound::Inclusive(bound), false) => value <= bound,
        (FullTextSearchBound::Exclusive(bound), true) => value > bound,
        (FullTextSearchBound::Exclusive(bound), false) => value < bound,
    }
}
