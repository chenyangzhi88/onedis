#[derive(Clone, Debug)]
enum FilterPredicate {
    TagEq(String, String),
    TagIn(String, Vec<String>),
    NumericCmp(String, NumericOp, f64),
}

#[derive(Clone, Copy, Debug)]
enum NumericOp {
    Gt,
    Ge,
    Lt,
    Le,
}
