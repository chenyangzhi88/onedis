fn substitute_fulltext_params(
    query: &str,
    params: &HashMap<String, Vec<u8>>,
) -> Result<String, Error> {
    if params.is_empty() {
        return Ok(query.to_string());
    }
    let mut out = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next.is_ascii_alphanumeric() || next == '_' {
                name.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            out.push('$');
        } else if let Some(value) = params.get(&name) {
            let value = std::str::from_utf8(value)
                .map_err(|_| Error::msg("ERR invalid query parameter"))?;
            out.push_str(value);
        } else {
            return Err(Error::msg("ERR missing query parameter"));
        }
    }
    Ok(out)
}
