struct FullTextQueryParser<'a> {
    input: &'a str,
    idx: usize,
    dialect: u8,
}

impl<'a> FullTextQueryParser<'a> {
    fn new(input: &'a str, dialect: u8) -> Self {
        Self {
            input,
            idx: 0,
            dialect,
        }
    }

    fn parse(mut self) -> Result<FullTextQueryAst, Error> {
        self.skip_ws();
        if self.eof() {
            return Ok(FullTextQueryAst::All);
        }
        let ast = self.parse_or()?;
        self.skip_ws();
        if !self.eof() {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(ast)
    }

    fn parse_or(&mut self) -> Result<FullTextQueryAst, Error> {
        let mut children = vec![self.parse_and()?];
        loop {
            self.skip_ws();
            if !self.consume_char('|') {
                break;
            }
            children.push(self.parse_and()?);
        }
        Ok(if children.len() == 1 {
            children.remove(0)
        } else {
            FullTextQueryAst::Or(children)
        })
    }

    fn parse_and(&mut self) -> Result<FullTextQueryAst, Error> {
        let mut children = Vec::new();
        loop {
            self.skip_ws();
            if self.eof() || self.peek_char() == Some(')') || self.peek_char() == Some('|') {
                break;
            }
            children.push(self.parse_unary()?);
            self.skip_ws();
        }
        if children.is_empty() {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(if children.len() == 1 {
            children.remove(0)
        } else {
            FullTextQueryAst::And(children)
        })
    }

    fn parse_unary(&mut self) -> Result<FullTextQueryAst, Error> {
        self.skip_ws();
        if self.consume_char('-') {
            let child = if self.dialect <= 1 {
                self.parse_postfix()?
            } else {
                self.parse_unary()?
            };
            return Ok(FullTextQueryAst::Not(Box::new(child)));
        }
        if self.consume_char('~') {
            return Ok(FullTextQueryAst::Optional(Box::new(self.parse_unary()?)));
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<FullTextQueryAst, Error> {
        let mut expr = self.parse_primary()?;
        loop {
            self.skip_ws();
            if !self.consume_str("=>") {
                break;
            }
            self.skip_ws();
            if self.peek_char() == Some('[') {
                expr = self.parse_vector_knn(expr)?;
            } else if self.peek_char() == Some('{') {
                let attrs = self.read_balanced('{', '}')?;
                expr = FullTextQueryAst::Attributed {
                    expr: Box::new(expr),
                    weight: parse_query_attribute_weight(&attrs)?,
                };
            } else {
                return Err(Error::msg("ERR syntax error"));
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<FullTextQueryAst, Error> {
        self.skip_ws();
        match self.peek_char() {
            Some('*') => {
                self.advance_char();
                Ok(FullTextQueryAst::All)
            }
            Some('(') => {
                self.advance_char();
                let ast = self.parse_or()?;
                self.skip_ws();
                if !self.consume_char(')') {
                    return Err(Error::msg("ERR syntax error"));
                }
                Ok(ast)
            }
            Some('@') => self.parse_field_modifier(),
            Some('"') => Ok(FullTextQueryAst::Phrase(self.read_quoted()?)),
            Some('%') => self.parse_fuzzy(),
            Some(')') | Some('|') | None => Err(Error::msg("ERR syntax error")),
            _ => self.parse_word(),
        }
    }

    fn parse_field_modifier(&mut self) -> Result<FullTextQueryAst, Error> {
        self.expect_char('@')?;
        let start = self.idx;
        while let Some(ch) = self.peek_char() {
            if ch == ':' {
                break;
            }
            if ch.is_ascii_whitespace() || ch == '(' || ch == ')' {
                return Err(Error::msg("ERR syntax error"));
            }
            self.advance_char();
        }
        if !self.consume_char(':') || self.idx == start {
            return Err(Error::msg("ERR syntax error"));
        }
        let fields = self.input[start..self.idx - 1]
            .split('|')
            .filter(|field| !field.is_empty())
            .map(|field| field.to_string())
            .collect::<Vec<_>>();
        if fields.is_empty() {
            return Err(Error::msg("ERR syntax error"));
        }

        match self.peek_char() {
            Some('{') => {
                let raw = self.read_balanced('{', '}')?;
                Ok(FullTextQueryAst::Tag {
                    field: fields[0].clone(),
                    values: split_tag_values(&raw),
                })
            }
            Some('[') => self.parse_field_bracket(fields[0].clone()),
            Some('(') => {
                self.advance_char();
                let expr = self.parse_or()?;
                self.skip_ws();
                if !self.consume_char(')') {
                    return Err(Error::msg("ERR syntax error"));
                }
                Ok(FullTextQueryAst::Field {
                    fields,
                    expr: Box::new(expr),
                })
            }
            _ => {
                let expr = self.parse_unary()?;
                Ok(FullTextQueryAst::Field {
                    fields,
                    expr: Box::new(expr),
                })
            }
        }
    }

    fn parse_field_bracket(&mut self, field: String) -> Result<FullTextQueryAst, Error> {
        let raw = self.read_balanced('[', ']')?;
        let parts = raw.split_whitespace().collect::<Vec<_>>();
        if parts.is_empty() {
            return Err(Error::msg("ERR syntax error"));
        }
        match parts[0].to_ascii_uppercase().as_str() {
            "VECTOR_RANGE" if parts.len() >= 3 => Ok(FullTextQueryAst::VectorRange {
                field,
                radius: parse_f64_token(parts[1], "ERR invalid vector range")?,
                blob_param: parts[2].trim_start_matches('$').to_string(),
            }),
            "WITHIN" | "CONTAINS" if parts.len() >= 2 => Ok(FullTextQueryAst::GeoShape {
                field,
                relation: parts[0].to_ascii_uppercase(),
                shape: parts[1..].join(" "),
            }),
            _ if parts.len() == 4 && parts[0].parse::<f64>().is_ok() => Ok(FullTextQueryAst::Geo {
                field,
                lon: parse_f64_token(parts[0], "ERR invalid geo filter")?,
                lat: parse_f64_token(parts[1], "ERR invalid geo filter")?,
                radius: parse_f64_token(parts[2], "ERR invalid geo filter")?,
                unit: parts[3].to_string(),
            }),
            _ if parts.len() == 2 => Ok(FullTextQueryAst::Numeric {
                field,
                min: parse_numeric_bound(parts[0])?,
                max: parse_numeric_bound(parts[1])?,
            }),
            _ => Err(Error::msg("ERR syntax error")),
        }
    }

    fn parse_vector_knn(&mut self, filter: FullTextQueryAst) -> Result<FullTextQueryAst, Error> {
        let raw = self.read_balanced('[', ']')?;
        let parts = raw.split_whitespace().collect::<Vec<_>>();
        if parts.len() < 4 || !parts[0].eq_ignore_ascii_case("KNN") {
            return Err(Error::msg("ERR syntax error"));
        }
        Ok(FullTextQueryAst::VectorKnn {
            filter: Box::new(filter),
            k: parts[1]
                .parse::<usize>()
                .map_err(|_| Error::msg("ERR invalid vector KNN"))?,
            field: parts[2].trim_start_matches('@').to_string(),
            blob_param: parts[3].trim_start_matches('$').to_string(),
        })
    }

    fn parse_fuzzy(&mut self) -> Result<FullTextQueryAst, Error> {
        self.expect_char('%')?;
        let start = self.idx;
        while let Some(ch) = self.peek_char() {
            if ch == '%' {
                let value = self.input[start..self.idx].to_string();
                self.advance_char();
                if value.is_empty() {
                    return Err(Error::msg("ERR syntax error"));
                }
                return Ok(FullTextQueryAst::Fuzzy(value));
            }
            self.advance_char();
        }
        Err(Error::msg("ERR syntax error"))
    }

    fn parse_word(&mut self) -> Result<FullTextQueryAst, Error> {
        let start = self.idx;
        let mut escaped = false;
        while let Some(ch) = self.peek_char() {
            if escaped {
                escaped = false;
                self.advance_char();
                continue;
            }
            if ch == '\\' {
                escaped = true;
                self.advance_char();
                continue;
            }
            if ch.is_ascii_whitespace() || ch == ')' || ch == '|' {
                break;
            }
            if self.input[self.idx..].starts_with("=>") {
                break;
            }
            self.advance_char();
        }
        if self.idx == start {
            return Err(Error::msg("ERR syntax error"));
        }
        let word = unescape_query_token(&self.input[start..self.idx]);
        if word == "*" {
            Ok(FullTextQueryAst::All)
        } else if word.ends_with('*')
            && !word[..word.len() - 1].contains('*')
            && !word.contains('?')
        {
            Ok(FullTextQueryAst::Prefix(
                word.trim_end_matches('*').to_string(),
            ))
        } else if word.contains('*') || word.contains('?') {
            Ok(FullTextQueryAst::Wildcard(word))
        } else {
            Ok(FullTextQueryAst::Text(word))
        }
    }

    fn read_quoted(&mut self) -> Result<String, Error> {
        self.expect_char('"')?;
        let mut out = String::new();
        let mut escaped = false;
        while let Some(ch) = self.peek_char() {
            self.advance_char();
            if escaped {
                out.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Ok(out);
            } else {
                out.push(ch);
            }
        }
        Err(Error::msg("ERR syntax error"))
    }

    fn read_balanced(&mut self, open: char, close: char) -> Result<String, Error> {
        self.expect_char(open)?;
        let start = self.idx;
        let mut escaped = false;
        while let Some(ch) = self.peek_char() {
            if escaped {
                escaped = false;
                self.advance_char();
                continue;
            }
            if ch == '\\' {
                escaped = true;
                self.advance_char();
                continue;
            }
            if ch == close {
                let out = self.input[start..self.idx].to_string();
                self.advance_char();
                return Ok(out);
            }
            self.advance_char();
        }
        Err(Error::msg("ERR syntax error"))
    }

    fn skip_ws(&mut self) {
        while self.peek_char().is_some_and(|ch| ch.is_ascii_whitespace()) {
            self.advance_char();
        }
    }

    fn eof(&self) -> bool {
        self.idx >= self.input.len()
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.idx..].chars().next()
    }

    fn advance_char(&mut self) {
        if let Some(ch) = self.peek_char() {
            self.idx += ch.len_utf8();
        }
    }

    fn consume_char(&mut self, expected: char) -> bool {
        if self.peek_char() == Some(expected) {
            self.advance_char();
            true
        } else {
            false
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), Error> {
        if self.consume_char(expected) {
            Ok(())
        } else {
            Err(Error::msg("ERR syntax error"))
        }
    }

    fn consume_str(&mut self, expected: &str) -> bool {
        if self.input[self.idx..].starts_with(expected) {
            self.idx += expected.len();
            true
        } else {
            false
        }
    }
}
