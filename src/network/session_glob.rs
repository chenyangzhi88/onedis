#[derive(Debug, PartialEq, Eq)]
enum GlobToken {
    Star,
    AnyByte,
    Never,
    Literal(u8),
    Class {
        negated: bool,
        ranges: Vec<(u8, u8)>,
    },
}

impl GlobToken {
    fn matches(&self, byte: u8) -> bool {
        match self {
            Self::AnyByte => true,
            Self::Never => false,
            Self::Literal(expected) => *expected == byte,
            Self::Class { negated, ranges } => {
                let contained = ranges
                    .iter()
                    .any(|(start, end)| *start <= byte && byte <= *end);
                contained != *negated
            }
            Self::Star => false,
        }
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let tokens = parse_glob(pattern.as_bytes());
    glob_match_tokens(&tokens, text.as_bytes())
}

fn glob_match_tokens(tokens: &[GlobToken], text: &[u8]) -> bool {
    let mut pattern_idx = 0usize;
    let mut text_idx = 0usize;
    let mut star_idx = None;
    let mut star_text_idx = 0usize;

    while text_idx < text.len() {
        if matches!(tokens.get(pattern_idx), Some(GlobToken::Star)) {
            star_idx = Some(pattern_idx);
            pattern_idx += 1;
            star_text_idx = text_idx;
        } else if tokens
            .get(pattern_idx)
            .is_some_and(|token| token.matches(text[text_idx]))
        {
            pattern_idx += 1;
            text_idx += 1;
        } else if let Some(star) = star_idx {
            star_text_idx += 1;
            text_idx = star_text_idx;
            pattern_idx = star + 1;
        } else {
            return false;
        }
    }

    while matches!(tokens.get(pattern_idx), Some(GlobToken::Star)) {
        pattern_idx += 1;
    }
    pattern_idx == tokens.len()
}

fn parse_glob(pattern: &[u8]) -> Vec<GlobToken> {
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut index = 0usize;
    while index < pattern.len() {
        match pattern[index] {
            b'*' => {
                if !matches!(tokens.last(), Some(GlobToken::Star)) {
                    tokens.push(GlobToken::Star);
                }
                index += 1;
            }
            b'?' => {
                tokens.push(GlobToken::AnyByte);
                index += 1;
            }
            b'\\' if index + 1 < pattern.len() => {
                tokens.push(GlobToken::Literal(pattern[index + 1]));
                index += 2;
            }
            b'[' => {
                if let Some((class, next_index)) = parse_class(pattern, index) {
                    tokens.push(class);
                    index = next_index;
                } else {
                    tokens.push(GlobToken::Never);
                    break;
                }
            }
            byte => {
                tokens.push(GlobToken::Literal(byte));
                index += 1;
            }
        }
    }
    tokens
}

fn parse_class(pattern: &[u8], start: usize) -> Option<(GlobToken, usize)> {
    let mut index = start + 1;
    let negated = pattern.get(index) == Some(&b'^');
    if negated {
        index += 1;
    }

    let mut ranges = Vec::new();
    while index < pattern.len() {
        if pattern[index] == b']' {
            return Some((GlobToken::Class { negated, ranges }, index + 1));
        }

        let (range_start, after_start) = parse_class_byte(pattern, index)?;
        if pattern.get(after_start) == Some(&b'-')
            && after_start + 1 < pattern.len()
            && pattern[after_start + 1] != b']'
        {
            let (range_end, after_end) = parse_class_byte(pattern, after_start + 1)?;
            ranges.push((
                range_start.min(range_end),
                range_start.max(range_end),
            ));
            index = after_end;
        } else {
            ranges.push((range_start, range_start));
            index = after_start;
        }
    }
    None
}

fn parse_class_byte(pattern: &[u8], index: usize) -> Option<(u8, usize)> {
    let byte = *pattern.get(index)?;
    if byte == b'\\' {
        pattern
            .get(index + 1)
            .copied()
            .map(|escaped| (escaped, index + 2))
    } else {
        Some((byte, index + 1))
    }
}
