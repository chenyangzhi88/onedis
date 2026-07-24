use regex::bytes::{Regex, RegexBuilder};

/// A bounded, linear-time matcher for Redis-style glob patterns.
///
/// Client bytes are emitted as escaped byte literals. Only Redis glob
/// operators (`*`, `?`, character classes, and backslash escapes) become
/// regular-expression syntax, so callers cannot inject regex operators.
pub struct Matcher {
    regex: Option<Regex>,
}

impl Matcher {
    pub fn new(pattern: &str) -> Self {
        let regex = glob_regex(pattern.as_bytes()).and_then(|source| {
            RegexBuilder::new(&source)
                .unicode(false)
                .dot_matches_new_line(true)
                .size_limit(8 * 1024 * 1024)
                .build()
                .ok()
        });
        Self { regex }
    }

    pub fn is_match(&self, value: &str) -> bool {
        self.regex
            .as_ref()
            .is_some_and(|regex| regex.is_match(value.as_bytes()))
    }
}

pub fn is_match(value: &str, pattern: &str) -> bool {
    Matcher::new(pattern).is_match(value)
}

fn glob_regex(pattern: &[u8]) -> Option<String> {
    let mut source = String::with_capacity(pattern.len().saturating_mul(4).saturating_add(4));
    source.push_str(r"\A");
    let mut idx = 0usize;
    while idx < pattern.len() {
        match pattern[idx] {
            b'*' => {
                source.push_str(".*");
                while pattern.get(idx + 1) == Some(&b'*') {
                    idx += 1;
                }
                idx += 1;
            }
            b'?' => {
                source.push('.');
                idx += 1;
            }
            b'\\' if idx + 1 < pattern.len() => {
                push_byte_literal(&mut source, pattern[idx + 1]);
                idx += 2;
            }
            b'[' => {
                let Some(close) = class_end(pattern, idx + 1) else {
                    push_byte_literal(&mut source, b'[');
                    idx += 1;
                    continue;
                };
                push_character_class(&mut source, &pattern[idx + 1..close])?;
                idx = close + 1;
            }
            byte => {
                push_byte_literal(&mut source, byte);
                idx += 1;
            }
        }
    }
    source.push_str(r"\z");
    Some(source)
}

fn class_end(pattern: &[u8], mut idx: usize) -> Option<usize> {
    while idx < pattern.len() {
        if pattern[idx] == b'\\' && idx + 1 < pattern.len() {
            idx += 2;
        } else if pattern[idx] == b']' {
            return Some(idx);
        } else {
            idx += 1;
        }
    }
    None
}

fn push_character_class(source: &mut String, class: &[u8]) -> Option<()> {
    let (negated, mut idx) = if matches!(class.first(), Some(b'^' | b'!')) {
        (true, 1)
    } else {
        (false, 0)
    };
    if idx == class.len() {
        if negated {
            source.push('.');
            return Some(());
        }
        return None;
    }

    source.push('[');
    if negated {
        source.push('^');
    }
    while idx < class.len() {
        let (first, next) = class_byte(class, idx);
        idx = next;
        if idx + 1 < class.len() && class[idx] == b'-' {
            let (last, next) = class_byte(class, idx + 1);
            if first <= last {
                push_byte_literal(source, first);
                source.push('-');
                push_byte_literal(source, last);
            } else {
                push_byte_literal(source, first);
                push_byte_literal(source, b'-');
                push_byte_literal(source, last);
            }
            idx = next;
        } else {
            push_byte_literal(source, first);
        }
    }
    source.push(']');
    Some(())
}

fn class_byte(class: &[u8], idx: usize) -> (u8, usize) {
    if class[idx] == b'\\' && idx + 1 < class.len() {
        (class[idx + 1], idx + 2)
    } else {
        (class[idx], idx + 1)
    }
}

fn push_byte_literal(output: &mut String, byte: u8) {
    use std::fmt::Write;
    let _ = write!(output, r"\x{byte:02x}");
}

#[cfg(test)]
mod tests {
    use super::{Matcher, is_match};

    #[test]
    fn glob_star_question_and_character_classes_match_expected_keys() {
        assert!(is_match("user:100:name", "user:*:name"));
        assert!(is_match("user:1:name", "user:?:name"));
        assert!(!is_match("user:12:name", "user:?:name"));
        assert!(is_match("key-b", "key-[abc]"));
        assert!(!is_match("key-z", "key-[abc]"));
        assert!(is_match("key-z", "key-[^abc]"));
        assert!(!is_match("key-a", "key-[^abc]"));
        assert!(is_match("key-z", "key-[!abc]"));
        assert!(is_match("key-c", "key-[a-d]"));
    }

    #[test]
    fn matching_is_anchored_and_regex_metacharacters_are_literals() {
        assert!(!is_match("prefix-value-suffix", "value"));
        assert!(!is_match("abc", "a.c"));
        assert!(is_match("a.c", "a.c"));
        assert!(is_match("a+(", r"a+\("));
    }

    #[test]
    fn malformed_empty_and_escaped_patterns_are_safe() {
        assert!(is_match("[", "["));
        assert!(!is_match("a", "["));
        assert!(is_match("*", r"\*"));
        assert!(is_match("\\", "\\"));
        assert!(!is_match("a", "[]"));
        assert!(is_match("a", "[^]"));
    }

    #[test]
    fn matcher_can_be_reused_without_a_global_pattern_cache() {
        let matcher = Matcher::new("user:*");
        assert!(matcher.is_match("user:1"));
        assert!(matcher.is_match("user:2"));
        assert!(!matcher.is_match("other:1"));
    }
}
