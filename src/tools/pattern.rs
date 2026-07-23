use regex::Regex;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

fn regex_cache() -> &'static Mutex<HashMap<String, Regex>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_regex_cache() -> std::sync::MutexGuard<'static, HashMap<String, Regex>> {
    regex_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn is_match(key: &str, pattern: &str) -> bool {
    fn convert_pattern(pattern: &str) -> String {
        let mut regex_pattern = String::new();
        let mut chars = pattern.chars().peekable();
        while let Some(p) = chars.next() {
            match p {
                '*' => regex_pattern.push_str(".*"),
                '?' => regex_pattern.push('.'),
                '[' => {
                    regex_pattern.push('[');
                    if let Some(next) = chars.peek() {
                        if *next == '^' {
                            regex_pattern.push('^');
                            chars.next(); // 跳过 '^'
                        }
                    }
                    while let Some(ch) = chars.next() {
                        if ch == ']' {
                            break;
                        }
                        regex_pattern.push(ch);
                    }
                    regex_pattern.push(']');
                }
                _ => regex_pattern.push(p),
            }
        }
        regex_pattern
    }

    // 尝试从缓存中获取已编译的正则表达式
    let regex = {
        let cache = lock_regex_cache();
        if let Some(regex) = cache.get(pattern) {
            regex.clone()
        } else {
            drop(cache); // 释放读锁后再进行写操作
            let regex_pattern = convert_pattern(pattern);
            let regex = Regex::new(&regex_pattern).unwrap();
            let mut cache = lock_regex_cache();
            cache.insert(pattern.to_string(), regex.clone());
            regex
        }
    };

    regex.is_match(key)
}

#[cfg(test)]
mod tests {
    use super::{is_match, lock_regex_cache};

    #[test]
    fn glob_star_question_and_character_classes_match_expected_keys() {
        assert!(is_match("user:100:name", "user:*:name"));
        assert!(is_match("user:1:name", "user:?:name"));
        assert!(!is_match("user:12:name", "user:?:name"));
        assert!(is_match("key-b", "key-[abc]"));
        assert!(!is_match("key-z", "key-[abc]"));
        assert!(is_match("key-z", "key-[^abc]"));
        assert!(!is_match("key-a", "key-[^abc]"));
    }

    #[test]
    fn cache_is_reused_for_repeated_patterns() {
        let prefix = format!(
            "cache:{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let pattern = format!("{prefix}:*");
        assert!(!lock_regex_cache().contains_key(&pattern));
        assert!(is_match(&format!("{prefix}:1"), &pattern));
        assert!(lock_regex_cache().contains_key(&pattern));
        assert!(is_match(&format!("{prefix}:2"), &pattern));
        assert!(lock_regex_cache().contains_key(&pattern));
    }

    #[test]
    fn current_regex_style_matching_is_unanchored_and_keeps_regex_meta_chars() {
        assert!(is_match("prefix-value-suffix", "value"));
        assert!(is_match("abc", "a.c"));
        assert!(is_match("aaa", "a+"));
        assert!(!is_match("bbb", "a+"));
    }
}
