fn fulltext_materialize_text(value: &str, settings: &FullTextTextFieldSettings) -> String {
    let mut tokens = Vec::new();
    for token in fulltext_tokenize(value) {
        if settings.stopwords.contains(&token) {
            continue;
        }
        fulltext_push_unique(&mut tokens, token.clone());
        if !settings.nostem {
            fulltext_push_unique(&mut tokens, fulltext_english_stem(&token));
        }
        if settings.phonetic
            && let Some(code) = fulltext_soundex(&token)
        {
            fulltext_push_unique(&mut tokens, format!("phon{}", code.to_lowercase()));
        }
        if settings.with_suffix_trie {
            for suffix in fulltext_suffix_tokens(&token) {
                fulltext_push_unique(&mut tokens, suffix);
            }
        }
    }
    tokens.join(" ")
}

fn fulltext_query_term_variants(
    term: &str,
    settings: Option<&FullTextTextFieldSettings>,
    synonyms: &HashMap<String, HashSet<String>>,
) -> Vec<String> {
    let mut variants = Vec::new();
    let settings = settings
        .cloned()
        .unwrap_or_else(|| FullTextTextFieldSettings {
            nostem: false,
            phonetic: false,
            with_suffix_trie: false,
            stopwords: HashSet::new(),
        });
    let mut input_tokens = fulltext_tokenize(term);
    for token in input_tokens.clone() {
        if let Some(terms) = synonyms.get(&token) {
            for synonym in terms {
                input_tokens.extend(fulltext_tokenize(synonym));
            }
        }
    }
    for token in input_tokens {
        if settings.stopwords.contains(&token) {
            continue;
        }
        fulltext_push_unique(&mut variants, token.clone());
        if !settings.nostem {
            fulltext_push_unique(&mut variants, fulltext_english_stem(&token));
        }
        if settings.phonetic
            && let Some(code) = fulltext_soundex(&token)
        {
            fulltext_push_unique(&mut variants, format!("phon{}", code.to_lowercase()));
        }
    }
    if variants.is_empty() {
        variants.push(term.to_lowercase());
    }
    variants
}

fn fulltext_simple_query_term(query_text: &str) -> Option<&str> {
    let term = query_text.trim();
    if term.is_empty()
        || term.starts_with('"')
        || term.contains(char::is_whitespace)
        || term.contains(['*', '?', '|', '(', ')', ':', '[', ']', '{', '}'])
    {
        None
    } else {
        Some(term)
    }
}

fn fulltext_tokenize(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch.to_ascii_lowercase());
            continue;
        }
        if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
        if fulltext_is_cjk(ch) {
            tokens.push(ch.to_string());
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    for window in value
        .chars()
        .filter(|ch| fulltext_is_cjk(*ch))
        .collect::<Vec<_>>()
        .windows(2)
    {
        tokens.push(window.iter().collect());
    }
    tokens
}

fn fulltext_is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

fn fulltext_english_stem(token: &str) -> String {
    if token.len() <= 3 {
        return token.to_string();
    }
    let mut stem = if token.ends_with("ies") && token.len() > 4 {
        format!("{}y", &token[..token.len() - 3])
    } else if token.ends_with("ing") && token.len() > 5 {
        token[..token.len() - 3].to_string()
    } else if token.ends_with("ed") && token.len() > 4 {
        token[..token.len() - 2].to_string()
    } else if token.ends_with('s') && token.len() > 4 {
        token[..token.len() - 1].to_string()
    } else {
        token.to_string()
    };
    if stem.len() >= 3 {
        let chars = stem.chars().collect::<Vec<_>>();
        let len = chars.len();
        if len >= 2 && chars[len - 1] == chars[len - 2] && !matches!(chars[len - 1], 's' | 'z') {
            stem.pop();
        }
    }
    stem
}

fn fulltext_soundex(token: &str) -> Option<String> {
    let letters = token
        .chars()
        .filter(|ch| ch.is_ascii_alphabetic())
        .map(|ch| ch.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let first = *letters.first()?;
    let mut out = String::new();
    out.push(first);
    let mut previous = fulltext_soundex_digit(first);
    for ch in letters.into_iter().skip(1) {
        let digit = fulltext_soundex_digit(ch);
        if digit != '0' && digit != previous {
            out.push(digit);
            if out.len() == 4 {
                return Some(out);
            }
        }
        previous = digit;
    }
    while out.len() < 4 {
        out.push('0');
    }
    Some(out)
}

fn fulltext_soundex_digit(ch: char) -> char {
    match ch {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}

fn fulltext_suffix_tokens(token: &str) -> Vec<String> {
    let chars = token.chars().collect::<Vec<_>>();
    if chars.len() < 4 {
        return Vec::new();
    }
    (1..chars.len() - 1)
        .map(|idx| chars[idx..].iter().collect::<String>())
        .filter(|suffix| suffix.len() >= 2)
        .collect()
}

fn fulltext_push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn load_fulltext_synonyms_from_store(
    store: &crate::store::kv_store::KvStore,
    db_index: u16,
    index: &str,
) -> Result<HashMap<String, HashSet<String>>, Error> {
    let mut synonyms: HashMap<String, HashSet<String>> = HashMap::new();
    for (_, raw) in store.scan_prefix_raw(&fulltext_syn_prefix(db_index, index)) {
        let group = decode_record::<FullTextSynonymGroup>(&raw)?;
        for term in &group.terms {
            for synonym in &group.terms {
                if synonym != term {
                    synonyms
                        .entry(term.clone())
                        .or_default()
                        .insert(synonym.clone());
                }
            }
        }
    }
    Ok(synonyms)
}

fn fulltext_edit_distance(left: &str, right: &str) -> usize {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    let mut prev = (0..=right.len()).collect::<Vec<_>>();
    let mut curr = vec![0; right.len() + 1];
    for (i, left_ch) in left.iter().enumerate() {
        curr[0] = i + 1;
        for (j, right_ch) in right.iter().enumerate() {
            let cost = usize::from(left_ch != right_ch);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[right.len()]
}

fn format_fulltext_spellcheck_score(distance: usize) -> String {
    if distance == 0 {
        "1".to_string()
    } else {
        format!("0.{}", 10usize.saturating_sub(distance).max(1))
    }
}

fn format_fulltext_suggestion_score(score: f64) -> String {
    if score.fract() == 0.0 {
        format!("{score:.0}")
    } else {
        score.to_string()
    }
}

fn fulltext_display_terms(query: &str) -> Vec<String> {
    fulltext_tokenize(query)
        .into_iter()
        .filter(|term| term.len() > 1 || term.chars().any(fulltext_is_cjk))
        .collect()
}

fn fulltext_display_value(
    value: &str,
    options: &FullTextSearchOptions,
    display_terms: &[String],
) -> String {
    let summarized = if options.summarize {
        fulltext_summarize_value(value, display_terms)
    } else {
        value.to_string()
    };
    if options.highlight {
        fulltext_highlight_value(&summarized, display_terms)
    } else {
        summarized
    }
}

fn fulltext_summarize_value(value: &str, display_terms: &[String]) -> String {
    if value.chars().count() <= 80 {
        return value.to_string();
    }
    let lower = value.to_lowercase();
    let offset = display_terms
        .iter()
        .filter(|term| !term.is_empty())
        .find_map(|term| lower.find(term))
        .unwrap_or(0);
    let start = offset.saturating_sub(30);
    let end = (offset + 50).min(value.len());
    let mut snippet = value.get(start..end).unwrap_or(value).trim().to_string();
    if start > 0 {
        snippet.insert_str(0, "...");
    }
    if end < value.len() {
        snippet.push_str("...");
    }
    snippet
}

fn fulltext_highlight_value(value: &str, display_terms: &[String]) -> String {
    let mut out = value.to_string();
    for term in display_terms {
        if term.is_empty() {
            continue;
        }
        out = fulltext_highlight_one(&out, term);
    }
    out
}

fn fulltext_highlight_one(value: &str, term: &str) -> String {
    let lower = value.to_lowercase();
    let term = term.to_lowercase();
    let mut out = String::new();
    let mut cursor = 0usize;
    let mut search_from = 0usize;
    while let Some(relative) = lower[search_from..].find(&term) {
        let start = search_from + relative;
        let end = start + term.len();
        out.push_str(&value[cursor..start]);
        out.push_str("<b>");
        out.push_str(&value[start..end]);
        out.push_str("</b>");
        cursor = end;
        search_from = end;
    }
    out.push_str(&value[cursor..]);
    out
}

