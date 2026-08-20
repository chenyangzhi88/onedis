use super::*;
pub(super) fn fulltext_materialize_text(
    value: &str,
    settings: &FullTextTextFieldSettings,
) -> (String, String) {
    fulltext_materialize_text_with_synonyms(value, settings, &HashMap::new())
}

pub(super) fn fulltext_materialize_text_with_synonyms(
    value: &str,
    settings: &FullTextTextFieldSettings,
    synonyms: &HashMap<String, HashSet<String>>,
) -> (String, String) {
    let source_tokens = fulltext_phrase_tokens(value, &settings.language);
    let indexed_tokens = source_tokens
        .iter()
        .filter(|token| !settings.stopwords.contains(*token))
        .cloned()
        .collect::<Vec<_>>();
    let mut variants = indexed_tokens.clone();
    for token in &indexed_tokens {
        let mut equivalent_terms = vec![token.clone()];
        if let Some(synonym_terms) = synonyms.get(token) {
            equivalent_terms.extend(synonym_terms.iter().cloned());
        }
        for equivalent in equivalent_terms {
            for variant in fulltext_tokenize_with_language(&equivalent, &settings.language) {
                if !variants.contains(&variant) {
                    variants.push(variant);
                }
            }
            if !settings.nostem {
                let stem = fulltext_stem(&equivalent, &settings.language);
                if !variants.contains(&stem) {
                    variants.push(stem);
                }
            }
            if settings.phonetic
                && let Some(code) = fulltext_soundex(&equivalent)
            {
                let code = format!("phon{}", code.to_lowercase());
                if !variants.contains(&code) {
                    variants.push(code);
                }
            }
            if settings.with_suffix_trie {
                for suffix in fulltext_suffix_tokens(&equivalent) {
                    if !variants.contains(&suffix) {
                        variants.push(suffix);
                    }
                }
            }
        }
    }
    (source_tokens.join(" "), variants.join(" "))
}

pub(super) fn fulltext_query_term_variants(
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
            language: "english".to_string(),
            weight: 1.0,
        });
    let mut input_tokens = fulltext_tokenize_with_language(term, &settings.language);
    for token in input_tokens.clone() {
        if let Some(terms) = synonyms.get(&token) {
            for synonym in terms {
                input_tokens.extend(fulltext_tokenize_with_language(synonym, &settings.language));
            }
        }
    }
    for token in input_tokens {
        if settings.stopwords.contains(&token) {
            continue;
        }
        fulltext_push_unique(&mut variants, token.clone());
        if !settings.nostem {
            fulltext_push_unique(&mut variants, fulltext_stem(&token, &settings.language));
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

pub(super) fn fulltext_simple_query_term(query_text: &str) -> Option<&str> {
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

pub(super) fn fulltext_tokenize(value: &str) -> Vec<String> {
    fulltext_tokenize_with_language(value, "english")
}

pub(super) fn fulltext_tokenize_with_language(value: &str, _language: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in value.unicode_words() {
        if word.chars().any(fulltext_is_cjk) {
            for segment in fulltext_jieba().cut(word, false) {
                let segment = segment.word.trim();
                if segment.is_empty() {
                    continue;
                }
                let mut variants = Vec::new();
                fulltext_push_unique(&mut variants, segment.to_lowercase());
                let cjk = segment
                    .chars()
                    .filter(|ch| fulltext_is_cjk(*ch))
                    .collect::<Vec<_>>();
                for ch in &cjk {
                    fulltext_push_unique(&mut variants, ch.to_string());
                }
                for pair in cjk.windows(2) {
                    fulltext_push_unique(&mut variants, pair.iter().collect());
                }
                tokens.extend(variants);
            }
        } else {
            let normalized = word
                .chars()
                .filter(|ch| ch.is_alphanumeric() || *ch == '_')
                .flat_map(char::to_lowercase)
                .collect::<String>();
            if !normalized.is_empty() {
                tokens.push(normalized);
            }
        }
    }
    tokens
}

pub(super) fn fulltext_phrase_tokens(value: &str, _language: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in value.unicode_words() {
        if word.chars().any(fulltext_is_cjk) {
            for segment in fulltext_jieba().cut(word, false) {
                let normalized = segment.word.trim().to_lowercase();
                if !normalized.is_empty() {
                    tokens.push(normalized);
                }
            }
        } else {
            let normalized = word
                .chars()
                .filter(|ch| ch.is_alphanumeric() || *ch == '_')
                .flat_map(char::to_lowercase)
                .collect::<String>();
            if !normalized.is_empty() {
                tokens.push(normalized);
            }
        }
    }
    tokens
}

pub(super) fn fulltext_split_indexed_tags(
    value: &str,
    separator: char,
    case_sensitive: bool,
) -> Vec<String> {
    value
        .split(separator)
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(|tag| {
            if case_sensitive {
                tag.to_string()
            } else {
                tag.to_lowercase()
            }
        })
        .collect()
}

pub(super) fn fulltext_jieba() -> &'static Jieba {
    static JIEBA: OnceLock<Jieba> = OnceLock::new();
    JIEBA.get_or_init(Jieba::new)
}

pub(super) fn fulltext_is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

pub(super) fn normalize_fulltext_language(language: &str) -> Result<String, Error> {
    let normalized = language.trim().to_ascii_lowercase();
    let canonical = match normalized.as_str() {
        "arabic" => "arabic",
        "chinese" | "zh" | "zh-cn" | "zh-tw" => "chinese",
        "danish" => "danish",
        "dutch" => "dutch",
        "english" | "en" => "english",
        "finnish" => "finnish",
        "french" => "french",
        "german" => "german",
        "greek" => "greek",
        "hungarian" => "hungarian",
        "italian" => "italian",
        "norwegian" => "norwegian",
        "portuguese" => "portuguese",
        "romanian" => "romanian",
        "russian" => "russian",
        "spanish" => "spanish",
        "swedish" => "swedish",
        "tamil" => "tamil",
        "turkish" => "turkish",
        _ => return Err(Error::msg("ERR unsupported fulltext language")),
    };
    Ok(canonical.to_string())
}

pub(super) fn fulltext_stem(token: &str, language: &str) -> String {
    let algorithm = match language {
        "arabic" => Some(StemmerAlgorithm::Arabic),
        "danish" => Some(StemmerAlgorithm::Danish),
        "dutch" => Some(StemmerAlgorithm::Dutch),
        "english" => Some(StemmerAlgorithm::English),
        "finnish" => Some(StemmerAlgorithm::Finnish),
        "french" => Some(StemmerAlgorithm::French),
        "german" => Some(StemmerAlgorithm::German),
        "greek" => Some(StemmerAlgorithm::Greek),
        "hungarian" => Some(StemmerAlgorithm::Hungarian),
        "italian" => Some(StemmerAlgorithm::Italian),
        "norwegian" => Some(StemmerAlgorithm::Norwegian),
        "portuguese" => Some(StemmerAlgorithm::Portuguese),
        "romanian" => Some(StemmerAlgorithm::Romanian),
        "russian" => Some(StemmerAlgorithm::Russian),
        "spanish" => Some(StemmerAlgorithm::Spanish),
        "swedish" => Some(StemmerAlgorithm::Swedish),
        "tamil" => Some(StemmerAlgorithm::Tamil),
        "turkish" => Some(StemmerAlgorithm::Turkish),
        "chinese" => None,
        _ => Some(StemmerAlgorithm::English),
    };
    algorithm
        .map(|algorithm| Stemmer::create(algorithm).stem(token).into_owned())
        .unwrap_or_else(|| token.to_string())
}

pub(super) fn fulltext_soundex(token: &str) -> Option<String> {
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

pub(super) fn fulltext_soundex_digit(ch: char) -> char {
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

pub(super) fn fulltext_suffix_tokens(token: &str) -> Vec<String> {
    let chars = token.chars().collect::<Vec<_>>();
    if chars.len() < 4 {
        return Vec::new();
    }
    (1..chars.len() - 1)
        .map(|idx| chars[idx..].iter().collect::<String>())
        .filter(|suffix| suffix.len() >= 2)
        .collect()
}

pub(super) fn fulltext_push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

pub(super) fn load_fulltext_synonyms_from_store(
    store: &crate::store::kv_store::KvStore,
    db_index: u16,
    index: &str,
) -> Result<HashMap<String, HashSet<String>>, Error> {
    let mut synonyms: HashMap<String, HashSet<String>> = HashMap::new();
    for (_, raw) in store.scan_prefix_raw(&fulltext_syn_prefix(db_index, index))? {
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

pub(super) fn fulltext_edit_distance(left: &str, right: &str) -> usize {
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

pub(super) fn format_fulltext_spellcheck_score(distance: usize) -> String {
    if distance == 0 {
        "1".to_string()
    } else {
        format!("0.{}", 10usize.saturating_sub(distance).max(1))
    }
}

pub(super) fn format_fulltext_suggestion_score(score: f64) -> String {
    if score.fract() == 0.0 {
        format!("{score:.0}")
    } else {
        score.to_string()
    }
}

pub(super) fn fulltext_display_terms(query: &str) -> Vec<String> {
    fulltext_tokenize(query)
        .into_iter()
        .filter(|term| term.len() > 1 || term.chars().any(fulltext_is_cjk))
        .collect()
}

pub(super) fn fulltext_display_value(
    field: &str,
    value: &str,
    options: &FullTextSearchOptions,
    display_terms: &[String],
) -> String {
    let summarized = if let Some(summarize) = options
        .summarize
        .as_ref()
        .filter(|options| fulltext_display_applies_to_field(options.fields.as_ref(), field))
    {
        fulltext_summarize_value_with_options(value, display_terms, summarize)
    } else {
        value.to_string()
    };
    if let Some(highlight) = options
        .highlight
        .as_ref()
        .filter(|options| fulltext_display_applies_to_field(options.fields.as_ref(), field))
    {
        fulltext_highlight_value_with_tags(
            &summarized,
            display_terms,
            &highlight.open_tag,
            &highlight.close_tag,
        )
    } else {
        summarized
    }
}

fn fulltext_display_applies_to_field(fields: Option<&HashSet<String>>, field: &str) -> bool {
    fields.is_none_or(|fields| fields.contains(field))
}

fn fulltext_summarize_value_with_options(
    value: &str,
    display_terms: &[String],
    options: &FullTextSummarizeOptions,
) -> String {
    let char_count = value.chars().count();
    if char_count <= options.len {
        return value.to_string();
    }
    let lower = value.to_lowercase();
    let mut offsets = display_terms
        .iter()
        .filter(|term| !term.is_empty())
        .filter_map(|term| lower.find(&term.to_lowercase()))
        .map(|byte_offset| value[..byte_offset].chars().count())
        .collect::<Vec<_>>();
    if offsets.is_empty() {
        offsets.push(0);
    }
    offsets.sort_unstable();
    offsets.dedup();

    let chars = value.chars().collect::<Vec<_>>();
    let mut ranges = Vec::new();
    for offset in offsets {
        let start = offset.saturating_sub(options.len / 2);
        let end = start.saturating_add(options.len).min(chars.len());
        if ranges
            .last()
            .is_some_and(|(_, previous_end)| start <= *previous_end)
        {
            continue;
        }
        ranges.push((start, end));
        if ranges.len() == options.frags {
            break;
        }
    }
    ranges
        .into_iter()
        .map(|(start, end)| {
            let mut snippet = chars[start..end]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            if start > 0 {
                snippet.insert_str(0, &options.separator);
            }
            if end < chars.len() {
                snippet.push_str(&options.separator);
            }
            snippet
        })
        .collect::<Vec<_>>()
        .join(&options.separator)
}

#[cfg(test)]
pub(super) fn fulltext_highlight_value(value: &str, display_terms: &[String]) -> String {
    fulltext_highlight_value_with_tags(value, display_terms, "<b>", "</b>")
}

fn fulltext_highlight_value_with_tags(
    value: &str,
    display_terms: &[String],
    open_tag: &str,
    close_tag: &str,
) -> String {
    let mut out = value.to_string();
    for term in display_terms {
        if term.is_empty() {
            continue;
        }
        out = fulltext_highlight_one_with_tags(&out, term, open_tag, close_tag);
    }
    out
}

fn fulltext_highlight_one_with_tags(
    value: &str,
    term: &str,
    open_tag: &str,
    close_tag: &str,
) -> String {
    let term = term.to_lowercase();
    let term_chars = term.chars().count();
    if term_chars == 0 {
        return value.to_string();
    }
    let mut boundaries = value.char_indices().map(|(idx, _)| idx).collect::<Vec<_>>();
    boundaries.push(value.len());
    let mut out = String::new();
    let mut char_idx = 0usize;
    let mut byte_cursor = 0usize;
    while char_idx + term_chars < boundaries.len() {
        let start = boundaries[char_idx];
        let end = boundaries[char_idx + term_chars];
        if value[start..end].to_lowercase() == term {
            out.push_str(&value[byte_cursor..start]);
            out.push_str(open_tag);
            out.push_str(&value[start..end]);
            out.push_str(close_tag);
            byte_cursor = end;
            char_idx += term_chars;
        } else {
            char_idx += 1;
        }
    }
    out.push_str(&value[byte_cursor..]);
    out
}
