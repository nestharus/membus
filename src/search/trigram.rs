use std::collections::HashSet;

/// Maximum distinct case-sensitive Unicode character trigrams indexed per document.
/// Documents exceeding this cap set `overflow = true` so literal substring search
/// always verifies them without truncation false negatives.
pub const MAX_TRIGRAMS_PER_DOC: usize = 65_536;

/// Extracted case-sensitive Unicode character trigrams for one document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrigramSet {
    pub grams: Vec<String>,
    pub overflow: bool,
}

/// Extract distinct case-sensitive Unicode character trigrams across `fields`,
/// capping stored grams at [`MAX_TRIGRAMS_PER_DOC`].
pub fn extract_trigrams(fields: &[&str]) -> TrigramSet {
    let mut seen: HashSet<String> = HashSet::new();
    let mut overflow = false;

    for field in fields {
        let chars: Vec<char> = field.chars().collect();
        if chars.len() < 3 {
            continue;
        }
        for window in chars.windows(3) {
            let mut gram = String::with_capacity(12);
            gram.push(window[0]);
            gram.push(window[1]);
            gram.push(window[2]);
            if seen.contains(&gram) {
                continue;
            }
            if seen.len() >= MAX_TRIGRAMS_PER_DOC {
                overflow = true;
                break;
            }
            seen.insert(gram);
        }
        if overflow {
            break;
        }
    }

    let mut grams: Vec<String> = seen.into_iter().collect();
    grams.sort();
    TrigramSet { grams, overflow }
}

/// Extract distinct case-sensitive Unicode character trigrams for a literal substring query.
///
/// Returns an empty `Vec` when the query has fewer than 3 Unicode characters (caller
/// uses exact substring scan for 1–2 char queries).
pub fn query_trigrams(needle: &str) -> Vec<String> {
    let chars: Vec<char> = needle.chars().collect();
    if chars.len() < 3 {
        return Vec::new();
    }
    let mut seen: HashSet<String> = HashSet::new();
    for window in chars.windows(3) {
        let mut gram = String::with_capacity(12);
        gram.push(window[0]);
        gram.push(window[1]);
        gram.push(window[2]);
        seen.insert(gram);
    }
    let mut grams: Vec<String> = seen.into_iter().collect();
    grams.sort();
    grams
}

/// Exact case-sensitive substring verification across visible searchable fields.
pub fn contains_exact_substring(needle: &str, fields: &[&str]) -> bool {
    if needle.is_empty() {
        return false;
    }
    fields.iter().any(|field| field.contains(needle))
}
