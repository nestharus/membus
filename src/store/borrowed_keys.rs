//! Borrow both components of an ordered committed-index query.
use super::Key;
use std::{borrow::Borrow, cmp::Ordering, collections::BTreeMap};

// Keep the stored tuple and its namespace-range ordering. Borrowing a tuple of
// str references directly would require references inside the owned key; this
// comparison view exposes its components without allocating or packing them.
trait Parts {
    fn parts(&self) -> (&str, &str);
}
impl Parts for Key {
    fn parts(&self) -> (&str, &str) {
        (&self.0, &self.1)
    }
}
impl Parts for (&str, &str) {
    fn parts(&self) -> (&str, &str) {
        (self.0, self.1)
    }
}
impl<'a> Borrow<dyn Parts + 'a> for Key {
    fn borrow(&self) -> &(dyn Parts + 'a) {
        self
    }
}
// Borrow requires the query order to agree with the owned (String, String)
// order: first namespace, then key, with ordinary UTF-8 string comparison.
impl PartialEq for dyn Parts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}
impl Eq for dyn Parts + '_ {}
impl PartialOrd for dyn Parts + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for dyn Parts + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        self.parts().cmp(&other.parts())
    }
}
pub(crate) fn get<'a, V>(
    entries: &'a BTreeMap<Key, V>,
    namespace: &str,
    key: &str,
) -> Option<&'a V> {
    entries.get(&(namespace, key) as &dyn Parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_index_matches_owned_tuple_order_and_missing_entries() {
        let mut parts: Vec<_> = [
            "",
            "\0",
            "\0a",
            "a",
            "a\0",
            "a\0b",
            "a/b",
            "aa",
            "ab",
            "b",
            "z",
            "界",
            "界\0",
            "界a",
            "键",
            "\u{10ffff}",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        parts.push(format!("{}x", "界".repeat(341)));
        let mut queries = parts.clone();
        queries.extend(["missing", "a\0missing", "界missing"].map(str::to_owned));
        for reverse in [false, true] {
            let mut entries = BTreeMap::new();
            let mut order: Vec<_> = (0..parts.len() * parts.len()).collect();
            if reverse {
                order.reverse();
            }
            for i in order {
                entries.insert(
                    (
                        parts[i / parts.len()].clone(),
                        parts[i % parts.len()].clone(),
                    ),
                    i,
                );
            }
            // Query both full and partially removed trees. This crosses leaf
            // splits and checks ambiguous concatenations without a separator.
            for removed in [false, true] {
                if removed {
                    entries.retain(|_, value| *value % 3 != 0);
                }
                for namespace in &queries {
                    for key in &queries {
                        assert_eq!(get(&entries, namespace, key),
                            entries.get(&(namespace.clone(), key.clone())),
                            "namespace={namespace:?} key={key:?} reverse={reverse} removed={removed}");
                    }
                }
            }
        }
    }
}
