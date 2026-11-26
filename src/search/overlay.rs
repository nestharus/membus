use std::collections::HashMap;

use parking_lot::RwLock;

use crate::search::trigram::contains_exact_substring;
use crate::types::{
    sanitize_score, SearchDocument, SearchHit, SearchLogOp, SearchMode, SearchQuery,
    SearchVisibility,
};

/// Composite unique document key `"{namespace}\0{doc_id}"`.
pub fn make_doc_key(namespace: &str, doc_id: &str) -> String {
    let mut out = String::with_capacity(namespace.len() + 1 + doc_id.len());
    out.push_str(namespace);
    out.push('\0');
    out.push_str(doc_id);
    out
}

/// Strictly ordered search operation version `(commit_seq, intra_tx_op_idx)`.
pub type OpVersion = (u64, u32);

/// Sanitized searchable projection respecting [`SearchVisibility`] isolation.
///
/// - `Sealed` returns `None` (never indexed; overwrites become tombstones).
/// - `MetadataOnly` blanks `body` so body text never enters RAM overlay or Tantivy.
#[derive(Clone, Debug)]
pub struct SanitizedDoc {
    pub doc_key: String,
    pub namespace: String,
    pub doc_id: String,
    pub kind: String,
    pub title: String,
    pub tags: Vec<String>,
    pub tags_joined: String,
    pub body: String,
}

impl SanitizedDoc {
    pub fn from_document(doc: &SearchDocument) -> Option<Self> {
        match doc.visibility {
            SearchVisibility::Sealed => None,
            SearchVisibility::MetadataOnly => Some(Self {
                doc_key: make_doc_key(&doc.namespace, &doc.doc_id),
                namespace: doc.namespace.clone(),
                doc_id: doc.doc_id.clone(),
                kind: doc.kind.clone(),
                title: doc.title.clone(),
                tags: doc.tags.clone(),
                tags_joined: doc.tags.join(" "),
                body: String::new(),
            }),
            SearchVisibility::Public => Some(Self {
                doc_key: make_doc_key(&doc.namespace, &doc.doc_id),
                namespace: doc.namespace.clone(),
                doc_id: doc.doc_id.clone(),
                kind: doc.kind.clone(),
                title: doc.title.clone(),
                tags: doc.tags.clone(),
                tags_joined: doc.tags.join(" "),
                body: doc.body.clone(),
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OverlayEntryState {
    Live(SanitizedDoc),
    Tombstone,
}

#[derive(Clone, Debug)]
pub struct OverlayEntry {
    pub seq: u64,
    pub op_idx: u32,
    pub state: OverlayEntryState,
}

impl OverlayEntry {
    pub fn version(&self) -> OpVersion {
        (self.seq, self.op_idx)
    }
}

#[derive(Clone, Debug)]
pub struct PrefixRevocation {
    pub namespace: String,
    pub prefix: String,
    pub seq: u64,
    pub op_idx: u32,
}

impl PrefixRevocation {
    pub fn version(&self) -> OpVersion {
        (self.seq, self.op_idx)
    }
}

/// Point-in-time snapshot of the Tier-0 RAM live delta overlay used during query merge.
#[derive(Clone, Debug)]
pub struct OverlaySnapshot {
    pub entries: HashMap<String, OverlayEntry>,
    pub revocations: Vec<PrefixRevocation>,
}

impl OverlaySnapshot {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.revocations.is_empty()
    }

    /// Returns `true` if a Tantivy hit at `(doc_key, namespace, doc_id, hit_seq, hit_op_idx)` is
    /// superseded by a newer Tier-0 live update, tombstone, or prefix revocation.
    pub fn shadows_tantivy_hit(
        &self,
        doc_key: &str,
        namespace: &str,
        doc_id: &str,
        hit_seq: u64,
        hit_op_idx: u32,
    ) -> bool {
        let hit_ver = (hit_seq, hit_op_idx);
        if let Some(entry) = self.entries.get(doc_key) {
            if entry.version() >= hit_ver {
                return true;
            }
        }
        self.revocations.iter().any(|rev| {
            rev.namespace == namespace && doc_id.starts_with(&rev.prefix) && rev.version() > hit_ver
        })
    }

    /// Evaluate matching live Tier-0 RAM documents for 0 ms search-your-writes recall.
    pub fn evaluate_live_hits(&self, query: &SearchQuery) -> Vec<(String, SearchHit)> {
        if query.query.is_empty() {
            return Vec::new();
        }
        let bm25_terms: Vec<String> = if query.mode == SearchMode::Bm25 {
            tokenize_lower(&query.query)
        } else {
            Vec::new()
        };

        let mut out = Vec::new();
        for (doc_key, entry) in &self.entries {
            let OverlayEntryState::Live(doc) = &entry.state else {
                continue;
            };
            if let Some(ns) = &query.namespace {
                if &doc.namespace != ns {
                    continue;
                }
            }
            if let Some(kind) = &query.kind {
                if &doc.kind != kind {
                    continue;
                }
            }
            if self.revocations.iter().any(|rev| {
                rev.namespace == doc.namespace
                    && doc.doc_id.starts_with(&rev.prefix)
                    && rev.version() > entry.version()
            }) {
                continue;
            }

            let score = sanitize_score(match query.mode {
                SearchMode::Bm25 => score_bm25_overlay(doc, &bm25_terms),
                SearchMode::LiteralSubstring => {
                    if contains_exact_substring(
                        &query.query,
                        &[&doc.title, &doc.tags_joined, &doc.body],
                    ) {
                        score_literal_overlay(doc, &query.query)
                    } else {
                        0.0
                    }
                }
            });

            if score > 0.0 {
                out.push((
                    doc_key.clone(),
                    SearchHit {
                        namespace: doc.namespace.clone(),
                        doc_id: doc.doc_id.clone(),
                        kind: doc.kind.clone(),
                        title: doc.title.clone(),
                        tags: doc.tags.clone(),
                        score,
                    },
                ));
            }
        }
        out
    }
}

struct OverlayInner {
    epoch: u64,
    max_entries: usize,
    entries: HashMap<String, OverlayEntry>,
    revocations: Vec<PrefixRevocation>,
}

/// Bounded in-RAM Tier-0 live delta overlay providing 0 ms search-your-writes
/// before background Tantivy segment commits complete.
pub struct Tier0Overlay {
    inner: RwLock<OverlayInner>,
}

impl Tier0Overlay {
    pub fn new(epoch: u64, max_entries: usize) -> Self {
        Self {
            inner: RwLock::new(OverlayInner {
                epoch,
                max_entries: max_entries.max(64),
                entries: HashMap::new(),
                revocations: Vec::new(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_over_budget(&self) -> bool {
        let guard = self.inner.read();
        guard.entries.len() >= guard.max_entries
    }

    /// Apply a search op tagged with strict `(seq, op_idx)` ordering; out-of-order lower
    /// versions can never overwrite or resurrect higher versions or post-revocation state.
    pub fn apply(&self, seq: u64, op_idx: u32, epoch: u64, op: &SearchLogOp) {
        let mut guard = self.inner.write();
        if epoch != guard.epoch {
            return;
        }
        let ver: OpVersion = (seq, op_idx);
        match op {
            SearchLogOp::Index(doc) => {
                let doc_key = make_doc_key(&doc.namespace, &doc.doc_id);
                if guard.revocations.iter().any(|rev| {
                    rev.namespace == doc.namespace
                        && doc.doc_id.starts_with(&rev.prefix)
                        && rev.version() > ver
                }) {
                    return;
                }
                if guard
                    .entries
                    .get(&doc_key)
                    .is_some_and(|existing| existing.version() > ver)
                {
                    return;
                }
                let state = match SanitizedDoc::from_document(doc) {
                    Some(sanitized) => OverlayEntryState::Live(sanitized),
                    None => OverlayEntryState::Tombstone,
                };
                guard.entries.insert(
                    doc_key,
                    OverlayEntry {
                        seq,
                        op_idx,
                        state,
                    },
                );
            }
            SearchLogOp::Delete { namespace, doc_id } => {
                let doc_key = make_doc_key(namespace, doc_id);
                if guard
                    .entries
                    .get(&doc_key)
                    .is_some_and(|existing| existing.version() > ver)
                {
                    return;
                }
                guard.entries.insert(
                    doc_key,
                    OverlayEntry {
                        seq,
                        op_idx,
                        state: OverlayEntryState::Tombstone,
                    },
                );
            }
            SearchLogOp::RevokePrefix { namespace, prefix } => {
                guard.entries.retain(|_k, entry| match &entry.state {
                    OverlayEntryState::Live(doc) => {
                        !(doc.namespace == *namespace
                            && doc.doc_id.starts_with(prefix.as_str())
                            && entry.version() < ver)
                    }
                    OverlayEntryState::Tombstone => true,
                });
                guard.revocations.push(PrefixRevocation {
                    namespace: namespace.clone(),
                    prefix: prefix.clone(),
                    seq,
                    op_idx,
                });
            }
        }
    }

    pub fn snapshot(&self) -> OverlaySnapshot {
        let guard = self.inner.read();
        OverlaySnapshot {
            entries: guard.entries.clone(),
            revocations: guard.revocations.clone(),
        }
    }

    /// Prune all overlay entries and revocations now durably visible in Tantivy's reader.
    pub fn prune_committed(&self, committed_seq: u64) {
        let mut guard = self.inner.write();
        guard.entries.retain(|_k, entry| entry.seq > committed_seq);
        guard.revocations.retain(|rev| rev.seq > committed_seq);
    }
}

fn tokenize_lower(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|tok| !tok.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn score_bm25_overlay(doc: &SanitizedDoc, terms: &[String]) -> f32 {
    if terms.is_empty() {
        return 0.0;
    }
    let title_tokens = tokenize_lower(&doc.title);
    let tag_tokens = tokenize_lower(&doc.tags_joined);
    let body_tokens = tokenize_lower(&doc.body);

    let mut score = 0.0f32;
    for term in terms {
        let t_hits = title_tokens.iter().filter(|t| *t == term).count() as f32;
        let g_hits = tag_tokens.iter().filter(|t| *t == term).count() as f32;
        let b_hits = body_tokens.iter().filter(|t| *t == term).count() as f32;
        if t_hits > 0.0 {
            score += 3.0 * (1.0 + t_hits.ln());
        }
        if g_hits > 0.0 {
            score += 2.0 * (1.0 + g_hits.ln());
        }
        if b_hits > 0.0 {
            score += 1.0 * (1.0 + b_hits.ln());
        }
    }
    score
}

fn score_literal_overlay(doc: &SanitizedDoc, needle: &str) -> f32 {
    let mut score = 0.0f32;
    if doc.title.contains(needle) {
        score += 3.0;
    }
    if doc.tags_joined.contains(needle) {
        score += 2.0;
    }
    if doc.body.contains(needle) {
        score += 1.0;
    }
    score
}
