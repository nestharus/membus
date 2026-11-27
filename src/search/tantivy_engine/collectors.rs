use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::schema::TantivyDocument;
use tantivy::store::StoreReader;
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader};

use crate::search::overlay::OverlaySnapshot;
use crate::search::trigram::contains_exact_substring;
use crate::types::{sanitize_score, SearchHit};

use super::schema::{extract_stored_doc, SearchSchemaFields};

/// Min-heap wrapper ordering the *worst* hit at the top so a bounded heap of size `limit`
/// retains the top-K hits by `(score desc, namespace asc, doc_id asc)`.
pub(super) struct HeapHit {
    doc_key: String,
    hit: SearchHit,
}

impl PartialEq for HeapHit {
    fn eq(&self, other: &Self) -> bool {
        self.hit.score.to_bits() == other.hit.score.to_bits()
            && self.hit.namespace == other.hit.namespace
            && self.hit.doc_id == other.hit.doc_id
    }
}

impl Eq for HeapHit {}

impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reversed so BinaryHeap (max-heap) pops the *worst* candidate first.
        other
            .hit
            .score
            .total_cmp(&self.hit.score)
            .then_with(|| self.hit.namespace.cmp(&other.hit.namespace))
            .then_with(|| self.hit.doc_id.cmp(&other.hit.doc_id))
    }
}

fn push_bounded_hit(heap: &mut BinaryHeap<HeapHit>, limit: usize, candidate: HeapHit) {
    if heap.len() < limit {
        heap.push(candidate);
    } else if let Some(worst) = heap.peek() {
        // Candidate is better than worst when `candidate < worst` under reversed `Ord`.
        if candidate < *worst {
            heap.pop();
            heap.push(candidate);
        }
    }
}

fn merge_heap_fruits(
    limit: usize,
    segment_fruits: Vec<Result<(Vec<HeapHit>, usize), String>>,
) -> tantivy::Result<(Vec<(String, SearchHit)>, usize)> {
    let mut merged_heap: BinaryHeap<HeapHit> = BinaryHeap::with_capacity(limit.min(1024));
    let mut total = 0usize;
    for res in segment_fruits {
        match res {
            Ok((items, count)) => {
                total = total.saturating_add(count);
                for item in items {
                    push_bounded_hit(&mut merged_heap, limit, item);
                }
            }
            Err(msg) => return Err(tantivy::TantivyError::SystemError(msg)),
        }
    }
    let hits = merged_heap
        .into_iter()
        .map(|h| (h.doc_key, h.hit))
        .collect();
    Ok((hits, total))
}

/// Streaming collector for `RevokePrefix` with ZERO hard document cap.
pub(super) struct RevokePrefixCollector {
    pub(super) fields: SearchSchemaFields,
    pub(super) prefix: String,
    pub(super) revoke_seq: u64,
    pub(super) revoke_op_idx: u32,
}

pub(super) struct RevokePrefixSegmentCollector {
    fields: SearchSchemaFields,
    prefix: String,
    revoke_seq: u64,
    revoke_op_idx: u32,
    store_reader: StoreReader,
    matched_keys: Vec<String>,
    error: Option<String>,
}

impl Collector for RevokePrefixCollector {
    type Fruit = Vec<String>;
    type Child = RevokePrefixSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(RevokePrefixSegmentCollector {
            fields: self.fields,
            prefix: self.prefix.clone(),
            revoke_seq: self.revoke_seq,
            revoke_op_idx: self.revoke_op_idx,
            store_reader,
            matched_keys: Vec::new(),
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<Vec<String>, String>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut out = Vec::new();
        for res in segment_fruits {
            match res {
                Ok(mut keys) => out.append(&mut keys),
                Err(msg) => return Err(tantivy::TantivyError::SystemError(msg)),
            }
        }
        Ok(out)
    }
}

impl SegmentCollector for RevokePrefixSegmentCollector {
    type Fruit = Result<Vec<String>, String>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.error.is_some() {
            return;
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                if let Some(extracted) = extract_stored_doc(&tdoc, self.fields) {
                    if extracted.doc_id.starts_with(self.prefix.as_str())
                        && (extracted.doc_seq, extracted.doc_op_idx)
                            < (self.revoke_seq, self.revoke_op_idx)
                    {
                        self.matched_keys.push(extracted.doc_key);
                    }
                }
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok(self.matched_keys)
        }
    }
}

/// Streaming collector for `SearchMode::Bm25` when Tier-0 overlay has pending updates,
/// tombstones, or prefix revocations — prevents recall collapse with O(limit) memory.
pub(super) struct FilteredBm25Collector {
    pub(super) fields: SearchSchemaFields,
    pub(super) overlay: Arc<OverlaySnapshot>,
    pub(super) limit: usize,
}

pub(super) struct FilteredBm25SegmentCollector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    limit: usize,
    store_reader: StoreReader,
    heap: BinaryHeap<HeapHit>,
    matched_count: usize,
    error: Option<String>,
}

impl Collector for FilteredBm25Collector {
    type Fruit = (Vec<(String, SearchHit)>, usize);
    type Child = FilteredBm25SegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(FilteredBm25SegmentCollector {
            fields: self.fields,
            overlay: Arc::clone(&self.overlay),
            limit: self.limit,
            store_reader,
            heap: BinaryHeap::with_capacity(self.limit.min(1024)),
            matched_count: 0,
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<(Vec<HeapHit>, usize), String>>,
    ) -> tantivy::Result<Self::Fruit> {
        merge_heap_fruits(self.limit, segment_fruits)
    }
}

impl SegmentCollector for FilteredBm25SegmentCollector {
    type Fruit = Result<(Vec<HeapHit>, usize), String>;

    fn collect(&mut self, doc: DocId, raw_score: Score) {
        if self.error.is_some() {
            return;
        }
        let score = sanitize_score(raw_score);
        // Fast lower-bound prune BEFORE reading stored document when heap is already full.
        if self.heap.len() >= self.limit {
            if let Some(worst) = self.heap.peek() {
                if score < worst.hit.score {
                    self.matched_count = self.matched_count.saturating_add(1);
                    return;
                }
            }
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                let Some(extracted) = extract_stored_doc(&tdoc, self.fields) else {
                    return;
                };
                if self.overlay.shadows_tantivy_hit(
                    &extracted.doc_key,
                    &extracted.namespace,
                    &extracted.doc_id,
                    extracted.doc_seq,
                    extracted.doc_op_idx,
                ) {
                    return;
                }
                self.matched_count = self.matched_count.saturating_add(1);
                push_bounded_hit(
                    &mut self.heap,
                    self.limit,
                    HeapHit {
                        doc_key: extracted.doc_key,
                        hit: SearchHit {
                            namespace: extracted.namespace,
                            doc_id: extracted.doc_id,
                            kind: extracted.kind,
                            title: extracted.title,
                            tags: extracted.tags,
                            score,
                        },
                    },
                );
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok((self.heap.into_vec(), self.matched_count))
        }
    }
}

/// Streaming exact-substring collector for `SearchMode::LiteralSubstring` with ZERO 10k cap.
pub(super) struct LiteralSubstringCollector {
    pub(super) fields: SearchSchemaFields,
    pub(super) overlay: Arc<OverlaySnapshot>,
    pub(super) needle: String,
    pub(super) limit: usize,
}

pub(super) struct LiteralSubstringSegmentCollector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    needle: String,
    limit: usize,
    store_reader: StoreReader,
    heap: BinaryHeap<HeapHit>,
    matched_count: usize,
    error: Option<String>,
}

impl Collector for LiteralSubstringCollector {
    type Fruit = (Vec<(String, SearchHit)>, usize);
    type Child = LiteralSubstringSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(LiteralSubstringSegmentCollector {
            fields: self.fields,
            overlay: Arc::clone(&self.overlay),
            needle: self.needle.clone(),
            limit: self.limit,
            store_reader,
            heap: BinaryHeap::with_capacity(self.limit.min(1024)),
            matched_count: 0,
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<(Vec<HeapHit>, usize), String>>,
    ) -> tantivy::Result<Self::Fruit> {
        merge_heap_fruits(self.limit, segment_fruits)
    }
}

impl SegmentCollector for LiteralSubstringSegmentCollector {
    type Fruit = Result<(Vec<HeapHit>, usize), String>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.error.is_some() {
            return;
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                let Some(extracted) = extract_stored_doc(&tdoc, self.fields) else {
                    return;
                };
                if self.overlay.shadows_tantivy_hit(
                    &extracted.doc_key,
                    &extracted.namespace,
                    &extracted.doc_id,
                    extracted.doc_seq,
                    extracted.doc_op_idx,
                ) {
                    return;
                }
                let tags_joined = extracted.tags.join(" ");
                if !contains_exact_substring(
                    &self.needle,
                    &[&extracted.title, &tags_joined, &extracted.body],
                ) {
                    return;
                }
                let mut score = 0.0f32;
                if extracted.title.contains(&self.needle) {
                    score += 3.0;
                }
                if tags_joined.contains(&self.needle) {
                    score += 2.0;
                }
                if extracted.body.contains(&self.needle) {
                    score += 1.0;
                }
                self.matched_count = self.matched_count.saturating_add(1);
                push_bounded_hit(
                    &mut self.heap,
                    self.limit,
                    HeapHit {
                        doc_key: extracted.doc_key,
                        hit: SearchHit {
                            namespace: extracted.namespace,
                            doc_id: extracted.doc_id,
                            kind: extracted.kind,
                            title: extracted.title,
                            tags: extracted.tags,
                            score: sanitize_score(score),
                        },
                    },
                );
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok((self.heap.into_vec(), self.matched_count))
        }
    }
}
