use std::collections::HashMap;
use std::sync::Arc;

use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{IndexRecordOption, TantivyDocument};
use tantivy::{Searcher, Term};

use crate::search::overlay::OverlaySnapshot;
use crate::search::trigram::query_trigrams;
use crate::types::{
    sanitize_score, validate_namespace, MembusError, SearchHit, SearchMode, SearchPage, SearchQuery,
};

use super::collectors::{FilteredBm25Collector, LiteralSubstringCollector};
use super::schema::{extract_stored_doc, map_tantivy_err, SearchSchemaFields, MAX_SEARCH_LIMIT};
use super::state::SearchEngine;

impl SearchEngine {
    /// Execute a BM25 or exact case-sensitive literal substring search merging Tier-0 RAM
    /// live delta entries and tombstones over committed Tantivy segments (0 ms read-your-writes).
    pub fn search(&self, query: &SearchQuery) -> Result<SearchPage, MembusError> {
        if query.limit == 0 || query.limit > MAX_SEARCH_LIMIT {
            return Err(MembusError::InvalidArgument(format!(
                "search limit {} must be in 1..={MAX_SEARCH_LIMIT}",
                query.limit
            )));
        }
        if let Some(ns) = &query.namespace {
            validate_namespace(ns)?;
        }
        if query.query.is_empty() {
            return Ok(SearchPage {
                hits: Vec::new(),
                total_hits_estimate: 0,
                committed_seq: self.committed_seq(),
            });
        }

        // Capture Tier-0 overlay snapshot BEFORE acquiring Tantivy searcher so any doc pruned
        // from overlay has already been reloaded into the searcher (zero commit-boundary gap).
        let overlay_snap = Arc::new(self.overlay.snapshot());
        let searcher = self.reader.searcher();
        let committed_seq = self.committed_seq();
        let mut merged: HashMap<String, SearchHit> = HashMap::new();

        // 1. Evaluate committed Tantivy segments.
        let tantivy_matched_estimate = match query.mode {
            SearchMode::Bm25 => {
                self.eval_bm25_tantivy(&searcher, &overlay_snap, query, &mut merged)?
            }
            SearchMode::LiteralSubstring => {
                self.eval_literal_substring_tantivy(&searcher, &overlay_snap, query, &mut merged)?
            }
        };

        // 2. Merge live Tier-0 RAM hits (overriding any stale Tantivy version).
        let live_hits = overlay_snap.evaluate_live_hits(query);
        let live_extra = live_hits
            .iter()
            .filter(|(k, _)| !merged.contains_key(k))
            .count();
        for (doc_key, hit) in live_hits {
            merged.insert(doc_key, hit);
        }

        let total_hits_estimate = tantivy_matched_estimate
            .saturating_add(live_extra)
            .max(merged.len());
        let mut hits: Vec<SearchHit> = merged.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.namespace.cmp(&b.namespace))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        hits.truncate(query.limit);

        Ok(SearchPage {
            hits,
            total_hits_estimate,
            committed_seq,
        })
    }

    fn eval_bm25_tantivy(
        &self,
        searcher: &Searcher,
        overlay_snap: &Arc<OverlaySnapshot>,
        query: &SearchQuery,
        merged: &mut HashMap<String, SearchHit>,
    ) -> Result<usize, MembusError> {
        let mut parser = QueryParser::for_index(
            &self.index,
            vec![self.fields.title, self.fields.tags, self.fields.body],
        );
        parser.set_field_boost(self.fields.title, 3.0);
        parser.set_field_boost(self.fields.tags, 2.0);
        parser.set_field_boost(self.fields.body, 1.0);
        let (text_query, _parse_errs) = parser.parse_query_lenient(&query.query);
        let combined = wrap_filters(text_query, self.fields, query);

        if overlay_snap.is_empty() {
            let top_docs = searcher
                .search(&combined, &TopDocs::with_limit(query.limit))
                .map_err(map_tantivy_err)?;
            let matched = top_docs.len();
            for (score, addr) in top_docs {
                let doc: TantivyDocument = searcher.doc(addr).map_err(map_tantivy_err)?;
                let Some(extracted) = extract_stored_doc(&doc, self.fields) else {
                    continue;
                };
                merged.insert(
                    extracted.doc_key,
                    SearchHit {
                        namespace: extracted.namespace,
                        doc_id: extracted.doc_id,
                        kind: extracted.kind,
                        title: extracted.title,
                        tags: extracted.tags,
                        score: sanitize_score(score),
                    },
                );
            }
            Ok(matched)
        } else {
            // Filtered streaming collector prevents recall collapse under RevokePrefix / tombstones.
            let collector = FilteredBm25Collector {
                fields: self.fields,
                overlay: Arc::clone(overlay_snap),
                limit: query.limit,
            };
            let (hits_vec, total_unshadowed) = searcher
                .search(&combined, &collector)
                .map_err(map_tantivy_err)?;
            for (doc_key, hit) in hits_vec {
                merged.insert(doc_key, hit);
            }
            Ok(total_unshadowed)
        }
    }

    fn eval_literal_substring_tantivy(
        &self,
        searcher: &Searcher,
        overlay_snap: &Arc<OverlaySnapshot>,
        query: &SearchQuery,
        merged: &mut HashMap<String, SearchHit>,
    ) -> Result<usize, MembusError> {
        let grams = query_trigrams(&query.query);
        let base_query: Box<dyn Query> = if !grams.is_empty() {
            let gram_clauses: Vec<(Occur, Box<dyn Query>)> = grams
                .into_iter()
                .map(|g| {
                    let term = Term::from_field_text(self.fields.trigrams, &g);
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                    )
                })
                .collect();
            let all_grams = BooleanQuery::new(gram_clauses);
            let overflow_term = Term::from_field_u64(self.fields.trigram_overflow, 1);
            let overflow_query = TermQuery::new(overflow_term, IndexRecordOption::Basic);
            Box::new(BooleanQuery::new(vec![
                (Occur::Should, Box::new(all_grams)),
                (Occur::Should, Box::new(overflow_query)),
            ]))
        } else {
            Box::new(AllQuery)
        };
        let combined = wrap_filters(base_query, self.fields, query);

        // Streaming exact-substring verification collector (ZERO 10k unverified candidate cap).
        let collector = LiteralSubstringCollector {
            fields: self.fields,
            overlay: Arc::clone(overlay_snap),
            needle: query.query.clone(),
            limit: query.limit,
        };
        let (hits_vec, total_verified) = searcher
            .search(&combined, &collector)
            .map_err(map_tantivy_err)?;
        for (doc_key, hit) in hits_vec {
            merged.insert(doc_key, hit);
        }
        Ok(total_verified)
    }
}

fn wrap_filters(
    inner: Box<dyn Query>,
    fields: SearchSchemaFields,
    query: &SearchQuery,
) -> BooleanQuery {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, inner)];
    if let Some(ns) = &query.namespace {
        let term = Term::from_field_text(fields.namespace, ns);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }
    if let Some(kind) = &query.kind {
        let term = Term::from_field_text(fields.kind, kind);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }
    BooleanQuery::new(clauses)
}
