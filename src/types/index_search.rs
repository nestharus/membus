use serde::{Deserialize, Serialize};

use super::core::{validate_namespace, CommitResult, MembusError, WriteMutation, MAX_KEY_BYTES};

/// Client-emitted ordered secondary index entry stored in `idx_ord`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexEntry {
    pub index_id: String,
    pub sort_key: Vec<u8>,
    pub summary: Vec<u8>,
}

/// Client-emitted labeled directed graph edge stored bidirectionally in `idx_edge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeEntry {
    pub index_id: String,
    pub src_id: Vec<u8>,
    pub rel_label: Vec<u8>,
    pub dst_id: Vec<u8>,
    pub meta: Vec<u8>,
}

/// Optional declarative index postings emitted atomically alongside a primary KV mutation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMutations {
    pub ordered: Vec<OrderedIndexEntry>,
    pub edges: Vec<GraphEdgeEntry>,
}

impl IndexMutations {
    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty() && self.edges.is_empty()
    }
}

/// A transaction mutation optionally carrying secondary index postings and search ops.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedMutation {
    pub mutation: WriteMutation,
    pub indexes: Option<IndexMutations>,
    pub search_op: Option<SearchLogOp>,
}

/// Traversal direction for graph neighbor queries and multi-hop BFS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GraphDirection {
    Out = 1,
    In = 2,
    Both = 3,
}

impl GraphDirection {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Out),
            2 => Ok(Self::In),
            3 => Ok(Self::Both),
            other => Err(MembusError::Protocol(format!(
                "unknown graph direction: {other}"
            ))),
        }
    }
}

/// A decoded entry from an ordered secondary index query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexHit {
    pub sort_key: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub summary: Vec<u8>,
}

/// Stateless keyset-paginated ordered index scan result pinned to an MVCC snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexPage {
    pub entries: Vec<OrderedIndexHit>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// A decoded bidirectional adjacency edge returned from graph queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeRecord {
    pub src_id: Vec<u8>,
    pub rel_label: Vec<u8>,
    pub dst_id: Vec<u8>,
    pub direction: GraphDirection,
    pub meta: Vec<u8>,
}

/// Keyset-paginated single-hop graph neighbor query result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeighborPage {
    pub edges: Vec<GraphEdgeRecord>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// Bounded multi-hop BFS traversal result executed strictly inside `idx_edge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BfsResult {
    pub visited_nodes: Vec<Vec<u8>>,
    pub traversed_edges: Vec<GraphEdgeRecord>,
    pub frontier_nodes: Vec<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// Visibility policy controlling what fields may enter the embedded search index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SearchVisibility {
    Public = 0,
    MetadataOnly = 1,
    Sealed = 2,
}

impl SearchVisibility {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            0 => Ok(Self::Public),
            1 => Ok(Self::MetadataOnly),
            2 => Ok(Self::Sealed),
            other => Err(MembusError::Protocol(format!(
                "unknown search visibility: {other}"
            ))),
        }
    }
}

/// Searchable document payload submitted to the embedded Tantivy engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchDocument {
    pub namespace: String,
    pub doc_id: String,
    pub kind: String,
    pub title: String,
    pub tags: Vec<String>,
    pub body: String,
    pub visibility: SearchVisibility,
}

/// Search mutation logged durably in `CF_LOG` for deterministic restart replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchLogOp {
    Index(SearchDocument),
    Delete { namespace: String, doc_id: String },
    RevokePrefix { namespace: String, prefix: String },
}

/// One replayed search mutation with its durable commit sequence, intra-tx index, and DB epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchLogRecord {
    pub seq: u64,
    pub op_idx: u32,
    pub epoch: u64,
    pub op: SearchLogOp,
}

/// Paginated search log scan page returned by `StorageEngine::scan_search_log_after`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchLogPage {
    pub records: Vec<SearchLogRecord>,
    pub scanned_through_seq: u64,
}

/// Search execution mode supported by [`crate::search::SearchEngine`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SearchMode {
    Bm25 = 1,
    LiteralSubstring = 2,
}

impl SearchMode {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Bm25),
            2 => Ok(Self::LiteralSubstring),
            other => Err(MembusError::Protocol(format!(
                "unknown search mode: {other}"
            ))),
        }
    }
}

/// Structured query submitted to [`crate::search::SearchEngine`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query: String,
    pub mode: SearchMode,
    pub namespace: Option<String>,
    pub kind: Option<String>,
    pub limit: usize,
}

/// One ranked search hit returned across Tier-0 RAM overlay and Tantivy segments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub namespace: String,
    pub doc_id: String,
    pub kind: String,
    pub title: String,
    pub tags: Vec<String>,
    pub score: f32,
}

/// Search response page with Tantivy committed watermark.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub total_hits_estimate: usize,
    pub committed_seq: u64,
}

/// Secondary search-index synchronization status reported after an authoritative storage commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchSyncStatus {
    /// Post-commit search ingestion into the Tier-0 overlay succeeded synchronously.
    #[default]
    Synced,
    /// Authoritative storage committed, but secondary search ingestion failed and was
    /// deferred for catch-up replay from `CF_LOG`.
    Deferred,
}

impl SearchSyncStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Deferred => "deferred",
        }
    }
}

/// Result of an authoritative storage write paired with its secondary search synchronization state.
///
/// Once RocksDB commits authoritatively, secondary search-index ingestion failures NEVER
/// convert the write into a client-visible rejection; instead they are logged, counted,
/// marked dirty for `CF_LOG` catch-up replay, and reported as [`SearchSyncStatus::Deferred`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedCommit {
    pub commit: CommitResult,
    pub search_sync: SearchSyncStatus,
    pub idempotent_replay: bool,
}

/// Validate a search document's namespace and doc_id without mutating input.
pub fn validate_search_document(doc: &SearchDocument) -> Result<(), MembusError> {
    validate_namespace(&doc.namespace)?;
    if doc.doc_id.is_empty() {
        return Err(MembusError::InvalidArgument(
            "search doc_id must not be empty".to_owned(),
        ));
    }
    if doc.doc_id.len() > MAX_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "search doc_id length {} exceeds maximum {MAX_KEY_BYTES}",
            doc.doc_id.len()
        )));
    }
    if doc.doc_id.as_bytes().contains(&0) {
        return Err(MembusError::InvalidArgument(
            "search doc_id must not contain NUL bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validate and visibility-sanitize a [`SearchLogOp`] BEFORE it enters `CF_LOG`, WAL, or search.
pub fn sanitize_search_log_op(op: &SearchLogOp) -> Result<SearchLogOp, MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            validate_search_document(doc)?;
            match doc.visibility {
                SearchVisibility::Sealed => Ok(SearchLogOp::Delete {
                    namespace: doc.namespace.clone(),
                    doc_id: doc.doc_id.clone(),
                }),
                SearchVisibility::MetadataOnly => {
                    let mut clean = doc.clone();
                    clean.body.clear();
                    Ok(SearchLogOp::Index(clean))
                }
                SearchVisibility::Public => Ok(SearchLogOp::Index(doc.clone())),
            }
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            validate_namespace(namespace)?;
            if doc_id.is_empty() {
                return Err(MembusError::InvalidArgument(
                    "search doc_id must not be empty".to_owned(),
                ));
            }
            if doc_id.len() > MAX_KEY_BYTES || doc_id.as_bytes().contains(&0) {
                return Err(MembusError::InvalidArgument(
                    "invalid search doc_id".to_owned(),
                ));
            }
            Ok(SearchLogOp::Delete {
                namespace: namespace.clone(),
                doc_id: doc_id.clone(),
            })
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            validate_namespace(namespace)?;
            if prefix.len() > MAX_KEY_BYTES || prefix.as_bytes().contains(&0) {
                return Err(MembusError::InvalidArgument(
                    "invalid search revoke prefix".to_owned(),
                ));
            }
            Ok(SearchLogOp::RevokePrefix {
                namespace: namespace.clone(),
                prefix: prefix.clone(),
            })
        }
    }
}

/// Sanitize a floating-point search score so NaN/Inf can never poison JSON serialization.
pub fn sanitize_score(score: f32) -> f32 {
    if score.is_finite() {
        score
    } else {
        0.0
    }
}
