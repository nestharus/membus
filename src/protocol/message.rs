use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::types::{
    sanitize_score, Ack, BfsResult, CollectionDescriptor, CommitResult, ExpectedVersion,
    HealthStatus, IndexMutations, IndexedMutation, LeaseGrant, LeaseInfo, MembusError, MutationOp,
    NamespaceParityDigest, NeighborPage, OrderedIndexPage, ParityDigestMode, RecordMeta,
    ReplicatedApplyResult, ReplicatedDelta, ScanOptions, ScanPage, SearchDocument, SearchLogOp,
    SearchPage, SearchQuery, SignalEvent, SignalKind, SlotRole, SlotRoleState, StoredRecord,
    VersionGuard, WriteMutation,
};

const OP_GET: u8 = 1;
const OP_GET_MANY: u8 = 2;
const OP_PUT: u8 = 3;
const OP_REMOVE: u8 = 4;
const OP_COMMIT_TX: u8 = 5;
const OP_OPEN_SNAPSHOT: u8 = 6;
const OP_CLOSE_SNAPSHOT: u8 = 7;
const OP_SCAN_PREFIX: u8 = 8;
const OP_SCAN_RANGE: u8 = 9;
const OP_SUBSCRIBE_EACH: u8 = 10;
const OP_SUBSCRIBE_LIVE: u8 = 11;
const OP_CHECKPOINT: u8 = 12;
const OP_HEALTH: u8 = 13;
const OP_PUT_BLOB: u8 = 14;
const OP_GET_BLOB: u8 = 15;
pub(crate) const OP_AUTHENTICATE: u8 = 16;
const OP_PUT_INDEXED: u8 = 17;
const OP_REMOVE_INDEXED: u8 = 18;
const OP_COMMIT_INDEXED_TX: u8 = 19;
const OP_QUERY_ORDERED_INDEX: u8 = 20;
const OP_QUERY_NEIGHBORS: u8 = 21;
const OP_BFS_TRAVERSE: u8 = 22;
const OP_PUT_CAS_BLOB: u8 = 23;
const OP_GET_CAS_BLOB: u8 = 24;
const OP_HAS_CAS_BLOB: u8 = 25;
const OP_ACQUIRE_LEASE: u8 = 26;
const OP_RENEW_LEASE: u8 = 27;
const OP_RELEASE_LEASE: u8 = 28;
const OP_INSPECT_LEASE: u8 = 29;
const OP_VALIDATE_LEASE: u8 = 30;
const OP_REGISTER_COLLECTION: u8 = 31;
const OP_GET_COLLECTION: u8 = 32;
const OP_LIST_COLLECTIONS: u8 = 33;
const OP_GET_COLLECTION_VERSION: u8 = 34;
const OP_BEGIN_MIGRATION: u8 = 35;
const OP_COMMIT_MIGRATION: u8 = 36;
const OP_ABORT_MIGRATION: u8 = 37;
const OP_INDEX_DOCUMENT: u8 = 38;
const OP_DELETE_SEARCH_DOCUMENT: u8 = 39;
const OP_REVOKE_SEARCH_PREFIX: u8 = 40;
const OP_SEARCH: u8 = 41;
const OP_HEALTH_V2: u8 = 42;
const OP_ADVANCE_MIGRATION: u8 = 43;
const OP_ENTER_REVERSE_SHADOW: u8 = 44;
const OP_ROLLBACK_MIGRATION: u8 = 45;
const OP_CONTRACT_MIGRATION: u8 = 46;
const OP_NAMESPACE_DIGEST: u8 = 47;
const OP_TRANSITION_SLOT_ROLE: u8 = 48;
const OP_APPLY_REPLICATED_DELTA: u8 = 49;

const RESP_GET: u8 = 1;
const RESP_GET_MANY: u8 = 2;
const RESP_COMMIT: u8 = 3;
const RESP_SNAPSHOT_OPENED: u8 = 4;
const RESP_UNIT: u8 = 5;
const RESP_SCAN: u8 = 6;
const RESP_SUBSCRIBED: u8 = 7;
const RESP_CHECKPOINT: u8 = 8;
const RESP_HEALTH: u8 = 9;
const RESP_BLOB: u8 = 10;
const RESP_AUTHENTICATED: u8 = 11;
const RESP_ORDERED_INDEX: u8 = 12;
const RESP_NEIGHBORS: u8 = 13;
const RESP_BFS: u8 = 14;
const RESP_CAS_BLOB_DIGEST: u8 = 15;
const RESP_BOOL: u8 = 16;
const RESP_LEASE: u8 = 17;
const RESP_OPT_LEASE: u8 = 18;
const RESP_COLLECTION: u8 = 19;
const RESP_OPT_COLLECTION: u8 = 20;
const RESP_COLLECTIONS: u8 = 21;
const RESP_SEARCH: u8 = 22;
const RESP_HEALTH_V2: u8 = 23;
const RESP_NAMESPACE_DIGEST: u8 = 24;
const RESP_SLOT_ROLE: u8 = 25;
const RESP_REPLICATED_APPLY: u8 = 26;
const RESP_ERROR: u8 = 255;

const ERR_INVALID_ARG: u8 = 1;
const ERR_CONFLICT: u8 = 2;
const ERR_TX_CONFLICT: u8 = 3;
const ERR_BUSY: u8 = 4;
const ERR_GAP: u8 = 5;
const ERR_NOT_FOUND: u8 = 6;
const ERR_CORRUPTION: u8 = 7;
const ERR_STORAGE: u8 = 8;
const ERR_PROTOCOL: u8 = 9;
const ERR_IO: u8 = 10;
const ERR_CLOSED: u8 = 11;
const ERR_FRAME_TOO_LARGE: u8 = 12;
const ERR_UNAUTHORIZED: u8 = 13;
const ERR_LEASE_HELD: u8 = 14;
const ERR_ROLE_FENCED: u8 = 15;
const ERR_IDEMPOTENCY_CONFLICT: u8 = 16;

const MAX_BATCH_ITEMS: usize = 65_536;

/// Strongly-typed RPC request carried in `FrameKind::Request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Get {
        namespace: String,
        key: Vec<u8>,
        snapshot_id: Option<u64>,
    },
    GetMany {
        namespace: String,
        keys: Vec<Vec<u8>>,
        snapshot_id: Option<u64>,
    },
    Put {
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
    },
    Remove {
        namespace: String,
        key: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
    },
    CommitTx {
        ack: Ack,
        guards: Vec<VersionGuard>,
        mutations: Vec<WriteMutation>,
    },
    OpenSnapshot,
    CloseSnapshot {
        snapshot_id: u64,
    },
    ScanPrefix {
        namespace: String,
        prefix: Vec<u8>,
        options: ScanOptions,
        snapshot_id: Option<u64>,
    },
    ScanRange {
        namespace: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        options: ScanOptions,
        snapshot_id: Option<u64>,
    },
    SubscribeEachChange {
        namespace: String,
        key_prefix: Vec<u8>,
        epoch: u64,
        since_seq: u64,
    },
    SubscribeLive {
        namespace: String,
        key_prefix: Vec<u8>,
        max_pending: Option<u32>,
    },
    Checkpoint {
        dest_dir: String,
    },
    Health,
    PutBlob {
        digest: Vec<u8>,
        data: Vec<u8>,
        ack: Ack,
    },
    GetBlob {
        digest: Vec<u8>,
    },
    Authenticate {
        token: String,
    },
    PutIndexed {
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<IndexMutations>,
        search_op: Option<SearchLogOp>,
    },
    RemoveIndexed {
        namespace: String,
        key: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<SearchLogOp>,
    },
    CommitIndexedTx {
        ack: Ack,
        guards: Vec<VersionGuard>,
        mutations: Vec<IndexedMutation>,
    },
    QueryOrderedIndex {
        namespace: String,
        index_id: String,
        options: OrderedQueryOptions,
        snapshot_id: Option<u64>,
    },
    QueryNeighbors {
        namespace: String,
        index_id: String,
        node_id: Vec<u8>,
        options: NeighborQueryOptions,
        snapshot_id: Option<u64>,
    },
    BfsTraverse {
        namespace: String,
        index_id: String,
        seed_node: Vec<u8>,
        options: BfsOptions,
        snapshot_id: Option<u64>,
    },
    PutCasBlob {
        namespace: String,
        data: Vec<u8>,
        ack: Ack,
    },
    GetCasBlob {
        namespace: String,
        sha256: [u8; 32],
    },
    HasCasBlob {
        namespace: String,
        sha256: [u8; 32],
    },
    AcquireLease {
        namespace: String,
        resource: String,
        holder: String,
        ttl_ms: u64,
    },
    RenewLease {
        namespace: String,
        resource: String,
        holder: String,
        generation: u64,
        token: String,
        ttl_ms: u64,
    },
    ReleaseLease {
        namespace: String,
        resource: String,
        holder: String,
        generation: u64,
        token: String,
    },
    InspectLease {
        namespace: String,
        resource: String,
    },
    ValidateLease {
        namespace: String,
        resource: String,
        holder: String,
        generation: u64,
        token: String,
    },
    RegisterCollection {
        expected_version: Option<u32>,
        descriptor: CollectionDescriptor,
    },
    GetCollection {
        namespace: String,
    },
    ListCollections,
    GetCollectionVersion {
        namespace: String,
        version: u32,
    },
    BeginMigration {
        namespace: String,
        expected_version: u32,
        target_version: u32,
        target_sha256: [u8; 32],
        target_schema_bytes: Vec<u8>,
    },
    CommitMigration {
        namespace: String,
        target_version: u32,
        verified_through: u64,
    },
    AbortMigration {
        namespace: String,
        target_version: u32,
    },
    IndexDocument {
        doc: SearchDocument,
        ack: Ack,
    },
    DeleteSearchDocument {
        namespace: String,
        doc_id: String,
        ack: Ack,
    },
    RevokeSearchPrefix {
        namespace: String,
        prefix: String,
        ack: Ack,
    },
    Search(SearchQuery),
    HealthV2,
    AdvanceMigrationVerification {
        namespace: String,
        target_version: u32,
        verified_through: u64,
    },
    EnterReverseShadow {
        namespace: String,
        target_version: u32,
        verified_through: u64,
    },
    RollbackMigration {
        namespace: String,
        rollback_version: u32,
    },
    ContractMigration {
        namespace: String,
        expected_version: u32,
    },
    NamespaceDigest {
        namespace: String,
        key_prefix: Vec<u8>,
        mode: ParityDigestMode,
    },
    TransitionSlotRole {
        expected_role: SlotRole,
        expected_role_epoch: u64,
        new_role: SlotRole,
    },
    ApplyReplicatedDelta {
        delta: ReplicatedDelta,
        ack: Ack,
        route_origin: Option<String>,
    },
}

/// Strongly-typed RPC response carried in `FrameKind::Response`.
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Get(Option<StoredRecord>),
    GetMany(Vec<Option<StoredRecord>>),
    Commit(CommitResult),
    SnapshotOpened { snapshot_id: u64, snapshot_seq: u64 },
    Unit,
    Scan(ScanPage),
    Subscribed { epoch: u64, start_seq: u64 },
    Checkpoint { seq: u64 },
    Health(HealthStatus),
    HealthV2(HealthStatus),
    Blob(Option<Vec<u8>>),
    Authenticated { principal_id: String },
    OrderedIndex(OrderedIndexPage),
    Neighbors(NeighborPage),
    Bfs(BfsResult),
    CasBlobDigest([u8; 32]),
    Bool(bool),
    Lease(LeaseGrant),
    OptLease(Option<LeaseInfo>),
    Collection(CollectionDescriptor),
    OptCollection(Option<CollectionDescriptor>),
    Collections(Vec<CollectionDescriptor>),
    Search(SearchPage),
    NamespaceDigest(NamespaceParityDigest),
    SlotRole(SlotRoleState),
    ReplicatedApply(ReplicatedApplyResult),
    Error(MembusError),
}

pub fn encode_request(req: &Request) -> Result<Bytes, MembusError> {
    let mut buf = BytesMut::with_capacity(128);
    match req {
        Request::Get {
            namespace,
            key,
            snapshot_id,
        } => {
            buf.put_u8(OP_GET);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::GetMany {
            namespace,
            keys,
            snapshot_id,
        } => {
            buf.put_u8(OP_GET_MANY);
            put_str(&mut buf, namespace);
            let key_count = u32::try_from(keys.len()).map_err(|_| {
                MembusError::InvalidArgument("get_many key count exceeds u32".to_owned())
            })?;
            buf.put_u32(key_count);
            for key in keys {
                put_bytes(&mut buf, key);
            }
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::Put {
            namespace,
            key,
            value,
            ack,
            expected,
        } => {
            buf.put_u8(OP_PUT);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            put_bytes(&mut buf, value);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
        }
        Request::Remove {
            namespace,
            key,
            ack,
            expected,
        } => {
            buf.put_u8(OP_REMOVE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
        }
        Request::CommitTx {
            ack,
            guards,
            mutations,
        } => {
            buf.put_u8(OP_COMMIT_TX);
            buf.put_u8(*ack as u8);
            let guard_count = u32::try_from(guards.len()).map_err(|_| {
                MembusError::InvalidArgument("commit_tx guard count exceeds u32".to_owned())
            })?;
            buf.put_u32(guard_count);
            for guard in guards {
                put_str(&mut buf, &guard.namespace);
                put_bytes(&mut buf, &guard.key);
                put_expected(&mut buf, guard.expected);
            }
            let mut_count = u32::try_from(mutations.len()).map_err(|_| {
                MembusError::InvalidArgument("commit_tx mutation count exceeds u32".to_owned())
            })?;
            buf.put_u32(mut_count);
            for m in mutations {
                put_str(&mut buf, &m.namespace);
                put_bytes(&mut buf, &m.key);
                match &m.op {
                    MutationOp::Put { value } => {
                        buf.put_u8(1);
                        put_bytes(&mut buf, value);
                    }
                    MutationOp::Remove => {
                        buf.put_u8(2);
                    }
                }
            }
        }
        Request::OpenSnapshot => {
            buf.put_u8(OP_OPEN_SNAPSHOT);
        }
        Request::CloseSnapshot { snapshot_id } => {
            buf.put_u8(OP_CLOSE_SNAPSHOT);
            buf.put_u64(*snapshot_id);
        }
        Request::ScanPrefix {
            namespace,
            prefix,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_SCAN_PREFIX);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, prefix);
            put_scan_options(&mut buf, options)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_SCAN_RANGE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, start_key);
            put_bytes(&mut buf, end_key);
            put_scan_options(&mut buf, options)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            epoch,
            since_seq,
        } => {
            buf.put_u8(OP_SUBSCRIBE_EACH);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key_prefix);
            buf.put_u64(*epoch);
            buf.put_u64(*since_seq);
        }
        Request::SubscribeLive {
            namespace,
            key_prefix,
            max_pending,
        } => {
            buf.put_u8(OP_SUBSCRIBE_LIVE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key_prefix);
            match max_pending {
                Some(v) => {
                    buf.put_u8(1);
                    buf.put_u32(*v);
                }
                None => buf.put_u8(0),
            }
        }
        Request::Checkpoint { dest_dir } => {
            buf.put_u8(OP_CHECKPOINT);
            put_str(&mut buf, dest_dir);
        }
        Request::Health => {
            buf.put_u8(OP_HEALTH);
        }
        Request::PutBlob { digest, data, ack } => {
            buf.put_u8(OP_PUT_BLOB);
            put_bytes(&mut buf, digest);
            put_bytes(&mut buf, data);
            buf.put_u8(*ack as u8);
        }
        Request::GetBlob { digest } => {
            buf.put_u8(OP_GET_BLOB);
            put_bytes(&mut buf, digest);
        }
        Request::Authenticate { token } => {
            buf.put_u8(OP_AUTHENTICATE);
            put_str(&mut buf, token);
        }
        Request::PutIndexed {
            namespace,
            key,
            value,
            ack,
            expected,
            indexes,
            search_op,
        } => {
            buf.put_u8(OP_PUT_INDEXED);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            put_bytes(&mut buf, value);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
            put_json(&mut buf, &(indexes, search_op))?;
        }
        Request::RemoveIndexed {
            namespace,
            key,
            ack,
            expected,
            search_op,
        } => {
            buf.put_u8(OP_REMOVE_INDEXED);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
            put_json(&mut buf, search_op)?;
        }
        Request::CommitIndexedTx {
            ack,
            guards,
            mutations,
        } => {
            if guards.len() > MAX_BATCH_ITEMS || mutations.len() > MAX_BATCH_ITEMS {
                return Err(MembusError::InvalidArgument(format!(
                    "commit_indexed_tx batch size exceeds maximum {MAX_BATCH_ITEMS}"
                )));
            }
            buf.put_u8(OP_COMMIT_INDEXED_TX);
            buf.put_u8(*ack as u8);
            put_json(&mut buf, &(guards, mutations))?;
        }
        Request::QueryOrderedIndex {
            namespace,
            index_id,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_QUERY_ORDERED_INDEX);
            put_str(&mut buf, namespace);
            put_str(&mut buf, index_id);
            let wire_opts = WireOrderedQueryOptions::from_options(options)?;
            put_json(&mut buf, &wire_opts)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::QueryNeighbors {
            namespace,
            index_id,
            node_id,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_QUERY_NEIGHBORS);
            put_str(&mut buf, namespace);
            put_str(&mut buf, index_id);
            put_bytes(&mut buf, node_id);
            let wire_opts = WireNeighborQueryOptions::from_options(options)?;
            put_json(&mut buf, &wire_opts)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::BfsTraverse {
            namespace,
            index_id,
            seed_node,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_BFS_TRAVERSE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, index_id);
            put_bytes(&mut buf, seed_node);
            let wire_opts = WireBfsOptions::from_options(options)?;
            put_json(&mut buf, &wire_opts)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::PutCasBlob {
            namespace,
            data,
            ack,
        } => {
            buf.put_u8(OP_PUT_CAS_BLOB);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, data);
            buf.put_u8(*ack as u8);
        }
        Request::GetCasBlob { namespace, sha256 } => {
            buf.put_u8(OP_GET_CAS_BLOB);
            put_str(&mut buf, namespace);
            buf.put_slice(sha256);
        }
        Request::HasCasBlob { namespace, sha256 } => {
            buf.put_u8(OP_HAS_CAS_BLOB);
            put_str(&mut buf, namespace);
            buf.put_slice(sha256);
        }
        Request::AcquireLease {
            namespace,
            resource,
            holder,
            ttl_ms,
        } => {
            buf.put_u8(OP_ACQUIRE_LEASE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, resource);
            put_str(&mut buf, holder);
            buf.put_u64(*ttl_ms);
        }
        Request::RenewLease {
            namespace,
            resource,
            holder,
            generation,
            token,
            ttl_ms,
        } => {
            buf.put_u8(OP_RENEW_LEASE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, resource);
            put_str(&mut buf, holder);
            buf.put_u64(*generation);
            put_str(&mut buf, token);
            buf.put_u64(*ttl_ms);
        }
        Request::ReleaseLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            buf.put_u8(OP_RELEASE_LEASE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, resource);
            put_str(&mut buf, holder);
            buf.put_u64(*generation);
            put_str(&mut buf, token);
        }
        Request::InspectLease {
            namespace,
            resource,
        } => {
            buf.put_u8(OP_INSPECT_LEASE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, resource);
        }
        Request::ValidateLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            buf.put_u8(OP_VALIDATE_LEASE);
            put_str(&mut buf, namespace);
            put_str(&mut buf, resource);
            put_str(&mut buf, holder);
            buf.put_u64(*generation);
            put_str(&mut buf, token);
        }
        Request::RegisterCollection {
            expected_version,
            descriptor,
        } => {
            buf.put_u8(OP_REGISTER_COLLECTION);
            put_json(&mut buf, &(expected_version, descriptor))?;
        }
        Request::GetCollection { namespace } => {
            buf.put_u8(OP_GET_COLLECTION);
            put_str(&mut buf, namespace);
        }
        Request::ListCollections => {
            buf.put_u8(OP_LIST_COLLECTIONS);
        }
        Request::GetCollectionVersion { namespace, version } => {
            buf.put_u8(OP_GET_COLLECTION_VERSION);
            put_str(&mut buf, namespace);
            buf.put_u32(*version);
        }
        Request::BeginMigration {
            namespace,
            expected_version,
            target_version,
            target_sha256,
            target_schema_bytes,
        } => {
            buf.put_u8(OP_BEGIN_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*expected_version);
            buf.put_u32(*target_version);
            buf.put_slice(target_sha256);
            put_bytes(&mut buf, target_schema_bytes);
        }
        Request::CommitMigration {
            namespace,
            target_version,
            verified_through,
        } => {
            buf.put_u8(OP_COMMIT_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*target_version);
            buf.put_u64(*verified_through);
        }
        Request::AbortMigration {
            namespace,
            target_version,
        } => {
            buf.put_u8(OP_ABORT_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*target_version);
        }
        Request::IndexDocument { doc, ack } => {
            buf.put_u8(OP_INDEX_DOCUMENT);
            buf.put_u8(*ack as u8);
            put_json(&mut buf, doc)?;
        }
        Request::DeleteSearchDocument {
            namespace,
            doc_id,
            ack,
        } => {
            buf.put_u8(OP_DELETE_SEARCH_DOCUMENT);
            put_str(&mut buf, namespace);
            put_str(&mut buf, doc_id);
            buf.put_u8(*ack as u8);
        }
        Request::RevokeSearchPrefix {
            namespace,
            prefix,
            ack,
        } => {
            buf.put_u8(OP_REVOKE_SEARCH_PREFIX);
            put_str(&mut buf, namespace);
            put_str(&mut buf, prefix);
            buf.put_u8(*ack as u8);
        }
        Request::Search(query) => {
            buf.put_u8(OP_SEARCH);
            put_json(&mut buf, query)?;
        }
        Request::HealthV2 => {
            buf.put_u8(OP_HEALTH_V2);
        }
        Request::AdvanceMigrationVerification {
            namespace,
            target_version,
            verified_through,
        } => {
            buf.put_u8(OP_ADVANCE_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*target_version);
            buf.put_u64(*verified_through);
        }
        Request::EnterReverseShadow {
            namespace,
            target_version,
            verified_through,
        } => {
            buf.put_u8(OP_ENTER_REVERSE_SHADOW);
            put_str(&mut buf, namespace);
            buf.put_u32(*target_version);
            buf.put_u64(*verified_through);
        }
        Request::RollbackMigration {
            namespace,
            rollback_version,
        } => {
            buf.put_u8(OP_ROLLBACK_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*rollback_version);
        }
        Request::ContractMigration {
            namespace,
            expected_version,
        } => {
            buf.put_u8(OP_CONTRACT_MIGRATION);
            put_str(&mut buf, namespace);
            buf.put_u32(*expected_version);
        }
        Request::NamespaceDigest {
            namespace,
            key_prefix,
            mode,
        } => {
            buf.put_u8(OP_NAMESPACE_DIGEST);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key_prefix);
            put_json(&mut buf, mode)?;
        }
        Request::TransitionSlotRole {
            expected_role,
            expected_role_epoch,
            new_role,
        } => {
            buf.put_u8(OP_TRANSITION_SLOT_ROLE);
            buf.put_u8(expected_role.as_u8());
            buf.put_u64(*expected_role_epoch);
            buf.put_u8(new_role.as_u8());
        }
        Request::ApplyReplicatedDelta {
            delta,
            ack,
            route_origin,
        } => {
            buf.put_u8(OP_APPLY_REPLICATED_DELTA);
            buf.put_u8(*ack as u8);
            put_json(&mut buf, &(delta, route_origin))?;
        }
    }
    Ok(buf.freeze())
}

pub fn decode_request(raw: &[u8]) -> Result<Request, MembusError> {
    let mut cur = raw;
    let op = take_u8(&mut cur)?;
    let req = match op {
        OP_GET => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::Get {
                namespace,
                key,
                snapshot_id,
            }
        }
        OP_GET_MANY => {
            let namespace = take_string(&mut cur)?;
            let count = take_count(&mut cur)?;
            let mut keys = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                keys.push(take_bytes(&mut cur)?);
            }
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::GetMany {
                namespace,
                keys,
                snapshot_id,
            }
        }
        OP_PUT => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let value = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            Request::Put {
                namespace,
                key,
                value,
                ack,
                expected,
            }
        }
        OP_REMOVE => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            Request::Remove {
                namespace,
                key,
                ack,
                expected,
            }
        }
        OP_COMMIT_TX => {
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let guard_count = take_count(&mut cur)?;
            let mut guards = Vec::with_capacity(bounded_vec_cap(guard_count, cur.len()));
            for _ in 0..guard_count {
                let namespace = take_string(&mut cur)?;
                let key = take_bytes(&mut cur)?;
                let expected = take_expected(&mut cur)?;
                guards.push(VersionGuard {
                    namespace,
                    key,
                    expected,
                });
            }
            let mut_count = take_count(&mut cur)?;
            let mut mutations = Vec::with_capacity(bounded_vec_cap(mut_count, cur.len()));
            for _ in 0..mut_count {
                let namespace = take_string(&mut cur)?;
                let key = take_bytes(&mut cur)?;
                let op_tag = take_u8(&mut cur)?;
                let op = match op_tag {
                    1 => MutationOp::Put {
                        value: take_bytes(&mut cur)?,
                    },
                    2 => MutationOp::Remove,
                    other => {
                        return Err(MembusError::Protocol(format!(
                            "unknown mutation op tag: {other}"
                        )));
                    }
                };
                mutations.push(WriteMutation { namespace, key, op });
            }
            Request::CommitTx {
                ack,
                guards,
                mutations,
            }
        }
        OP_OPEN_SNAPSHOT => Request::OpenSnapshot,
        OP_CLOSE_SNAPSHOT => Request::CloseSnapshot {
            snapshot_id: take_u64(&mut cur)?,
        },
        OP_SCAN_PREFIX => {
            let namespace = take_string(&mut cur)?;
            let prefix = take_bytes(&mut cur)?;
            let options = take_scan_options(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::ScanPrefix {
                namespace,
                prefix,
                options,
                snapshot_id,
            }
        }
        OP_SCAN_RANGE => {
            let namespace = take_string(&mut cur)?;
            let start_key = take_bytes(&mut cur)?;
            let end_key = take_bytes(&mut cur)?;
            let options = take_scan_options(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::ScanRange {
                namespace,
                start_key,
                end_key,
                options,
                snapshot_id,
            }
        }
        OP_SUBSCRIBE_EACH => {
            let namespace = take_string(&mut cur)?;
            let key_prefix = take_bytes(&mut cur)?;
            let epoch = take_u64(&mut cur)?;
            let since_seq = take_u64(&mut cur)?;
            Request::SubscribeEachChange {
                namespace,
                key_prefix,
                epoch,
                since_seq,
            }
        }
        OP_SUBSCRIBE_LIVE => {
            let namespace = take_string(&mut cur)?;
            let key_prefix = take_bytes(&mut cur)?;
            let max_pending = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_u32(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid optional u32 tag: {other}"
                    )));
                }
            };
            Request::SubscribeLive {
                namespace,
                key_prefix,
                max_pending,
            }
        }
        OP_CHECKPOINT => Request::Checkpoint {
            dest_dir: take_string(&mut cur)?,
        },
        OP_HEALTH => Request::Health,
        OP_PUT_BLOB => {
            let digest = take_bytes(&mut cur)?;
            let data = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            Request::PutBlob { digest, data, ack }
        }
        OP_GET_BLOB => Request::GetBlob {
            digest: take_bytes(&mut cur)?,
        },
        OP_AUTHENTICATE => Request::Authenticate {
            token: take_string(&mut cur)?,
        },
        OP_PUT_INDEXED => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let value = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            let (indexes, search_op): (Option<IndexMutations>, Option<SearchLogOp>) =
                take_json(&mut cur)?;
            Request::PutIndexed {
                namespace,
                key,
                value,
                ack,
                expected,
                indexes,
                search_op,
            }
        }
        OP_REMOVE_INDEXED => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            let search_op: Option<SearchLogOp> = take_json(&mut cur)?;
            Request::RemoveIndexed {
                namespace,
                key,
                ack,
                expected,
                search_op,
            }
        }
        OP_COMMIT_INDEXED_TX => {
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let (guards, mutations): (Vec<VersionGuard>, Vec<IndexedMutation>) =
                take_json(&mut cur)?;
            if guards.len() > MAX_BATCH_ITEMS || mutations.len() > MAX_BATCH_ITEMS {
                return Err(MembusError::Protocol(format!(
                    "commit_indexed_tx batch size exceeds maximum {MAX_BATCH_ITEMS}"
                )));
            }
            Request::CommitIndexedTx {
                ack,
                guards,
                mutations,
            }
        }
        OP_QUERY_ORDERED_INDEX => {
            let namespace = take_string(&mut cur)?;
            let index_id = take_string(&mut cur)?;
            let wire_opts: WireOrderedQueryOptions = take_json(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::QueryOrderedIndex {
                namespace,
                index_id,
                options: wire_opts.into_options(),
                snapshot_id,
            }
        }
        OP_QUERY_NEIGHBORS => {
            let namespace = take_string(&mut cur)?;
            let index_id = take_string(&mut cur)?;
            let node_id = take_bytes(&mut cur)?;
            let wire_opts: WireNeighborQueryOptions = take_json(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::QueryNeighbors {
                namespace,
                index_id,
                node_id,
                options: wire_opts.into_options(),
                snapshot_id,
            }
        }
        OP_BFS_TRAVERSE => {
            let namespace = take_string(&mut cur)?;
            let index_id = take_string(&mut cur)?;
            let seed_node = take_bytes(&mut cur)?;
            let wire_opts: WireBfsOptions = take_json(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::BfsTraverse {
                namespace,
                index_id,
                seed_node,
                options: wire_opts.into_options(),
                snapshot_id,
            }
        }
        OP_PUT_CAS_BLOB => {
            let namespace = take_string(&mut cur)?;
            let data = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            Request::PutCasBlob {
                namespace,
                data,
                ack,
            }
        }
        OP_GET_CAS_BLOB => {
            let namespace = take_string(&mut cur)?;
            let sha256 = take_sha256(&mut cur)?;
            Request::GetCasBlob { namespace, sha256 }
        }
        OP_HAS_CAS_BLOB => {
            let namespace = take_string(&mut cur)?;
            let sha256 = take_sha256(&mut cur)?;
            Request::HasCasBlob { namespace, sha256 }
        }
        OP_ACQUIRE_LEASE => Request::AcquireLease {
            namespace: take_string(&mut cur)?,
            resource: take_string(&mut cur)?,
            holder: take_string(&mut cur)?,
            ttl_ms: take_u64(&mut cur)?,
        },
        OP_RENEW_LEASE => Request::RenewLease {
            namespace: take_string(&mut cur)?,
            resource: take_string(&mut cur)?,
            holder: take_string(&mut cur)?,
            generation: take_u64(&mut cur)?,
            token: take_string(&mut cur)?,
            ttl_ms: take_u64(&mut cur)?,
        },
        OP_RELEASE_LEASE => Request::ReleaseLease {
            namespace: take_string(&mut cur)?,
            resource: take_string(&mut cur)?,
            holder: take_string(&mut cur)?,
            generation: take_u64(&mut cur)?,
            token: take_string(&mut cur)?,
        },
        OP_INSPECT_LEASE => Request::InspectLease {
            namespace: take_string(&mut cur)?,
            resource: take_string(&mut cur)?,
        },
        OP_VALIDATE_LEASE => Request::ValidateLease {
            namespace: take_string(&mut cur)?,
            resource: take_string(&mut cur)?,
            holder: take_string(&mut cur)?,
            generation: take_u64(&mut cur)?,
            token: take_string(&mut cur)?,
        },
        OP_REGISTER_COLLECTION => {
            let (expected_version, descriptor): (Option<u32>, CollectionDescriptor) =
                take_json(&mut cur)?;
            Request::RegisterCollection {
                expected_version,
                descriptor,
            }
        }
        OP_GET_COLLECTION => Request::GetCollection {
            namespace: take_string(&mut cur)?,
        },
        OP_LIST_COLLECTIONS => Request::ListCollections,
        OP_GET_COLLECTION_VERSION => Request::GetCollectionVersion {
            namespace: take_string(&mut cur)?,
            version: take_u32(&mut cur)?,
        },
        OP_BEGIN_MIGRATION => Request::BeginMigration {
            namespace: take_string(&mut cur)?,
            expected_version: take_u32(&mut cur)?,
            target_version: take_u32(&mut cur)?,
            target_sha256: take_sha256(&mut cur)?,
            target_schema_bytes: take_bytes(&mut cur)?,
        },
        OP_COMMIT_MIGRATION => Request::CommitMigration {
            namespace: take_string(&mut cur)?,
            target_version: take_u32(&mut cur)?,
            verified_through: take_u64(&mut cur)?,
        },
        OP_ABORT_MIGRATION => Request::AbortMigration {
            namespace: take_string(&mut cur)?,
            target_version: take_u32(&mut cur)?,
        },
        OP_INDEX_DOCUMENT => {
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let doc: SearchDocument = take_json(&mut cur)?;
            Request::IndexDocument { doc, ack }
        }
        OP_DELETE_SEARCH_DOCUMENT => Request::DeleteSearchDocument {
            namespace: take_string(&mut cur)?,
            doc_id: take_string(&mut cur)?,
            ack: Ack::from_u8(take_u8(&mut cur)?)?,
        },
        OP_REVOKE_SEARCH_PREFIX => Request::RevokeSearchPrefix {
            namespace: take_string(&mut cur)?,
            prefix: take_string(&mut cur)?,
            ack: Ack::from_u8(take_u8(&mut cur)?)?,
        },
        OP_SEARCH => Request::Search(take_json(&mut cur)?),
        OP_HEALTH_V2 => Request::HealthV2,
        OP_ADVANCE_MIGRATION => Request::AdvanceMigrationVerification {
            namespace: take_string(&mut cur)?,
            target_version: take_u32(&mut cur)?,
            verified_through: take_u64(&mut cur)?,
        },
        OP_ENTER_REVERSE_SHADOW => Request::EnterReverseShadow {
            namespace: take_string(&mut cur)?,
            target_version: take_u32(&mut cur)?,
            verified_through: take_u64(&mut cur)?,
        },
        OP_ROLLBACK_MIGRATION => Request::RollbackMigration {
            namespace: take_string(&mut cur)?,
            rollback_version: take_u32(&mut cur)?,
        },
        OP_CONTRACT_MIGRATION => Request::ContractMigration {
            namespace: take_string(&mut cur)?,
            expected_version: take_u32(&mut cur)?,
        },
        OP_NAMESPACE_DIGEST => Request::NamespaceDigest {
            namespace: take_string(&mut cur)?,
            key_prefix: take_bytes(&mut cur)?,
            mode: take_json(&mut cur)?,
        },
        OP_TRANSITION_SLOT_ROLE => Request::TransitionSlotRole {
            expected_role: SlotRole::from_u8(take_u8(&mut cur)?)?,
            expected_role_epoch: take_u64(&mut cur)?,
            new_role: SlotRole::from_u8(take_u8(&mut cur)?)?,
        },
        OP_APPLY_REPLICATED_DELTA => {
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let (delta, route_origin): (ReplicatedDelta, Option<String>) = take_json(&mut cur)?;
            Request::ApplyReplicatedDelta {
                delta,
                ack,
                route_origin,
            }
        }
        other => {
            return Err(MembusError::Protocol(format!(
                "unknown request opcode: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(req)
}

pub fn encode_response(resp: &Response) -> Bytes {
    let mut buf = BytesMut::with_capacity(128);
    match resp {
        Response::Get(rec) => {
            buf.put_u8(RESP_GET);
            put_opt_record(&mut buf, rec.as_ref());
        }
        Response::GetMany(recs) => {
            buf.put_u8(RESP_GET_MANY);
            buf.put_u32(recs.len() as u32);
            for rec in recs {
                put_opt_record(&mut buf, rec.as_ref());
            }
        }
        Response::Commit(commit) => {
            buf.put_u8(RESP_COMMIT);
            buf.put_u64(commit.seq);
            buf.put_u64(commit.epoch);
            buf.put_u32(commit.versions.len() as u32);
            for ver in &commit.versions {
                buf.put_u64(*ver);
            }
        }
        Response::SnapshotOpened {
            snapshot_id,
            snapshot_seq,
        } => {
            buf.put_u8(RESP_SNAPSHOT_OPENED);
            buf.put_u64(*snapshot_id);
            buf.put_u64(*snapshot_seq);
        }
        Response::Unit => {
            buf.put_u8(RESP_UNIT);
        }
        Response::Scan(page) => {
            buf.put_u8(RESP_SCAN);
            buf.put_u64(page.snapshot_seq);
            buf.put_u8(u8::from(page.truncated));
            match &page.next_cursor {
                Some(cursor) => {
                    buf.put_u8(1);
                    put_bytes(&mut buf, cursor);
                }
                None => buf.put_u8(0),
            }
            buf.put_u32(page.records.len() as u32);
            for rec in &page.records {
                put_record(&mut buf, rec);
            }
        }
        Response::Subscribed { epoch, start_seq } => {
            buf.put_u8(RESP_SUBSCRIBED);
            buf.put_u64(*epoch);
            buf.put_u64(*start_seq);
        }
        Response::Checkpoint { seq } => {
            buf.put_u8(RESP_CHECKPOINT);
            buf.put_u64(*seq);
        }
        Response::Health(h) => {
            buf.put_u8(RESP_HEALTH);
            buf.put_u64(h.epoch);
            buf.put_u64(h.latest_seq);
            buf.put_u64(h.log_horizon_seq);
            buf.put_u64(h.in_flight_ops as u64);
            buf.put_u64(h.in_flight_bytes as u64);
            buf.put_u64(h.active_connections as u64);
            buf.put_u64(h.active_subscriptions as u64);
        }
        Response::HealthV2(h) => {
            buf.put_u8(RESP_HEALTH_V2);
            put_json_infallible(&mut buf, h);
        }
        Response::Blob(data) => {
            buf.put_u8(RESP_BLOB);
            match data {
                Some(bytes) => {
                    buf.put_u8(1);
                    put_bytes(&mut buf, bytes);
                }
                None => buf.put_u8(0),
            }
        }
        Response::Authenticated { principal_id } => {
            buf.put_u8(RESP_AUTHENTICATED);
            put_str(&mut buf, principal_id);
        }
        Response::OrderedIndex(page) => {
            buf.put_u8(RESP_ORDERED_INDEX);
            put_json_infallible(&mut buf, page);
        }
        Response::Neighbors(page) => {
            buf.put_u8(RESP_NEIGHBORS);
            put_json_infallible(&mut buf, page);
        }
        Response::Bfs(res) => {
            buf.put_u8(RESP_BFS);
            put_json_infallible(&mut buf, res);
        }
        Response::CasBlobDigest(digest) => {
            buf.put_u8(RESP_CAS_BLOB_DIGEST);
            buf.put_slice(digest);
        }
        Response::Bool(v) => {
            buf.put_u8(RESP_BOOL);
            buf.put_u8(u8::from(*v));
        }
        Response::Lease(grant) => {
            buf.put_u8(RESP_LEASE);
            put_json_infallible(&mut buf, grant);
        }
        Response::OptLease(grant) => {
            buf.put_u8(RESP_OPT_LEASE);
            put_json_infallible(&mut buf, grant);
        }
        Response::Collection(desc) => {
            buf.put_u8(RESP_COLLECTION);
            put_json_infallible(&mut buf, desc);
        }
        Response::OptCollection(desc) => {
            buf.put_u8(RESP_OPT_COLLECTION);
            put_json_infallible(&mut buf, desc);
        }
        Response::Collections(list) => {
            buf.put_u8(RESP_COLLECTIONS);
            put_json_infallible(&mut buf, list);
        }
        Response::Search(page) => {
            buf.put_u8(RESP_SEARCH);
            let mut clean = page.clone();
            for hit in &mut clean.hits {
                hit.score = sanitize_score(hit.score);
            }
            put_json_infallible(&mut buf, &clean);
        }
        Response::NamespaceDigest(digest) => {
            buf.put_u8(RESP_NAMESPACE_DIGEST);
            put_json_infallible(&mut buf, digest);
        }
        Response::SlotRole(state) => {
            buf.put_u8(RESP_SLOT_ROLE);
            put_json_infallible(&mut buf, state);
        }
        Response::ReplicatedApply(res) => {
            buf.put_u8(RESP_REPLICATED_APPLY);
            put_json_infallible(&mut buf, res);
        }
        Response::Error(err) => {
            buf.put_u8(RESP_ERROR);
            put_error(&mut buf, err);
        }
    }
    buf.freeze()
}

pub fn decode_response(raw: &[u8]) -> Result<Response, MembusError> {
    let mut cur = raw;
    let tag = take_u8(&mut cur)?;
    let resp = match tag {
        RESP_GET => Response::Get(take_opt_record(&mut cur)?),
        RESP_GET_MANY => {
            let count = take_count(&mut cur)?;
            let mut recs = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                recs.push(take_opt_record(&mut cur)?);
            }
            Response::GetMany(recs)
        }
        RESP_COMMIT => {
            let seq = take_u64(&mut cur)?;
            let epoch = take_u64(&mut cur)?;
            let count = take_count(&mut cur)?;
            let mut versions = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                versions.push(take_u64(&mut cur)?);
            }
            Response::Commit(CommitResult {
                seq,
                epoch,
                versions,
            })
        }
        RESP_SNAPSHOT_OPENED => Response::SnapshotOpened {
            snapshot_id: take_u64(&mut cur)?,
            snapshot_seq: take_u64(&mut cur)?,
        },
        RESP_UNIT => Response::Unit,
        RESP_SCAN => {
            let snapshot_seq = take_u64(&mut cur)?;
            let truncated = take_u8(&mut cur)? != 0;
            let next_cursor = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_bytes(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid cursor presence tag: {other}"
                    )));
                }
            };
            let count = take_count(&mut cur)?;
            let mut records = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                records.push(take_record(&mut cur)?);
            }
            Response::Scan(ScanPage {
                records,
                next_cursor,
                truncated,
                snapshot_seq,
            })
        }
        RESP_SUBSCRIBED => Response::Subscribed {
            epoch: take_u64(&mut cur)?,
            start_seq: take_u64(&mut cur)?,
        },
        RESP_CHECKPOINT => Response::Checkpoint {
            seq: take_u64(&mut cur)?,
        },
        RESP_HEALTH => Response::Health(HealthStatus {
            epoch: take_u64(&mut cur)?,
            latest_seq: take_u64(&mut cur)?,
            log_horizon_seq: take_u64(&mut cur)?,
            in_flight_ops: take_u64(&mut cur)? as usize,
            in_flight_bytes: take_u64(&mut cur)? as usize,
            active_connections: take_u64(&mut cur)? as usize,
            active_subscriptions: take_u64(&mut cur)? as usize,
            ..HealthStatus::default()
        }),
        RESP_HEALTH_V2 => Response::HealthV2(take_json(&mut cur)?),
        RESP_BLOB => {
            let blob = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_bytes(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid blob presence tag: {other}"
                    )));
                }
            };
            Response::Blob(blob)
        }
        RESP_AUTHENTICATED => Response::Authenticated {
            principal_id: take_string(&mut cur)?,
        },
        RESP_ORDERED_INDEX => Response::OrderedIndex(take_json(&mut cur)?),
        RESP_NEIGHBORS => Response::Neighbors(take_json(&mut cur)?),
        RESP_BFS => Response::Bfs(take_json(&mut cur)?),
        RESP_CAS_BLOB_DIGEST => Response::CasBlobDigest(take_sha256(&mut cur)?),
        RESP_BOOL => Response::Bool(take_u8(&mut cur)? != 0),
        RESP_LEASE => Response::Lease(take_json(&mut cur)?),
        RESP_OPT_LEASE => Response::OptLease(take_json(&mut cur)?),
        RESP_COLLECTION => Response::Collection(take_json(&mut cur)?),
        RESP_OPT_COLLECTION => Response::OptCollection(take_json(&mut cur)?),
        RESP_COLLECTIONS => Response::Collections(take_json(&mut cur)?),
        RESP_SEARCH => Response::Search(take_json(&mut cur)?),
        RESP_NAMESPACE_DIGEST => Response::NamespaceDigest(take_json(&mut cur)?),
        RESP_SLOT_ROLE => Response::SlotRole(take_json(&mut cur)?),
        RESP_REPLICATED_APPLY => Response::ReplicatedApply(take_json(&mut cur)?),
        RESP_ERROR => Response::Error(take_error(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "unknown response tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(resp)
}

pub fn encode_signal_event(ev: &SignalEvent) -> Bytes {
    let mut buf = BytesMut::with_capacity(64 + ev.namespace.len() + ev.key.len());
    buf.put_u64(ev.seq);
    buf.put_u64(ev.epoch);
    buf.put_u64(ev.version);
    buf.put_u8(ev.kind as u8);
    put_str(&mut buf, &ev.namespace);
    put_bytes(&mut buf, &ev.key);
    match &ev.value_hint {
        Some(hint) => {
            buf.put_u8(1);
            put_bytes(&mut buf, hint);
        }
        None => buf.put_u8(0),
    }
    buf.freeze()
}

pub fn decode_signal_event(raw: &[u8]) -> Result<SignalEvent, MembusError> {
    let mut cur = raw;
    let seq = take_u64(&mut cur)?;
    let epoch = take_u64(&mut cur)?;
    let version = take_u64(&mut cur)?;
    let kind = SignalKind::from_u8(take_u8(&mut cur)?)?;
    let namespace = take_string(&mut cur)?;
    let key = take_bytes(&mut cur)?;
    let value_hint = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(take_bytes(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid value_hint tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(SignalEvent {
        seq,
        epoch,
        namespace,
        key,
        version,
        kind,
        value_hint,
    })
}

pub fn encode_stream_end(err: Option<&MembusError>) -> Bytes {
    let mut buf = BytesMut::with_capacity(32);
    match err {
        None => buf.put_u8(0),
        Some(e) => {
            buf.put_u8(1);
            put_error(&mut buf, e);
        }
    }
    buf.freeze()
}

pub fn decode_stream_end(raw: &[u8]) -> Result<Option<MembusError>, MembusError> {
    let mut cur = raw;
    let out = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(take_error(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid stream_end tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(out)
}

fn put_str(buf: &mut BytesMut, s: &str) {
    put_bytes(buf, s.as_bytes());
}

fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
}

fn put_opt_u64(buf: &mut BytesMut, val: Option<u64>) {
    match val {
        Some(v) => {
            buf.put_u8(1);
            buf.put_u64(v);
        }
        None => buf.put_u8(0),
    }
}

fn put_expected(buf: &mut BytesMut, expected: ExpectedVersion) {
    match expected {
        ExpectedVersion::Any => buf.put_u8(0),
        ExpectedVersion::Absent => buf.put_u8(1),
        ExpectedVersion::Version(v) => {
            buf.put_u8(2);
            buf.put_u64(v);
        }
    }
}

fn put_scan_options(buf: &mut BytesMut, opts: &ScanOptions) -> Result<(), MembusError> {
    let limit = u32::try_from(opts.limit).map_err(|_| {
        MembusError::InvalidArgument(format!(
            "scan limit {} exceeds u32 maximum {}",
            opts.limit,
            u32::MAX
        ))
    })?;
    match &opts.after_key {
        Some(k) => {
            buf.put_u8(1);
            put_bytes(buf, k);
        }
        None => buf.put_u8(0),
    }
    buf.put_u32(limit);
    buf.put_u64(opts.max_bytes as u64);
    Ok(())
}

fn put_record(buf: &mut BytesMut, rec: &StoredRecord) {
    put_bytes(buf, &rec.key);
    put_bytes(buf, &rec.value);
    buf.put_u64(rec.meta.version);
    buf.put_u8(rec.meta.flags);
    buf.put_u64(rec.meta.ts_ms);
    buf.put_u32(rec.meta.crc32);
}

fn put_opt_record(buf: &mut BytesMut, rec: Option<&StoredRecord>) {
    match rec {
        Some(r) => {
            buf.put_u8(1);
            put_record(buf, r);
        }
        None => buf.put_u8(0),
    }
}

fn put_error(buf: &mut BytesMut, err: &MembusError) {
    match err {
        MembusError::InvalidArgument(msg) => {
            buf.put_u8(ERR_INVALID_ARG);
            put_str(buf, msg);
        }
        MembusError::Unauthorized(msg) => {
            buf.put_u8(ERR_UNAUTHORIZED);
            put_str(buf, msg);
        }
        MembusError::Conflict {
            namespace,
            key,
            expected,
            actual,
        } => {
            buf.put_u8(ERR_CONFLICT);
            put_str(buf, namespace);
            put_bytes(buf, key);
            put_expected(buf, *expected);
            put_opt_u64(buf, *actual);
        }
        MembusError::LeaseHeld {
            resource,
            holder,
            generation,
            expires_at_ms,
        } => {
            buf.put_u8(ERR_LEASE_HELD);
            put_str(buf, resource);
            put_str(buf, holder);
            buf.put_u64(*generation);
            buf.put_u64(*expires_at_ms);
        }
        MembusError::TxConflict(msg) => {
            buf.put_u8(ERR_TX_CONFLICT);
            put_str(buf, msg);
        }
        MembusError::RoleFenced {
            role,
            role_epoch,
            reason,
        } => {
            buf.put_u8(ERR_ROLE_FENCED);
            buf.put_u8(role.as_u8());
            buf.put_u64(*role_epoch);
            put_str(buf, reason);
        }
        MembusError::IdempotencyConflict(msg) => {
            buf.put_u8(ERR_IDEMPOTENCY_CONFLICT);
            put_str(buf, msg);
        }
        MembusError::Busy {
            retry_after_ms,
            reason,
        } => {
            buf.put_u8(ERR_BUSY);
            buf.put_u32(*retry_after_ms);
            put_str(buf, reason);
        }
        MembusError::Gap {
            earliest_available_seq,
            current_epoch,
        } => {
            buf.put_u8(ERR_GAP);
            buf.put_u64(*earliest_available_seq);
            buf.put_u64(*current_epoch);
        }
        MembusError::NotFound(msg) => {
            buf.put_u8(ERR_NOT_FOUND);
            put_str(buf, msg);
        }
        MembusError::Corruption(msg) => {
            buf.put_u8(ERR_CORRUPTION);
            put_str(buf, msg);
        }
        MembusError::Storage(msg) => {
            buf.put_u8(ERR_STORAGE);
            put_str(buf, msg);
        }
        MembusError::Protocol(msg) => {
            buf.put_u8(ERR_PROTOCOL);
            put_str(buf, msg);
        }
        MembusError::FrameTooLarge { len, max_len } => {
            buf.put_u8(ERR_FRAME_TOO_LARGE);
            buf.put_u64(*len as u64);
            buf.put_u64(*max_len as u64);
        }
        MembusError::Io(msg) => {
            buf.put_u8(ERR_IO);
            put_str(buf, msg);
        }
        MembusError::Closed => {
            buf.put_u8(ERR_CLOSED);
        }
    }
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, MembusError> {
    if cur.is_empty() {
        return Err(MembusError::Protocol("unexpected eof decoding u8".to_owned()));
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, MembusError> {
    let s = take_slice(cur, 4)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_u64(cur: &mut &[u8]) -> Result<u64, MembusError> {
    let s = take_slice(cur, 8)?;
    Ok(u64::from_be_bytes([
        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]))
}

fn take_slice<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], MembusError> {
    if cur.len() < len {
        return Err(MembusError::Protocol(format!(
            "unexpected eof decoding {len} bytes (remaining {})",
            cur.len()
        )));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

fn take_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, MembusError> {
    let len = take_u32(cur)? as usize;
    Ok(take_slice(cur, len)?.to_vec())
}

fn take_string(cur: &mut &[u8]) -> Result<String, MembusError> {
    let bytes = take_bytes(cur)?;
    String::from_utf8(bytes)
        .map_err(|err| MembusError::Protocol(format!("invalid utf8 string: {err}")))
}

fn bounded_vec_cap(count: usize, remaining_bytes: usize) -> usize {
    count.min(remaining_bytes).min(256)
}

fn take_count(cur: &mut &[u8]) -> Result<usize, MembusError> {
    let count = take_u32(cur)? as usize;
    if count > MAX_BATCH_ITEMS {
        return Err(MembusError::Protocol(format!(
            "batch count {count} exceeds limit {MAX_BATCH_ITEMS}"
        )));
    }
    Ok(count)
}

fn take_opt_u64(cur: &mut &[u8]) -> Result<Option<u64>, MembusError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(take_u64(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid optional u64 tag: {other}"
        ))),
    }
}

fn take_expected(cur: &mut &[u8]) -> Result<ExpectedVersion, MembusError> {
    match take_u8(cur)? {
        0 => Ok(ExpectedVersion::Any),
        1 => Ok(ExpectedVersion::Absent),
        2 => Ok(ExpectedVersion::Version(take_u64(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid expected_version tag: {other}"
        ))),
    }
}

fn take_scan_options(cur: &mut &[u8]) -> Result<ScanOptions, MembusError> {
    let after_key = match take_u8(cur)? {
        0 => None,
        1 => Some(take_bytes(cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid after_key tag: {other}"
            )));
        }
    };
    let limit = take_u32(cur)? as usize;
    let max_bytes = take_u64(cur)? as usize;
    Ok(ScanOptions {
        after_key,
        limit,
        max_bytes,
    })
}

fn take_record(cur: &mut &[u8]) -> Result<StoredRecord, MembusError> {
    let key = take_bytes(cur)?;
    let value = take_bytes(cur)?;
    let version = take_u64(cur)?;
    let flags = take_u8(cur)?;
    let ts_ms = take_u64(cur)?;
    let crc32 = take_u32(cur)?;
    Ok(StoredRecord {
        key,
        value,
        meta: RecordMeta {
            version,
            flags,
            ts_ms,
            crc32,
        },
    })
}

fn take_opt_record(cur: &mut &[u8]) -> Result<Option<StoredRecord>, MembusError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(take_record(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid optional record tag: {other}"
        ))),
    }
}

fn take_error(cur: &mut &[u8]) -> Result<MembusError, MembusError> {
    match take_u8(cur)? {
        ERR_INVALID_ARG => Ok(MembusError::InvalidArgument(take_string(cur)?)),
        ERR_UNAUTHORIZED => Ok(MembusError::Unauthorized(take_string(cur)?)),
        ERR_CONFLICT => Ok(MembusError::Conflict {
            namespace: take_string(cur)?,
            key: take_bytes(cur)?,
            expected: take_expected(cur)?,
            actual: take_opt_u64(cur)?,
        }),
        ERR_LEASE_HELD => Ok(MembusError::LeaseHeld {
            resource: take_string(cur)?,
            holder: take_string(cur)?,
            generation: take_u64(cur)?,
            expires_at_ms: take_u64(cur)?,
        }),
        ERR_TX_CONFLICT => Ok(MembusError::TxConflict(take_string(cur)?)),
        ERR_ROLE_FENCED => Ok(MembusError::RoleFenced {
            role: SlotRole::from_u8(take_u8(cur)?)?,
            role_epoch: take_u64(cur)?,
            reason: take_string(cur)?,
        }),
        ERR_IDEMPOTENCY_CONFLICT => Ok(MembusError::IdempotencyConflict(take_string(cur)?)),
        ERR_BUSY => Ok(MembusError::Busy {
            retry_after_ms: take_u32(cur)?,
            reason: take_string(cur)?,
        }),
        ERR_GAP => Ok(MembusError::Gap {
            earliest_available_seq: take_u64(cur)?,
            current_epoch: take_u64(cur)?,
        }),
        ERR_NOT_FOUND => Ok(MembusError::NotFound(take_string(cur)?)),
        ERR_CORRUPTION => Ok(MembusError::Corruption(take_string(cur)?)),
        ERR_STORAGE => Ok(MembusError::Storage(take_string(cur)?)),
        ERR_PROTOCOL => Ok(MembusError::Protocol(take_string(cur)?)),
        ERR_FRAME_TOO_LARGE => Ok(MembusError::FrameTooLarge {
            len: take_u64(cur)? as usize,
            max_len: take_u64(cur)? as usize,
        }),
        ERR_IO => Ok(MembusError::Io(take_string(cur)?)),
        ERR_CLOSED => Ok(MembusError::Closed),
        other => Err(MembusError::Protocol(format!(
            "unknown wire error tag: {other}"
        ))),
    }
}

fn ensure_empty(cur: &[u8]) -> Result<(), MembusError> {
    if cur.is_empty() {
        Ok(())
    } else {
        Err(MembusError::Protocol(format!(
            "trailing {} bytes in wire message",
            cur.len()
        )))
    }
}

fn put_json<T: Serialize>(buf: &mut BytesMut, val: &T) -> Result<(), MembusError> {
    let bytes = serde_json::to_vec(val)
        .map_err(|err| MembusError::Protocol(format!("wire JSON encode failed: {err}")))?;
    put_bytes(buf, &bytes);
    Ok(())
}

fn put_json_infallible<T: Serialize>(buf: &mut BytesMut, val: &T) {
    let bytes = serde_json::to_vec(val).unwrap_or_default();
    put_bytes(buf, &bytes);
}

fn take_json<T: for<'de> Deserialize<'de>>(cur: &mut &[u8]) -> Result<T, MembusError> {
    let len = take_u32(cur)? as usize;
    let slice = take_slice(cur, len)?;
    serde_json::from_slice(slice)
        .map_err(|err| MembusError::Protocol(format!("wire JSON decode failed: {err}")))
}

fn take_sha256(cur: &mut &[u8]) -> Result<[u8; 32], MembusError> {
    let slice = take_slice(cur, 32)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(slice);
    Ok(out)
}

#[derive(Serialize, Deserialize)]
enum WireOrderedFilter {
    All,
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Serialize, Deserialize)]
struct WireOrderedQueryOptions {
    filter: WireOrderedFilter,
    after_cursor: Option<Vec<u8>>,
    limit: u32,
    max_bytes: u64,
}

impl WireOrderedQueryOptions {
    fn from_options(opts: &OrderedQueryOptions) -> Result<Self, MembusError> {
        let limit = u32::try_from(opts.limit).map_err(|_| {
            MembusError::InvalidArgument("ordered query limit exceeds u32".to_owned())
        })?;
        let filter = match &opts.filter {
            crate::index::OrderedKeyFilter::All => WireOrderedFilter::All,
            crate::index::OrderedKeyFilter::Prefix(p) => WireOrderedFilter::Prefix(p.clone()),
            crate::index::OrderedKeyFilter::Range { start, end } => WireOrderedFilter::Range {
                start: start.clone(),
                end: end.clone(),
            },
        };
        Ok(Self {
            filter,
            after_cursor: opts.after_cursor.clone(),
            limit,
            max_bytes: opts.max_bytes as u64,
        })
    }

    fn into_options(self) -> OrderedQueryOptions {
        let filter = match self.filter {
            WireOrderedFilter::All => crate::index::OrderedKeyFilter::All,
            WireOrderedFilter::Prefix(p) => crate::index::OrderedKeyFilter::Prefix(p),
            WireOrderedFilter::Range { start, end } => {
                crate::index::OrderedKeyFilter::Range { start, end }
            }
        };
        OrderedQueryOptions {
            filter,
            after_cursor: self.after_cursor,
            limit: self.limit as usize,
            max_bytes: self.max_bytes as usize,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WireNeighborQueryOptions {
    direction: crate::types::GraphDirection,
    rel_filter: Option<Vec<u8>>,
    after_cursor: Option<Vec<u8>>,
    limit: u32,
}

impl WireNeighborQueryOptions {
    fn from_options(opts: &NeighborQueryOptions) -> Result<Self, MembusError> {
        let limit = u32::try_from(opts.limit).map_err(|_| {
            MembusError::InvalidArgument("neighbor query limit exceeds u32".to_owned())
        })?;
        Ok(Self {
            direction: opts.direction,
            rel_filter: opts.rel_filter.clone(),
            after_cursor: opts.after_cursor.clone(),
            limit,
        })
    }

    fn into_options(self) -> NeighborQueryOptions {
        NeighborQueryOptions {
            direction: self.direction,
            rel_filter: self.rel_filter,
            after_cursor: self.after_cursor,
            limit: self.limit as usize,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WireBfsOptions {
    direction: crate::types::GraphDirection,
    rel_filter: Option<Vec<u8>>,
    max_hops: u8,
    max_nodes: u32,
}

impl WireBfsOptions {
    fn from_options(opts: &BfsOptions) -> Result<Self, MembusError> {
        let max_nodes = u32::try_from(opts.max_nodes).map_err(|_| {
            MembusError::InvalidArgument("bfs max_nodes exceeds u32".to_owned())
        })?;
        Ok(Self {
            direction: opts.direction,
            rel_filter: opts.rel_filter.clone(),
            max_hops: opts.max_hops,
            max_nodes,
        })
    }

    fn into_options(self) -> BfsOptions {
        BfsOptions {
            direction: self.direction,
            rel_filter: self.rel_filter,
            max_hops: self.max_hops,
            max_nodes: self.max_nodes as usize,
        }
    }
}
