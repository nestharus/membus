use crate::types::{
    validate_key, GraphDirection, GraphEdgeEntry, IndexMutations, MembusError, OrderedIndexEntry,
};

/// Maximum permitted index identifier length in bytes.
pub const MAX_INDEX_ID_BYTES: usize = 128;

/// Maximum permitted BFS hop depth (hard cap per contract).
pub const MAX_BFS_HOPS: u8 = 8;

/// Maximum permitted BFS visited node count per traversal.
pub const MAX_BFS_NODES: usize = 100_000;

/// Maximum secondary index postings emitted by a single primary key mutation.
pub const MAX_POSTINGS_PER_KEY: usize = 4_096;

/// Direction tag stored in `idx_edge` keys for forward (`src -> dst`) adjacency.
pub const EDGE_DIR_FORWARD: u8 = b'F';

/// Direction tag stored in `idx_edge` keys for reverse (`dst <- src`) adjacency.
pub const EDGE_DIR_REVERSE: u8 = b'R';

/// Range or prefix filter for [`crate::StorageEngine::query_ordered_index`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderedKeyFilter {
    All,
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

/// Bounded options for ordered secondary index queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderedQueryOptions {
    pub filter: OrderedKeyFilter,
    pub after_cursor: Option<Vec<u8>>,
    pub limit: usize,
    pub max_bytes: usize,
}

impl Default for OrderedQueryOptions {
    fn default() -> Self {
        Self {
            filter: OrderedKeyFilter::All,
            after_cursor: None,
            limit: 256,
            max_bytes: 1024 * 1024,
        }
    }
}

/// Bounded options for single-hop graph neighbor queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NeighborQueryOptions {
    pub direction: GraphDirection,
    pub rel_filter: Option<Vec<u8>>,
    pub after_cursor: Option<Vec<u8>>,
    pub limit: usize,
}

impl Default for NeighborQueryOptions {
    fn default() -> Self {
        Self {
            direction: GraphDirection::Out,
            rel_filter: None,
            after_cursor: None,
            limit: 256,
        }
    }
}

/// Bounded options for multi-hop graph BFS traversals strictly inside `idx_edge`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BfsOptions {
    pub direction: GraphDirection,
    pub rel_filter: Option<Vec<u8>>,
    pub max_hops: u8,
    pub max_nodes: usize,
}

impl Default for BfsOptions {
    fn default() -> Self {
        Self {
            direction: GraphDirection::Out,
            rel_filter: None,
            max_hops: 2,
            max_nodes: 1_000,
        }
    }
}

/// Validate a secondary index identifier without mutating input.
pub fn validate_index_id(index_id: &str) -> Result<(), MembusError> {
    if index_id.is_empty() {
        return Err(MembusError::InvalidArgument(
            "index_id must not be empty".to_owned(),
        ));
    }
    if index_id.len() > MAX_INDEX_ID_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "index_id length {} exceeds maximum {MAX_INDEX_ID_BYTES}",
            index_id.len()
        )));
    }
    if index_id.as_bytes().contains(&0) {
        return Err(MembusError::InvalidArgument(
            "index_id must not contain NUL bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validate all client-emitted ordered & graph index mutations before entering storage.
pub fn validate_index_mutations(
    mutations: &IndexMutations,
    max_value_bytes: usize,
) -> Result<(), MembusError> {
    let total = mutations
        .ordered
        .len()
        .saturating_add(mutations.edges.len());
    if total > MAX_POSTINGS_PER_KEY {
        return Err(MembusError::InvalidArgument(format!(
            "index posting count {total} exceeds maximum {MAX_POSTINGS_PER_KEY}"
        )));
    }
    for entry in &mutations.ordered {
        validate_ordered_entry(entry, max_value_bytes)?;
    }
    for edge in &mutations.edges {
        validate_graph_edge(edge, max_value_bytes)?;
    }
    Ok(())
}

fn validate_ordered_entry(
    entry: &OrderedIndexEntry,
    max_value_bytes: usize,
) -> Result<(), MembusError> {
    validate_index_id(&entry.index_id)?;
    if entry.sort_key.len() > crate::types::MAX_KEY_BYTES {
        return Err(MembusError::InvalidArgument(
            "ordered index sort_key exceeds maximum key length".to_owned(),
        ));
    }
    if entry.summary.len() > max_value_bytes {
        return Err(MembusError::InvalidArgument(
            "ordered index summary exceeds max_value_bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_graph_edge(
    edge: &GraphEdgeEntry,
    max_value_bytes: usize,
) -> Result<(), MembusError> {
    validate_index_id(&edge.index_id)?;
    validate_key(&edge.src_id)?;
    validate_key(&edge.rel_label)?;
    validate_key(&edge.dst_id)?;
    if edge.meta.len() > max_value_bytes {
        return Err(MembusError::InvalidArgument(
            "graph edge meta exceeds max_value_bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validate bounded BFS options before entering storage.
pub fn validate_bfs_options(opts: &BfsOptions) -> Result<(), MembusError> {
    if opts.max_hops > MAX_BFS_HOPS {
        return Err(MembusError::InvalidArgument(format!(
            "bfs max_hops {} exceeds maximum {MAX_BFS_HOPS}",
            opts.max_hops
        )));
    }
    if opts.max_nodes == 0 || opts.max_nodes > MAX_BFS_NODES {
        return Err(MembusError::InvalidArgument(format!(
            "bfs max_nodes {} must be in 1..={MAX_BFS_NODES}",
            opts.max_nodes
        )));
    }
    Ok(())
}

/// Order-preserving byte encoding: escapes `0x00` as `[0x00, 0xFF]` and appends `[0x00, 0x00]`.
pub fn encode_order_preserving(bytes: &[u8], out: &mut Vec<u8>) {
    encode_order_preserving_prefix(bytes, out);
    out.push(0x00);
    out.push(0x00);
}

/// Order-preserving byte prefix encoding (no trailing terminator) for prefix seeks.
pub fn encode_order_preserving_prefix(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
}

/// Decode one order-preserving field terminated by `[0x00, 0x00]`, advancing `cur`.
pub fn decode_order_preserving(cur: &mut &[u8]) -> Result<Vec<u8>, MembusError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let slice = *cur;
    while i < slice.len() {
        let b = slice[i];
        if b == 0x00 {
            if i + 1 >= slice.len() {
                return Err(MembusError::Corruption(
                    "truncated escape sequence in index key".to_owned(),
                ));
            }
            match slice[i + 1] {
                0x00 => {
                    *cur = &slice[i + 2..];
                    return Ok(out);
                }
                0xFF => {
                    out.push(0x00);
                    i += 2;
                }
                other => {
                    return Err(MembusError::Corruption(format!(
                        "invalid escape byte 0x{other:02x} in index key"
                    )));
                }
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    Err(MembusError::Corruption(
        "unterminated order-preserving field in index key".to_owned(),
    ))
}

/// Encode `[u32_be ns_len][ns][u32_be idx_len][index_id]`.
pub fn encode_index_prefix(namespace: &str, index_id: &str) -> Vec<u8> {
    let ns_b = namespace.as_bytes();
    let idx_b = index_id.as_bytes();
    let mut out = Vec::with_capacity(8 + ns_b.len() + idx_b.len());
    out.extend_from_slice(&(ns_b.len() as u32).to_be_bytes());
    out.extend_from_slice(ns_b);
    out.extend_from_slice(&(idx_b.len() as u32).to_be_bytes());
    out.extend_from_slice(idx_b);
    out
}

/// Encode an `idx_ord` CF key: `[ns_idx_prefix][escaped(sort_key)][escaped(pk)]`.
pub fn encode_ordered_key(namespace: &str, index_id: &str, sort_key: &[u8], pk: &[u8]) -> Vec<u8> {
    let mut out = encode_index_prefix(namespace, index_id);
    encode_order_preserving(sort_key, &mut out);
    encode_order_preserving(pk, &mut out);
    out
}

/// Decode an `idx_ord` CF key suffix after `prefix_len` into `(sort_key, pk)`.
pub fn decode_ordered_suffix(suffix: &[u8]) -> Result<(Vec<u8>, Vec<u8>), MembusError> {
    let mut cur = suffix;
    let sort_key = decode_order_preserving(&mut cur)?;
    let pk = decode_order_preserving(&mut cur)?;
    if !cur.is_empty() {
        return Err(MembusError::Corruption(
            "trailing bytes in idx_ord key".to_owned(),
        ));
    }
    Ok((sort_key, pk))
}

/// Encode a forward (`b'F'`) or reverse (`b'R'`) `idx_edge` CF key owned exclusively by `pk`.
///
/// Layout: `[ns_idx_prefix][dir_tag][esc(first_node)][esc(rel_label)][esc(second_node)][esc(pk)]`.
#[allow(clippy::too_many_arguments)]
pub fn encode_edge_key(
    namespace: &str,
    index_id: &str,
    dir_tag: u8,
    first_node: &[u8],
    rel_label: &[u8],
    second_node: &[u8],
    pk: &[u8],
) -> Vec<u8> {
    let mut out = encode_index_prefix(namespace, index_id);
    out.push(dir_tag);
    encode_order_preserving(first_node, &mut out);
    encode_order_preserving(rel_label, &mut out);
    encode_order_preserving(second_node, &mut out);
    encode_order_preserving(pk, &mut out);
    out
}

/// Encode the seek prefix for neighbors of `node_id` in one direction (`b'F'` or `b'R'`).
pub fn encode_edge_seek_prefix(
    namespace: &str,
    index_id: &str,
    dir_tag: u8,
    node_id: &[u8],
    rel_filter: Option<&[u8]>,
) -> Vec<u8> {
    let mut out = encode_index_prefix(namespace, index_id);
    out.push(dir_tag);
    encode_order_preserving(node_id, &mut out);
    if let Some(rel) = rel_filter {
        encode_order_preserving(rel, &mut out);
    }
    out
}

/// Decoded `(node_a, rel_label, node_b, owner_pk)` tuple from an `idx_edge` key suffix.
pub type EdgeQuad = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

/// Decode an `idx_edge` key suffix starting right after `[ns_idx_prefix][dir_tag]`.
pub fn decode_edge_suffix(suffix: &[u8]) -> Result<EdgeQuad, MembusError> {
    let mut cur = suffix;
    let first = decode_order_preserving(&mut cur)?;
    let rel = decode_order_preserving(&mut cur)?;
    let second = decode_order_preserving(&mut cur)?;
    let owner_pk = decode_order_preserving(&mut cur)?;
    if !cur.is_empty() {
        return Err(MembusError::Corruption(
            "trailing bytes in idx_edge key".to_owned(),
        ));
    }
    Ok((first, rel, second, owner_pk))
}

/// Compact serialized reverse-posting record stored per `(ns, pk)` in `idx_rev` CF.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReversePostings {
    pub ord_keys: Vec<Vec<u8>>,
    pub edge_keys: Vec<Vec<u8>>,
}

impl ReversePostings {
    pub fn is_empty(&self) -> bool {
        self.ord_keys.is_empty() && self.edge_keys.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            8 + self.ord_keys.iter().map(|k| 4 + k.len()).sum::<usize>()
                + self.edge_keys.iter().map(|k| 4 + k.len()).sum::<usize>(),
        );
        out.extend_from_slice(&(self.ord_keys.len() as u32).to_be_bytes());
        for k in &self.ord_keys {
            out.extend_from_slice(&(k.len() as u32).to_be_bytes());
            out.extend_from_slice(k);
        }
        out.extend_from_slice(&(self.edge_keys.len() as u32).to_be_bytes());
        for k in &self.edge_keys {
            out.extend_from_slice(&(k.len() as u32).to_be_bytes());
            out.extend_from_slice(k);
        }
        out
    }

    pub fn decode(raw: &[u8]) -> Result<Self, MembusError> {
        let mut cur = raw;
        let ord_count = take_u32(&mut cur)? as usize;
        let mut ord_keys = Vec::with_capacity(ord_count.min(cur.len() / 4).min(256));
        for _ in 0..ord_count {
            let len = take_u32(&mut cur)? as usize;
            ord_keys.push(take_slice(&mut cur, len)?.to_vec());
        }
        let edge_count = take_u32(&mut cur)? as usize;
        let mut edge_keys = Vec::with_capacity(edge_count.min(cur.len() / 4).min(256));
        for _ in 0..edge_count {
            let len = take_u32(&mut cur)? as usize;
            edge_keys.push(take_slice(&mut cur, len)?.to_vec());
        }
        if !cur.is_empty() {
            return Err(MembusError::Corruption(
                "trailing bytes in idx_rev record".to_owned(),
            ));
        }
        Ok(Self {
            ord_keys,
            edge_keys,
        })
    }
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, MembusError> {
    let s = take_slice(cur, 4)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_slice<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], MembusError> {
    if cur.len() < len {
        return Err(MembusError::Corruption(
            "unexpected eof decoding idx_rev record".to_owned(),
        ));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}
