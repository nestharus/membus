use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use rocksdb::{Direction, IteratorMode, ReadOptions};

use crate::index::{
    decode_edge_suffix, decode_ordered_suffix, encode_edge_seek_prefix, encode_index_prefix,
    encode_order_preserving_prefix, validate_bfs_options, validate_index_id, BfsOptions,
    NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions, EDGE_DIR_FORWARD,
    EDGE_DIR_REVERSE,
};
use crate::storage::codec::{decode_kv_key, decode_record, encode_kv_key, next_key_after};
use crate::storage::constants::{CF_IDX_EDGE, CF_IDX_ORD, CF_KV};
use crate::storage::rocksdb_kv::helpers::{map_rocksdb_err, validate_scan_options};
use crate::storage::rocksdb_kv::state::RocksDbStorage;
use crate::storage::snapshot::OwnedSnapshot;
use crate::types::{
    validate_key, validate_namespace, BfsResult, GraphDirection, GraphEdgeRecord, MembusError,
    NeighborPage, OrderedIndexHit, OrderedIndexPage, ScanOptions, ScanPage, StoredRecord,
};

impl RocksDbStorage {
    pub(crate) fn get_impl(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        let kv_cf = self.cf(CF_KV)?;
        let enc_key = encode_kv_key(namespace, key);
        match self
            .db
            .get_pinned_cf(&kv_cf, &enc_key)
            .map_err(map_rocksdb_err)?
        {
            Some(raw) => Ok(Some(decode_record(key.to_vec(), raw.as_ref())?)),
            None => Ok(None),
        }
    }

    pub(crate) fn get_many_impl(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        validate_namespace(namespace)?;
        for key in keys {
            validate_key(key)?;
        }
        let kv_cf = self.cf(CF_KV)?;
        let encoded_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| encode_kv_key(namespace, k))
            .collect();
        let query_pairs: Vec<_> = encoded_keys.iter().map(|k| (&kv_cf, k.as_slice())).collect();
        let raw_results = self.db.multi_get_cf(query_pairs);
        decode_multi_get_results(keys, raw_results)
    }

    pub(crate) fn open_snapshot_impl(&self) -> Result<OwnedSnapshot, MembusError> {
        let permit = self.snapshot_gate.try_acquire()?;
        // Capture contiguous published sequence BEFORE pinning the RocksDB snapshot so
        // every `seq <= snapshot_seq` is guaranteed visible in the snapshot.
        let snapshot_seq = self.sequencer.published_seq();
        Ok(OwnedSnapshot::new(
            Arc::clone(&self.db),
            snapshot_seq,
            permit,
        ))
    }

    pub(crate) fn get_in_snapshot_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_key(key)?;
        let kv_cf = self.cf(CF_KV)?;
        let enc_key = encode_kv_key(namespace, key);
        match snapshot
            .inner()
            .get_pinned_cf(&kv_cf, &enc_key)
            .map_err(map_rocksdb_err)?
        {
            Some(raw) => Ok(Some(decode_record(key.to_vec(), raw.as_ref())?)),
            None => Ok(None),
        }
    }

    pub(crate) fn get_many_in_snapshot_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        for key in keys {
            validate_key(key)?;
        }
        let kv_cf = self.cf(CF_KV)?;
        let encoded_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| encode_kv_key(namespace, k))
            .collect();
        let query_pairs: Vec<_> = encoded_keys.iter().map(|k| (&kv_cf, k.as_slice())).collect();
        let raw_results = snapshot.inner().multi_get_cf(query_pairs);
        decode_multi_get_results(keys, raw_results)
    }

    fn execute_keyset_scan<F>(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        effective_start: &[u8],
        options: &ScanOptions,
        in_bounds: F,
    ) -> Result<ScanPage, MembusError>
    where
        F: Fn(&[u8]) -> bool,
    {
        let deadline = self.query_deadline();
        let seek_key = encode_kv_key(namespace, effective_start);
        let kv_cf = self.cf(CF_KV)?;
        let mut read_opts = ReadOptions::default();
        read_opts.set_prefix_same_as_start(true);
        read_opts.fill_cache(false);

        let iter = snapshot.inner().iterator_cf_opt(
            &kv_cf,
            read_opts,
            IteratorMode::From(&seek_key, Direction::Forward),
        );

        let mut records = Vec::new();
        let mut accum_bytes = 0usize;
        let mut truncated = false;

        for (step, item) in iter.enumerate() {
            if step & 63 == 0 {
                Self::check_query_deadline(deadline)?;
            }
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            let (rec_ns, rec_key) = decode_kv_key(&raw_k)?;
            if rec_ns != namespace || !in_bounds(rec_key) {
                break;
            }
            let item_bytes = rec_key.len().saturating_add(raw_v.len());
            if records.len() >= options.limit
                || (!records.is_empty()
                    && accum_bytes.saturating_add(item_bytes) > options.max_bytes)
            {
                truncated = true;
                break;
            }
            let record = decode_record(rec_key.to_vec(), &raw_v)?;
            accum_bytes = accum_bytes.saturating_add(item_bytes);
            records.push(record);
        }

        let next_cursor = if truncated {
            records.last().map(|r| r.key.clone())
        } else {
            None
        };

        Ok(ScanPage {
            records,
            next_cursor,
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    pub(crate) fn scan_prefix_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_scan_options(options, self.config.max_scan_limit)?;
        let effective_start = compute_scan_start(prefix, options.after_key.as_deref());
        self.execute_keyset_scan(snapshot, namespace, &effective_start, options, |key| {
            key.starts_with(prefix)
        })
    }

    pub(crate) fn scan_range_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_scan_options(options, self.config.max_scan_limit)?;

        if !end_key.is_empty() && end_key <= start_key {
            return Ok(ScanPage {
                records: Vec::new(),
                next_cursor: None,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }

        let effective_start = compute_scan_start(start_key, options.after_key.as_deref());
        if !end_key.is_empty() && effective_start.as_slice() >= end_key {
            return Ok(ScanPage {
                records: Vec::new(),
                next_cursor: None,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }

        self.execute_keyset_scan(snapshot, namespace, &effective_start, options, |key| {
            end_key.is_empty() || key < end_key
        })
    }

    pub(crate) fn query_ordered_index_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        options: &OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        if options.limit == 0 || options.max_bytes == 0 {
            return Err(MembusError::InvalidArgument(
                "ordered index limit and max_bytes must be > 0".to_owned(),
            ));
        }
        if options.limit > self.config.max_scan_limit {
            return Err(MembusError::InvalidArgument(format!(
                "ordered index limit {} exceeds max_scan_limit {}",
                options.limit, self.config.max_scan_limit
            )));
        }

        let deadline = self.query_deadline();
        let ns_idx_prefix = encode_index_prefix(namespace, index_id);
        let base_suffix: Vec<u8> = match &options.filter {
            OrderedKeyFilter::All => Vec::new(),
            OrderedKeyFilter::Prefix(p) => {
                let mut out = Vec::with_capacity(p.len() + 2);
                encode_order_preserving_prefix(p, &mut out);
                out
            }
            OrderedKeyFilter::Range { start, end } => {
                if !end.is_empty() && end <= start {
                    return Ok(OrderedIndexPage {
                        entries: Vec::new(),
                        next_cursor: None,
                        truncated: false,
                        snapshot_seq: snapshot.snapshot_seq(),
                    });
                }
                let mut out = Vec::with_capacity(start.len() + 2);
                encode_order_preserving_prefix(start, &mut out);
                out
            }
        };

        let effective_suffix = compute_scan_start(&base_suffix, options.after_cursor.as_deref());
        let mut seek_key = Vec::with_capacity(ns_idx_prefix.len() + effective_suffix.len());
        seek_key.extend_from_slice(&ns_idx_prefix);
        seek_key.extend_from_slice(&effective_suffix);

        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);

        let iter = snapshot.inner().iterator_cf_opt(
            &idx_ord_cf,
            read_opts,
            IteratorMode::From(&seek_key, Direction::Forward),
        );

        let mut entries = Vec::new();
        let mut last_suffix: Option<Vec<u8>> = None;
        let mut accum_bytes = 0usize;
        let mut truncated = false;

        for (step, item) in iter.enumerate() {
            if step & 63 == 0 {
                Self::check_query_deadline(deadline)?;
            }
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            if !raw_k.starts_with(&ns_idx_prefix) {
                break;
            }
            let suffix = &raw_k[ns_idx_prefix.len()..];
            let (sort_key, primary_key) = decode_ordered_suffix(suffix)?;
            let in_bounds = match &options.filter {
                OrderedKeyFilter::All => true,
                OrderedKeyFilter::Prefix(p) => sort_key.starts_with(p),
                OrderedKeyFilter::Range { start, end } => {
                    sort_key.as_slice() >= start.as_slice()
                        && (end.is_empty() || sort_key.as_slice() < end.as_slice())
                }
            };
            if !in_bounds {
                break;
            }
            let item_bytes = sort_key
                .len()
                .saturating_add(primary_key.len())
                .saturating_add(raw_v.len());
            if entries.len() >= options.limit
                || (!entries.is_empty()
                    && accum_bytes.saturating_add(item_bytes) > options.max_bytes)
            {
                truncated = true;
                break;
            }
            accum_bytes = accum_bytes.saturating_add(item_bytes);
            last_suffix = Some(suffix.to_vec());
            entries.push(OrderedIndexHit {
                sort_key,
                primary_key,
                summary: raw_v.to_vec(),
            });
        }

        Ok(OrderedIndexPage {
            entries,
            next_cursor: if truncated { last_suffix } else { None },
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    pub(crate) fn query_neighbors_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: &NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        validate_key(node_id)?;
        if let Some(rel) = &options.rel_filter {
            validate_key(rel)?;
        }
        if options.limit == 0 {
            return Err(MembusError::InvalidArgument(
                "query_neighbors limit must be > 0".to_owned(),
            ));
        }
        if options.limit > self.config.max_scan_limit {
            return Err(MembusError::InvalidArgument(format!(
                "query_neighbors limit {} exceeds max_scan_limit {}",
                options.limit, self.config.max_scan_limit
            )));
        }

        let deadline = self.query_deadline();
        let dirs = direction_tags(options.direction);
        let (resume_dir, resume_suffix) = parse_neighbor_cursor(options, dirs)?;

        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let ns_idx_prefix = encode_index_prefix(namespace, index_id);
        let mut edges = Vec::new();
        let mut last_cursor: Option<Vec<u8>> = None;
        let mut truncated = false;

        'dir_loop: for &dir_tag in dirs {
            if resume_dir == Some(EDGE_DIR_REVERSE) && dir_tag == EDGE_DIR_FORWARD {
                continue;
            }
            let seek_prefix = encode_edge_seek_prefix(
                namespace,
                index_id,
                dir_tag,
                node_id,
                options.rel_filter.as_deref(),
            );
            let dir_base_len = ns_idx_prefix.len() + 1;
            let seek_key = build_neighbor_seek_key(
                &ns_idx_prefix,
                dir_tag,
                &seek_prefix,
                resume_dir,
                resume_suffix,
            );

            let mut read_opts = ReadOptions::default();
            read_opts.fill_cache(false);
            let iter = snapshot.inner().iterator_cf_opt(
                &idx_edge_cf,
                read_opts,
                IteratorMode::From(&seek_key, Direction::Forward),
            );

            for (step, item) in iter.enumerate() {
                if step & 63 == 0 {
                    Self::check_query_deadline(deadline)?;
                }
                let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
                if !raw_k.starts_with(&seek_prefix) {
                    break;
                }
                if edges.len() >= options.limit {
                    truncated = true;
                    break 'dir_loop;
                }
                let suffix = &raw_k[dir_base_len..];
                let (first, rel_label, second, _owner_pk) = decode_edge_suffix(suffix)?;
                let (src_id, dst_id, direction) = if dir_tag == EDGE_DIR_FORWARD {
                    (first, second, GraphDirection::Out)
                } else {
                    (second, first, GraphDirection::In)
                };
                let mut cursor = Vec::with_capacity(1 + suffix.len());
                cursor.push(dir_tag);
                cursor.extend_from_slice(suffix);
                last_cursor = Some(cursor);
                edges.push(GraphEdgeRecord {
                    src_id,
                    rel_label,
                    dst_id,
                    direction,
                    meta: raw_v.to_vec(),
                });
            }
        }

        Ok(NeighborPage {
            edges,
            next_cursor: if truncated { last_cursor } else { None },
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    pub(crate) fn bfs_traverse_impl(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: &BfsOptions,
    ) -> Result<BfsResult, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        validate_key(seed_node)?;
        validate_bfs_options(options)?;
        if let Some(rel) = &options.rel_filter {
            validate_key(rel)?;
        }
        if options.max_nodes > self.config.max_scan_limit {
            return Err(MembusError::InvalidArgument(format!(
                "bfs max_nodes {} exceeds max_scan_limit {}",
                options.max_nodes, self.config.max_scan_limit
            )));
        }

        let deadline = self.query_deadline();
        let mut visited_set: HashSet<Vec<u8>> = HashSet::new();
        let mut visited_nodes: Vec<Vec<u8>> = Vec::new();
        let mut traversed_edges: Vec<GraphEdgeRecord> = Vec::new();
        let mut frontier_set: HashSet<Vec<u8>> = HashSet::new();
        let mut frontier_nodes: Vec<Vec<u8>> = Vec::new();
        let mut queue: VecDeque<(Vec<u8>, u8)> = VecDeque::new();
        let mut truncated = false;

        visited_set.insert(seed_node.to_vec());
        visited_nodes.push(seed_node.to_vec());

        if options.max_hops == 0 {
            frontier_nodes.push(seed_node.to_vec());
            return Ok(BfsResult {
                visited_nodes,
                traversed_edges,
                frontier_nodes,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }
        queue.push_back((seed_node.to_vec(), 0));

        let dirs = direction_tags(options.direction);
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let dir_base_len = encode_index_prefix(namespace, index_id).len() + 1;
        let mut total_steps = 0usize;

        'bfs: while let Some((node, hop)) = queue.pop_front() {
            if hop >= options.max_hops {
                continue;
            }
            for &dir_tag in dirs {
                let seek_prefix = encode_edge_seek_prefix(
                    namespace,
                    index_id,
                    dir_tag,
                    &node,
                    options.rel_filter.as_deref(),
                );
                let mut read_opts = ReadOptions::default();
                read_opts.fill_cache(false);
                let iter = snapshot.inner().iterator_cf_opt(
                    &idx_edge_cf,
                    read_opts,
                    IteratorMode::From(&seek_prefix, Direction::Forward),
                );
                for item in iter {
                    if total_steps & 63 == 0 {
                        Self::check_query_deadline(deadline)?;
                    }
                    total_steps = total_steps.saturating_add(1);
                    let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
                    if !raw_k.starts_with(&seek_prefix) {
                        break;
                    }
                    let (first, rel_label, second, _owner_pk) =
                        decode_edge_suffix(&raw_k[dir_base_len..])?;
                    let (src_id, dst_id, neighbor, direction) = if dir_tag == EDGE_DIR_FORWARD {
                        (first, second.clone(), second, GraphDirection::Out)
                    } else {
                        (second.clone(), first, second, GraphDirection::In)
                    };

                    if visited_set.contains(&neighbor) {
                        traversed_edges.push(GraphEdgeRecord {
                            src_id,
                            rel_label,
                            dst_id,
                            direction,
                            meta: raw_v.to_vec(),
                        });
                        continue;
                    }

                    if visited_set.len() >= options.max_nodes {
                        truncated = true;
                        if frontier_set.insert(node.clone()) {
                            frontier_nodes.push(node.clone());
                        }
                        while let Some((queued_node, _)) = queue.pop_front() {
                            if frontier_set.insert(queued_node.clone()) {
                                frontier_nodes.push(queued_node);
                            }
                        }
                        break 'bfs;
                    }

                    visited_set.insert(neighbor.clone());
                    visited_nodes.push(neighbor.clone());
                    traversed_edges.push(GraphEdgeRecord {
                        src_id,
                        rel_label,
                        dst_id,
                        direction,
                        meta: raw_v.to_vec(),
                    });
                    if hop + 1 < options.max_hops {
                        queue.push_back((neighbor, hop + 1));
                    } else if frontier_set.insert(neighbor.clone()) {
                        frontier_nodes.push(neighbor);
                    }
                }
            }
        }

        Ok(BfsResult {
            visited_nodes,
            traversed_edges,
            frontier_nodes,
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }
}

fn compute_scan_start(base_start: &[u8], after_key: Option<&[u8]>) -> Vec<u8> {
    match after_key {
        Some(after) => {
            let next = next_key_after(after);
            if next.as_slice() > base_start {
                next
            } else {
                base_start.to_vec()
            }
        }
        None => base_start.to_vec(),
    }
}

fn decode_multi_get_results(
    keys: &[Vec<u8>],
    raw_results: Vec<Result<Option<Vec<u8>>, rocksdb::Error>>,
) -> Result<Vec<Option<StoredRecord>>, MembusError> {
    let mut out = Vec::with_capacity(keys.len());
    for (key, res) in keys.iter().zip(raw_results) {
        match res.map_err(map_rocksdb_err)? {
            Some(raw) => out.push(Some(decode_record(key.clone(), &raw)?)),
            None => out.push(None),
        }
    }
    Ok(out)
}

fn direction_tags(direction: GraphDirection) -> &'static [u8] {
    match direction {
        GraphDirection::Out => &[EDGE_DIR_FORWARD],
        GraphDirection::In => &[EDGE_DIR_REVERSE],
        GraphDirection::Both => &[EDGE_DIR_FORWARD, EDGE_DIR_REVERSE],
    }
}

fn parse_neighbor_cursor<'a>(
    options: &'a NeighborQueryOptions,
    dirs: &[u8],
) -> Result<(Option<u8>, Option<&'a [u8]>), MembusError> {
    match options.after_cursor.as_deref() {
        Some(cur) if !cur.is_empty() => {
            if !dirs.contains(&cur[0]) {
                return Err(MembusError::InvalidArgument(format!(
                    "graph neighbor cursor direction tag 0x{:02x} is invalid for {:?}",
                    cur[0], options.direction
                )));
            }
            Ok((Some(cur[0]), Some(&cur[1..])))
        }
        Some(_) => Err(MembusError::InvalidArgument(
            "invalid empty graph neighbor cursor".to_owned(),
        )),
        None => Ok((None, None)),
    }
}

fn build_neighbor_seek_key(
    ns_idx_prefix: &[u8],
    dir_tag: u8,
    seek_prefix: &[u8],
    resume_dir: Option<u8>,
    resume_suffix: Option<&[u8]>,
) -> Vec<u8> {
    if resume_dir != Some(dir_tag) {
        return seek_prefix.to_vec();
    }
    let after = resume_suffix.unwrap_or(&[]);
    let next_after = next_key_after(after);
    let mut candidate = Vec::with_capacity(ns_idx_prefix.len() + 1 + next_after.len());
    candidate.extend_from_slice(ns_idx_prefix);
    candidate.push(dir_tag);
    candidate.extend_from_slice(&next_after);
    if candidate.as_slice() > seek_prefix {
        candidate
    } else {
        seek_prefix.to_vec()
    }
}
