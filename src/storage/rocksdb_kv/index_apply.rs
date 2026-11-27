use std::collections::HashSet;
use std::sync::Arc;

use rocksdb::{BoundColumnFamily, ErrorKind, OptimisticTransactionDB, ReadOptions, Transaction};

use crate::index::{
    encode_edge_key, encode_ordered_key, validate_index_mutations, ReversePostings,
    EDGE_DIR_FORWARD, EDGE_DIR_REVERSE,
};
use crate::types::{IndexMutations, MembusError};

fn map_tx_err(err: rocksdb::Error) -> MembusError {
    match err.kind() {
        ErrorKind::Busy | ErrorKind::TryAgain => MembusError::TxConflict(err.into_string()),
        ErrorKind::Corruption => MembusError::Corruption(err.into_string()),
        ErrorKind::InvalidArgument => MembusError::InvalidArgument(err.into_string()),
        _ => MembusError::Storage(err.into_string()),
    }
}

/// Column-family handles for applying secondary index mutations inside a RocksDB transaction.
pub(crate) struct IndexCfHandles<'a> {
    pub(crate) ord: &'a Arc<BoundColumnFamily<'a>>,
    pub(crate) edge: &'a Arc<BoundColumnFamily<'a>>,
    pub(crate) rev: &'a Arc<BoundColumnFamily<'a>>,
}

/// Target record identity for secondary index mutation application.
pub(crate) struct IndexTarget<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) pk: &'a [u8],
    pub(crate) enc_pk: &'a [u8],
}

pub(crate) fn validate_and_estimate_indexes(
    muts: &IndexMutations,
    max_value_bytes: usize,
) -> Result<usize, MembusError> {
    validate_index_mutations(muts, max_value_bytes)?;
    let ord_bytes: usize = muts
        .ordered
        .iter()
        .map(|o| o.index_id.len() + o.sort_key.len() + o.summary.len() + 16)
        .sum();
    let edge_bytes: usize = muts
        .edges
        .iter()
        .map(|e| {
            (e.index_id.len()
                + e.src_id.len()
                + e.rel_label.len()
                + e.dst_id.len()
                + e.meta.len()
                + 24)
                * 2
        })
        .sum();
    Ok(ord_bytes.saturating_add(edge_bytes))
}

pub(crate) fn apply_index_mutations_in_tx(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    read_opts: &ReadOptions,
    cfs: &IndexCfHandles<'_>,
    target: &IndexTarget<'_>,
    new_indexes: Option<&IndexMutations>,
) -> Result<(), MembusError> {
    let prev_raw = tx
        .get_pinned_for_update_cf_opt(cfs.rev, target.enc_pk, true, read_opts)
        .map_err(map_tx_err)?;

    let had_prev = if let Some(raw) = prev_raw {
        let prev = ReversePostings::decode(raw.as_ref())?;
        for old_ord in &prev.ord_keys {
            tx.delete_cf(cfs.ord, old_ord).map_err(map_tx_err)?;
        }
        for old_edge in &prev.edge_keys {
            tx.delete_cf(cfs.edge, old_edge).map_err(map_tx_err)?;
        }
        true
    } else {
        false
    };

    if let Some(muts) = new_indexes.filter(|m| !m.is_empty()) {
        let mut new_rev = ReversePostings::default();
        let mut seen_ord: HashSet<Vec<u8>> = HashSet::with_capacity(muts.ordered.len());
        let mut seen_edge: HashSet<Vec<u8>> = HashSet::with_capacity(muts.edges.len() * 2);

        for ord in &muts.ordered {
            let k = encode_ordered_key(target.namespace, &ord.index_id, &ord.sort_key, target.pk);
            tx.put_cf(cfs.ord, &k, &ord.summary).map_err(map_tx_err)?;
            if seen_ord.insert(k.clone()) {
                new_rev.ord_keys.push(k);
            }
        }
        for edge in &muts.edges {
            let fwd = encode_edge_key(
                target.namespace,
                &edge.index_id,
                EDGE_DIR_FORWARD,
                &edge.src_id,
                &edge.rel_label,
                &edge.dst_id,
                target.pk,
            );
            let rev = encode_edge_key(
                target.namespace,
                &edge.index_id,
                EDGE_DIR_REVERSE,
                &edge.dst_id,
                &edge.rel_label,
                &edge.src_id,
                target.pk,
            );
            tx.put_cf(cfs.edge, &fwd, &edge.meta).map_err(map_tx_err)?;
            tx.put_cf(cfs.edge, &rev, &edge.meta).map_err(map_tx_err)?;
            if seen_edge.insert(fwd.clone()) {
                new_rev.edge_keys.push(fwd);
            }
            if seen_edge.insert(rev.clone()) {
                new_rev.edge_keys.push(rev);
            }
        }
        tx.put_cf(cfs.rev, target.enc_pk, new_rev.encode())
            .map_err(map_tx_err)?;
    } else if had_prev {
        tx.delete_cf(cfs.rev, target.enc_pk).map_err(map_tx_err)?;
    }

    Ok(())
}
