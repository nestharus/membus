use crate::protocol::{Request, Response};
use crate::types::{
    Ack, CommitResult, ExpectedVersion, IndexMutations, IndexedMutation, MembusError, ScanOptions,
    ScanPage, SearchLogOp, StoredRecord, VersionGuard, WriteMutation,
};

use super::core::Client;

impl Client {
    pub async fn get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.get_with_snapshot(namespace, key, None).await
    }

    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.get_many_with_snapshot(namespace, keys, None).await
    }

    pub async fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.cas(namespace, key, value, ack, ExpectedVersion::Any)
            .await
    }

    pub async fn cas(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Put {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put/cas: {other:?}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<IndexMutations>,
        search_op: Option<SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::PutIndexed {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                ack,
                expected,
                indexes,
                search_op,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_indexed: {other:?}"
            ))),
        }
    }

    pub async fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Remove {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for remove: {other:?}"
            ))),
        }
    }

    pub async fn remove_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::RemoveIndexed {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                ack,
                expected,
                search_op,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for remove_indexed: {other:?}"
            ))),
        }
    }

    pub async fn commit_tx(
        &self,
        guards: Vec<VersionGuard>,
        mutations: Vec<WriteMutation>,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::CommitTx {
                ack,
                guards,
                mutations,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_tx: {other:?}"
            ))),
        }
    }

    pub async fn commit_indexed_tx(
        &self,
        guards: Vec<VersionGuard>,
        mutations: Vec<IndexedMutation>,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::CommitIndexedTx {
                ack,
                guards,
                mutations,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_indexed_tx: {other:?}"
            ))),
        }
    }

    pub async fn scan_prefix(
        &self,
        namespace: &str,
        prefix: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanPrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_prefix: {other:?}"
            ))),
        }
    }

    pub async fn scan_range(
        &self,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanRange {
                namespace: namespace.to_owned(),
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_range: {other:?}"
            ))),
        }
    }

    pub(super) async fn get_with_snapshot(
        &self,
        namespace: &str,
        key: &[u8],
        snapshot_id: Option<u64>,
    ) -> Result<Option<StoredRecord>, MembusError> {
        let resp = self
            .call(Request::Get {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Get(rec) => Ok(rec),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get: {other:?}"
            ))),
        }
    }

    pub(super) async fn get_many_with_snapshot(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
        snapshot_id: Option<u64>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        let resp = self
            .call(Request::GetMany {
                namespace: namespace.to_owned(),
                keys,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::GetMany(recs) => Ok(recs),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_many: {other:?}"
            ))),
        }
    }

    pub(super) async fn close_snapshot_id(&self, snapshot_id: u64) -> Result<(), MembusError> {
        let resp = self.call(Request::CloseSnapshot { snapshot_id }).await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for close_snapshot: {other:?}"
            ))),
        }
    }
}
