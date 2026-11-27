use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::protocol::{Request, Response};
use crate::types::{
    Ack, BfsResult, CommitResult, MembusError, NeighborPage, OrderedIndexPage, SearchDocument,
    SearchPage, SearchQuery,
};

use super::core::Client;

impl Client {
    pub async fn query_ordered_index(
        &self,
        namespace: &str,
        index_id: &str,
        options: OrderedQueryOptions,
        snapshot_id: Option<u64>,
    ) -> Result<OrderedIndexPage, MembusError> {
        let resp = self
            .call(Request::QueryOrderedIndex {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::OrderedIndex(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for query_ordered_index: {other:?}"
            ))),
        }
    }

    pub async fn query_neighbors(
        &self,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: NeighborQueryOptions,
        snapshot_id: Option<u64>,
    ) -> Result<NeighborPage, MembusError> {
        let resp = self
            .call(Request::QueryNeighbors {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                node_id: node_id.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Neighbors(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for query_neighbors: {other:?}"
            ))),
        }
    }

    pub async fn bfs_traverse(
        &self,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: BfsOptions,
        snapshot_id: Option<u64>,
    ) -> Result<BfsResult, MembusError> {
        let resp = self
            .call(Request::BfsTraverse {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                seed_node: seed_node.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Bfs(res) => Ok(res),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for bfs_traverse: {other:?}"
            ))),
        }
    }

    pub async fn index_document(
        &self,
        doc: SearchDocument,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self.call(Request::IndexDocument { doc, ack }).await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for index_document: {other:?}"
            ))),
        }
    }

    pub async fn delete_search_document(
        &self,
        namespace: &str,
        doc_id: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::DeleteSearchDocument {
                namespace: namespace.to_owned(),
                doc_id: doc_id.to_owned(),
                ack,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for delete_search_document: {other:?}"
            ))),
        }
    }

    pub async fn revoke_search_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::RevokeSearchPrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_owned(),
                ack,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for revoke_search_prefix: {other:?}"
            ))),
        }
    }

    pub async fn search(&self, query: SearchQuery) -> Result<SearchPage, MembusError> {
        let resp = self.call(Request::Search(query)).await?;
        match resp {
            Response::Search(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for search: {other:?}"
            ))),
        }
    }
}
