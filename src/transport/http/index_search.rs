use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response as AxumResponse;
use axum::Json;
use serde::Deserialize;

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions};
use crate::transport::helpers::run_blocking_query;
use crate::transport::http::state::{
    authenticate_http, coordinated_commit_response, decode_bytes_field, decode_opt_bytes_field,
    default_ack, parse_ack_param, parse_role_epoch_header, HttpError, HttpState,
};
use crate::transport::write_coord::{
    coordinated_delete_search_document, coordinated_index_document,
    coordinated_revoke_search_prefix,
};
use crate::types::{
    Ack, BfsResult, GraphDirection, MembusError, NeighborPage, OrderedIndexPage, Permission,
    SearchDocument, SearchPage, SearchQuery,
};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HttpOrderedFilter {
    All,
    Prefix {
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
    },
    Range {
        start_utf8: Option<String>,
        start_b64: Option<String>,
        end_utf8: Option<String>,
        end_b64: Option<String>,
    },
}

impl HttpOrderedFilter {
    fn into_filter(self) -> Result<OrderedKeyFilter, MembusError> {
        match self {
            Self::All => Ok(OrderedKeyFilter::All),
            Self::Prefix {
                prefix_utf8,
                prefix_b64,
            } => Ok(OrderedKeyFilter::Prefix(decode_bytes_field(
                prefix_utf8,
                prefix_b64,
                "prefix",
            )?)),
            Self::Range {
                start_utf8,
                start_b64,
                end_utf8,
                end_b64,
            } => Ok(OrderedKeyFilter::Range {
                start: decode_bytes_field(start_utf8, start_b64, "start")?,
                end: decode_bytes_field(end_utf8, end_b64, "end")?,
            }),
        }
    }
}

#[derive(Deserialize)]
pub(super) struct HttpOrderedQueryReq {
    namespace: String,
    index_id: String,
    filter: Option<HttpOrderedFilter>,
    after_cursor_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

pub(super) async fn handle_ordered_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpOrderedQueryReq>,
) -> Result<Json<OrderedIndexPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let defaults = OrderedQueryOptions::default();
    let options = OrderedQueryOptions {
        filter: match req.filter {
            Some(f) => f.into_filter()?,
            None => OrderedKeyFilter::All,
        },
        after_cursor: decode_opt_bytes_field(None, req.after_cursor_b64, "after_cursor")?,
        limit: req.limit.unwrap_or(defaults.limit),
        max_bytes: req.max_bytes.unwrap_or(defaults.max_bytes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "ordered_index_query",
        move || {
            let snap = storage.open_snapshot()?;
            storage.query_ordered_index(&snap, &req.namespace, &req.index_id, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
pub(super) struct HttpNeighborsReq {
    namespace: String,
    index_id: String,
    node_id_utf8: Option<String>,
    node_id_b64: Option<String>,
    #[serde(default = "default_graph_dir")]
    direction: GraphDirection,
    rel_filter_utf8: Option<String>,
    rel_filter_b64: Option<String>,
    after_cursor_b64: Option<String>,
    limit: Option<usize>,
}

fn default_graph_dir() -> GraphDirection {
    GraphDirection::Out
}

pub(super) async fn handle_graph_neighbors(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpNeighborsReq>,
) -> Result<Json<NeighborPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let node_id = decode_bytes_field(req.node_id_utf8, req.node_id_b64, "node_id")?;
    let options = NeighborQueryOptions {
        direction: req.direction,
        rel_filter: decode_opt_bytes_field(req.rel_filter_utf8, req.rel_filter_b64, "rel_filter")?,
        after_cursor: decode_opt_bytes_field(None, req.after_cursor_b64, "after_cursor")?,
        limit: req.limit.unwrap_or(NeighborQueryOptions::default().limit),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "graph_neighbors",
        move || {
            let snap = storage.open_snapshot()?;
            storage.query_neighbors(&snap, &req.namespace, &req.index_id, &node_id, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
pub(super) struct HttpBfsReq {
    namespace: String,
    index_id: String,
    seed_node_utf8: Option<String>,
    seed_node_b64: Option<String>,
    #[serde(default = "default_graph_dir")]
    direction: GraphDirection,
    rel_filter_utf8: Option<String>,
    rel_filter_b64: Option<String>,
    max_hops: Option<u8>,
    max_nodes: Option<usize>,
}

pub(super) async fn handle_graph_bfs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpBfsReq>,
) -> Result<Json<BfsResult>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let seed_node = decode_bytes_field(req.seed_node_utf8, req.seed_node_b64, "seed_node")?;
    let defaults = BfsOptions::default();
    let options = BfsOptions {
        direction: req.direction,
        rel_filter: decode_opt_bytes_field(req.rel_filter_utf8, req.rel_filter_b64, "rel_filter")?,
        max_hops: req.max_hops.unwrap_or(defaults.max_hops),
        max_nodes: req.max_nodes.unwrap_or(defaults.max_nodes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let res = run_blocking_query(
        state.ctx.config.query_timeout,
        "graph_bfs",
        move || {
            let snap = storage.open_snapshot()?;
            storage.bfs_traverse(&snap, &req.namespace, &req.index_id, &seed_node, &options)
        },
    )
    .await?;
    Ok(Json(res))
}

#[derive(Deserialize)]
pub(super) struct IndexDocReq {
    doc: SearchDocument,
    #[serde(default = "default_ack")]
    ack: Ack,
}

pub(super) async fn handle_search_index_doc(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<IndexDocReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.doc.namespace,
        req.doc.doc_id.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_index_document(&search, req.doc, req.ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("index_document failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
pub(super) struct SearchDeleteQuery {
    ack: Option<String>,
}

pub(super) async fn handle_search_delete_doc(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, doc_id)): Path<(String, String)>,
    Query(q): Query<SearchDeleteQuery>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        doc_id.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ack = parse_ack_param(q.ack.as_deref())?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_delete_search_document(&search, &ns, &doc_id, ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("delete_search_document failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
pub(super) struct RevokePrefixReq {
    namespace: String,
    prefix: String,
    #[serde(default = "default_ack")]
    ack: Ack,
}

pub(super) async fn handle_search_revoke_prefix(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<RevokePrefixReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&req.namespace))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_revoke_search_prefix(&search, &req.namespace, &req.prefix, req.ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("revoke_search_prefix failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

pub(super) async fn handle_search_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(query): Json<SearchQuery>,
) -> Result<Json<SearchPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let ns = query.namespace.as_deref().unwrap_or("*");
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), ns, b"", Permission::Read)?;
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "search",
        move || search.search(&query),
    )
    .await?;
    Ok(Json(page))
}
