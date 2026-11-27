use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::auth::AuthConfig;
use crate::types::{AuthMode, IndexMutations, MembusError, Permission, Principal, SearchLogOp};

/// RAII guard tracking one active transport connection (binary, WS, or SSE).
pub(crate) struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    pub(crate) fn acquire(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: Arc::clone(counter),
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Authorize write permissions on all graph edge endpoints referenced by `indexes`.
pub(crate) fn authorize_index_mutations(
    auth: &AuthConfig,
    principal: Option<&Principal>,
    namespace: &str,
    indexes: &IndexMutations,
) -> Result<(), MembusError> {
    if auth.mode() == AuthMode::Disabled {
        return Ok(());
    }
    for edge in &indexes.edges {
        auth.authorize(principal, namespace, &edge.src_id, Permission::Write)?;
        auth.authorize(principal, namespace, &edge.dst_id, Permission::Write)?;
    }
    Ok(())
}

/// Authorize write/admin permissions required for a secondary search log operation.
pub(crate) fn authorize_search_op(
    auth: &AuthConfig,
    principal: Option<&Principal>,
    op: &SearchLogOp,
) -> Result<(), MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            auth.authorize(principal, &doc.namespace, doc.doc_id.as_bytes(), Permission::Write)
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            auth.authorize(principal, namespace, doc_id.as_bytes(), Permission::Write)
        }
        SearchLogOp::RevokePrefix { namespace, .. } => {
            auth.authorize_admin(principal, Some(namespace))
        }
    }
}

/// Run a blocking query closure with an optional server-side timeout.
pub(crate) async fn run_blocking_query<T, F>(
    timeout: Option<Duration>,
    op_name: &'static str,
    f: F,
) -> Result<T, MembusError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, MembusError> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(f);
    match timeout {
        Some(limit) => match tokio::time::timeout(limit, handle).await {
            Ok(Ok(res)) => res,
            Ok(Err(join_err)) => Err(MembusError::Storage(format!(
                "{op_name} task failed: {join_err}"
            ))),
            Err(_) => Err(MembusError::Busy {
                retry_after_ms: 50,
                reason: format!("{op_name} query timeout exceeded ({limit:?})"),
            }),
        },
        None => handle
            .await
            .map_err(|join_err| MembusError::Storage(format!("{op_name} task failed: {join_err}")))?,
    }
}

/// Restrict remote checkpoint destinations strictly inside a configured `checkpoint_root`.
pub(crate) fn resolve_checkpoint_path(
    root: Option<&Path>,
    raw_dest: &str,
) -> Result<PathBuf, MembusError> {
    let root = root.ok_or_else(|| {
        MembusError::InvalidArgument(
            "checkpoint RPC is disabled unless server checkpoint_root is configured".to_owned(),
        )
    })?;
    if raw_dest.is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must not be empty".to_owned(),
        ));
    }
    let rel = Path::new(raw_dest);
    if rel.is_absolute() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must be a relative path under checkpoint_root".to_owned(),
        ));
    }

    let mut clean = PathBuf::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(MembusError::InvalidArgument(format!(
                    "checkpoint destination '{raw_dest}' escapes checkpoint_root"
                )));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must name a subdirectory under checkpoint_root".to_owned(),
        ));
    }
    Ok(root.join(clean))
}
