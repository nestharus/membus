//! Multi-tenant namespace scoping (`t:<tenant_id>:<workspace>`) and hot-reloadable `AuthStore`.
use parking_lot::RwLock;
use subtle::ConstantTimeEq;

use crate::auth::{AuthConfig, TokenBinding};
use crate::types::{AclGrant, AuthMode, MembusError, Permission, Principal};

pub const TENANT_NAMESPACE_PREFIX: &str = "t:";
pub const MAX_TENANT_ID_LEN: usize = 64;
pub const MAX_WORKSPACE_SLUG_LEN: usize = 128;
pub const MIN_TENANT_TOKEN_LEN: usize = 16;

/// Validate a tenant identifier (`[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}`, never containing `:`, `*`, or traversal).
pub fn validate_tenant_id(tenant_id: &str) -> Result<(), MembusError> {
    if tenant_id.is_empty() || tenant_id.len() > MAX_TENANT_ID_LEN {
        return Err(MembusError::InvalidArgument(
            "tenant_id length must be in 1..=64".to_owned(),
        ));
    }
    let bytes = tenant_id.as_bytes();
    if !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(MembusError::InvalidArgument(format!(
            "invalid tenant_id '{tenant_id}' (only ASCII alphanumeric, '-', '_' allowed)"
        )));
    }
    Ok(())
}

/// Validate a workspace slug within a tenant (`[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}`, no `..`, `:`, `*`).
pub fn validate_workspace_slug(workspace: &str) -> Result<(), MembusError> {
    if workspace.is_empty() || workspace.len() > MAX_WORKSPACE_SLUG_LEN {
        return Err(MembusError::InvalidArgument(
            "workspace length must be in 1..=128".to_owned(),
        ));
    }
    if workspace.contains("..") || workspace.ends_with('.') {
        return Err(MembusError::InvalidArgument(format!(
            "invalid workspace slug '{workspace}'"
        )));
    }
    let bytes = workspace.as_bytes();
    if !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(MembusError::InvalidArgument(format!(
            "invalid workspace slug '{workspace}'"
        )));
    }
    Ok(())
}

/// Construct a tenant-isolated namespace `t:<tenant_id>:<workspace>`.
pub fn tenant_namespace(tenant_id: &str, workspace: &str) -> Result<String, MembusError> {
    validate_tenant_id(tenant_id)?;
    validate_workspace_slug(workspace)?;
    Ok(format!("{TENANT_NAMESPACE_PREFIX}{tenant_id}:{workspace}"))
}

/// Construct a strictly delimited namespace ACL pattern `t:<tenant_id>:*`.
///
/// The trailing colon guarantees tenant `foo` (`t:foo:*`) can never match tenant `foobar` (`t:foobar:ws`).
pub fn tenant_namespace_pattern(tenant_id: &str) -> Result<String, MembusError> {
    validate_tenant_id(tenant_id)?;
    Ok(format!("{TENANT_NAMESPACE_PREFIX}{tenant_id}:*"))
}

/// Extract `tenant_id` from a canonical `t:<tenant_id>:<workspace>` namespace, if valid.
pub fn extract_tenant_id(namespace: &str) -> Option<&str> {
    let rest = namespace.strip_prefix(TENANT_NAMESPACE_PREFIX)?;
    let (tenant, ws) = rest.split_once(':')?;
    if validate_tenant_id(tenant).is_ok() && validate_workspace_slug(ws).is_ok() {
        Some(tenant)
    } else {
        None
    }
}

/// Build a tenant-isolated [`AclGrant`] scoped strictly to `t:<tenant_id>:*`.
pub fn tenant_grant(tenant_id: &str, permissions: Vec<Permission>) -> Result<AclGrant, MembusError> {
    if permissions.is_empty() {
        return Err(MembusError::InvalidArgument(
            "tenant_grant requires at least one permission".to_owned(),
        ));
    }
    Ok(AclGrant {
        namespace_pattern: tenant_namespace_pattern(tenant_id)?,
        key_prefix: Vec::new(),
        permissions,
    })
}

/// Build a validated tenant-scoped [`TokenBinding`] enforcing minimum secret length and isolation.
pub fn tenant_token_binding(
    token: impl Into<String>,
    principal_id: impl Into<String>,
    tenant_id: &str,
    permissions: Vec<Permission>,
) -> Result<TokenBinding, MembusError> {
    let token = token.into();
    if token.len() < MIN_TENANT_TOKEN_LEN || token.trim().len() != token.len() {
        return Err(MembusError::InvalidArgument(format!(
            "tenant token must be at least {MIN_TENANT_TOKEN_LEN} non-whitespace-padded bytes"
        )));
    }
    let principal_id = principal_id.into();
    if principal_id.trim().is_empty() {
        return Err(MembusError::InvalidArgument(
            "principal_id must be non-empty".to_owned(),
        ));
    }
    let grant = tenant_grant(tenant_id, permissions)?;
    Ok(TokenBinding {
        token,
        principal: Principal {
            id: principal_id,
            grants: vec![grant],
        },
    })
}

/// Thread-safe, hot-reloadable authentication and ACL store for live token rotation and revocation.
///
/// Supports lock-free-friendly concurrent reads via `parking_lot::RwLock` while allowing
/// control planes (e.g. `managed-ai-cloud`) to atomically upsert, rotate, revoke, or reload
/// tenant tokens without restarting the resident or dropping active connections.
#[derive(Debug)]
pub struct AuthStore {
    inner: RwLock<AuthConfig>,
}

impl AuthStore {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            inner: RwLock::new(config),
        }
    }

    pub fn disabled() -> Self {
        Self::new(AuthConfig::disabled())
    }

    pub fn mode(&self) -> AuthMode {
        self.inner.read().mode()
    }

    /// Snapshot the current [`AuthConfig`].
    pub fn snapshot(&self) -> AuthConfig {
        self.inner.read().clone()
    }

    /// Atomically replace the active [`AuthConfig`].
    pub fn reload(&self, new_config: AuthConfig) {
        *self.inner.write() = new_config;
    }

    /// Upsert a token binding by constant-time token match (or insert if new).
    pub fn upsert_token(&self, binding: TokenBinding) -> Result<(), MembusError> {
        if binding.token.is_empty() {
            return Err(MembusError::InvalidArgument(
                "token binding must have a non-empty token".to_owned(),
            ));
        }
        let mut guard = self.inner.write();
        if let Some(existing) = guard
            .tokens
            .iter_mut()
            .find(|b| bool::from(b.token.as_bytes().ct_eq(binding.token.as_bytes())))
        {
            existing.principal = binding.principal;
        } else {
            guard.tokens.push(binding);
        }
        Ok(())
    }

    /// Revoke a token binding immediately; returns `true` iff a matching token was removed.
    pub fn revoke_token(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let mut guard = self.inner.write();
        let before = guard.tokens.len();
        guard
            .tokens
            .retain(|b| !bool::from(b.token.as_bytes().ct_eq(token.as_bytes())));
        guard.tokens.len() < before
    }

    /// Revoke all tokens belonging to `principal_id`; returns number of revoked bindings.
    pub fn revoke_principal(&self, principal_id: &str) -> usize {
        let mut guard = self.inner.write();
        let before = guard.tokens.len();
        guard.tokens.retain(|b| b.principal.id != principal_id);
        before - guard.tokens.len()
    }

    pub fn require_authenticated(&self, principal: Option<&Principal>) -> Result<(), MembusError> {
        self.inner.read().require_authenticated(principal)
    }

    pub fn authenticate_token(&self, token: Option<&str>) -> Result<Option<Principal>, MembusError> {
        self.inner.read().authenticate_token(token)
    }

    pub fn authorize(
        &self,
        principal: Option<&Principal>,
        namespace: &str,
        key: &[u8],
        required: Permission,
    ) -> Result<(), MembusError> {
        self.inner
            .read()
            .authorize(principal, namespace, key, required)
    }

    pub fn authorize_range(
        &self,
        principal: Option<&Principal>,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        required: Permission,
    ) -> Result<(), MembusError> {
        self.inner
            .read()
            .authorize_range(principal, namespace, start_key, end_key, required)
    }

    pub fn authorize_admin(
        &self,
        principal: Option<&Principal>,
        namespace: Option<&str>,
    ) -> Result<(), MembusError> {
        self.inner.read().authorize_admin(principal, namespace)
    }
}
