use std::sync::Arc;
use std::time::Instant;

use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::storage::codec::encode_kv_key;
use crate::storage::StorageEngine;
use crate::types::{
    now_unix_ms, validate_key, validate_namespace, AuthMode, LeaseGrant, LeaseInfo, MembusError,
    Permission, Principal,
};

/// Maximum permitted lease TTL (24 hours).
pub const MAX_LEASE_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Token-to-principal binding configured in [`AuthConfig`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenBinding {
    pub token: String,
    pub principal: Principal,
}

/// Authentication & ACL configuration shared across binary sockets and HTTP/WS/SSE.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub tokens: Vec<TokenBinding>,
}

impl AuthConfig {
    pub fn disabled() -> Self {
        Self {
            mode: AuthMode::Disabled,
            tokens: Vec::new(),
        }
    }

    pub fn enforced(tokens: Vec<TokenBinding>) -> Self {
        Self {
            mode: AuthMode::Enforced,
            tokens,
        }
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Require that a principal is authenticated when `AuthMode::Enforced`.
    pub fn require_authenticated(
        &self,
        principal: Option<&Principal>,
    ) -> Result<(), MembusError> {
        if self.mode == AuthMode::Disabled {
            return Ok(());
        }
        if principal.is_some() {
            Ok(())
        } else {
            Err(MembusError::Unauthorized(
                "authentication required".to_owned(),
            ))
        }
    }

    /// Authorize `required` permission over a half-open key range `[start_key, end_key)`.
    pub fn authorize_range(
        &self,
        principal: Option<&Principal>,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        required: Permission,
    ) -> Result<(), MembusError> {
        if self.mode == AuthMode::Disabled {
            return Ok(());
        }
        let principal = principal
            .ok_or_else(|| MembusError::Unauthorized("authentication required".to_owned()))?;
        for grant in &principal.grants {
            if !namespace_matches(&grant.namespace_pattern, namespace) {
                continue;
            }
            if !permission_satisfied(&grant.permissions, required) {
                continue;
            }
            if grant.key_prefix.is_empty() {
                return Ok(());
            }
            if start_key.starts_with(&grant.key_prefix)
                && !end_key.is_empty()
                && end_key.starts_with(&grant.key_prefix)
            {
                return Ok(());
            }
        }
        Err(MembusError::Unauthorized(format!(
            "principal '{}' lacks {:?} range access to namespace '{}'",
            principal.id, required, namespace
        )))
    }

    /// Authenticate a bearer token using constant-time byte comparison across all bindings.
    pub fn authenticate_token(&self, token: Option<&str>) -> Result<Option<Principal>, MembusError> {
        match self.mode {
            AuthMode::Disabled => Ok(None),
            AuthMode::Enforced => {
                let candidate = token.ok_or_else(|| {
                    MembusError::Unauthorized("missing bearer token".to_owned())
                })?;
                if candidate.is_empty() {
                    return Err(MembusError::Unauthorized(
                        "empty bearer token".to_owned(),
                    ));
                }
                let candidate_hash: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
                let mut matched: Option<Principal> = None;
                for binding in &self.tokens {
                    let expected_hash: [u8; 32] = Sha256::digest(binding.token.as_bytes()).into();
                    let eq = bool::from(candidate_hash.ct_eq(&expected_hash));
                    if eq {
                        matched = Some(binding.principal.clone());
                    }
                }
                matched
                    .map(Some)
                    .ok_or_else(|| MembusError::Unauthorized("invalid bearer token".to_owned()))
            }
        }
    }

    /// Authorize `required` permission on `(namespace, key)` for an authenticated principal.
    pub fn authorize(
        &self,
        principal: Option<&Principal>,
        namespace: &str,
        key: &[u8],
        required: Permission,
    ) -> Result<(), MembusError> {
        if self.mode == AuthMode::Disabled {
            return Ok(());
        }
        let principal = principal.ok_or_else(|| {
            MembusError::Unauthorized("authentication required".to_owned())
        })?;
        for grant in &principal.grants {
            if !namespace_matches(&grant.namespace_pattern, namespace) {
                continue;
            }
            if !grant.key_prefix.is_empty() && !key.starts_with(&grant.key_prefix) {
                continue;
            }
            if permission_satisfied(&grant.permissions, required) {
                return Ok(());
            }
        }
        Err(MembusError::Unauthorized(format!(
            "principal '{}' lacks {:?} access to namespace '{}'",
            principal.id, required, namespace
        )))
    }

    /// Authorize an administrative operation globally (`namespace = None`) or scoped to a namespace.
    pub fn authorize_admin(
        &self,
        principal: Option<&Principal>,
        namespace: Option<&str>,
    ) -> Result<(), MembusError> {
        if self.mode == AuthMode::Disabled {
            return Ok(());
        }
        let principal = principal.ok_or_else(|| {
            MembusError::Unauthorized("authentication required for admin operation".to_owned())
        })?;
        for grant in &principal.grants {
            let ns_ok = match namespace {
                Some(ns) => namespace_matches(&grant.namespace_pattern, ns),
                None => grant.namespace_pattern == "*",
            };
            if ns_ok
                && grant.key_prefix.is_empty()
                && grant.permissions.contains(&Permission::Admin)
            {
                return Ok(());
            }
        }
        Err(MembusError::Unauthorized(format!(
            "principal '{}' lacks Admin authorization",
            principal.id
        )))
    }
}

fn namespace_matches(pattern: &str, namespace: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return namespace.starts_with(prefix);
    }
    pattern == namespace
}

fn permission_satisfied(granted: &[Permission], required: Permission) -> bool {
    granted.contains(&required) || granted.contains(&Permission::Admin)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseRecord {
    holder: String,
    generation: u64,
    token: String,
    expires_at_ms: u64,
    released: bool,
}

/// Per-resource atomic fenced lease manager backed by `lease` CF (`OptimisticTransactionDB`).
///
/// In-process lease timing is anchored monotonically to prevent NTP/wall-clock jumps from
/// extending or prematurely expiring active leases, while persisting epoch-ms deadlines
/// across process restarts. Lease secret tokens are drawn from OS CSPRNG (`ring::rand::SystemRandom`).
pub struct LeaseManager {
    storage: Arc<dyn StorageEngine>,
    rng: SystemRandom,
    boot_wall_ms: u64,
    boot_instant: Instant,
}

impl LeaseManager {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            rng: SystemRandom::new(),
            boot_wall_ms: now_unix_ms(),
            boot_instant: Instant::now(),
        }
    }

    /// Monotonically advancing millisecond clock anchored to wall-clock time at startup.
    fn monotonic_now_ms(&self) -> u64 {
        let elapsed_ms = u64::try_from(self.boot_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.boot_wall_ms.saturating_add(elapsed_ms)
    }

    fn generate_lease_token(&self) -> Result<String, MembusError> {
        let mut raw = [0u8; 32];
        self.rng
            .fill(&mut raw)
            .map_err(|_| MembusError::Storage("CSPRNG failure generating lease token".to_owned()))?;
        Ok(hex_encode(&raw))
    }

    /// Acquire an exclusive fenced lease for `(namespace, resource)`.
    ///
    /// Rejects ANY active non-expired, unreleased lease with [`MembusError::LeaseHeld`],
    /// even if the caller echoes the active `holder` name (active holders MUST renew via
    /// `renew_lease` proving `(generation, token)`). Reclaims expired or released leases
    /// with a strictly monotonic `generation`.
    pub fn acquire_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        validate_lease_inputs(namespace, resource, holder, ttl_ms)?;
        let key = encode_kv_key(namespace, resource.as_bytes());

        for _ in 0..16 {
            let prev_raw = self.storage.lease_get(&key)?;
            let prev: Option<LeaseRecord> = match &prev_raw {
                Some(bytes) => Some(decode_lease_record(bytes)?),
                None => None,
            };
            let now = self.monotonic_now_ms();

            if let Some(rec) = &prev {
                if !rec.released && rec.expires_at_ms > now {
                    return Err(MembusError::LeaseHeld {
                        resource: resource.to_owned(),
                        holder: rec.holder.clone(),
                        generation: rec.generation,
                        expires_at_ms: rec.expires_at_ms,
                    });
                }
            }

            let generation = prev.as_ref().map_or(1, |r| r.generation.saturating_add(1));
            let token = self.generate_lease_token()?;
            let expires_at_ms = now.saturating_add(ttl_ms);
            let next = LeaseRecord {
                holder: holder.to_owned(),
                generation,
                token: token.clone(),
                expires_at_ms,
                released: false,
            };
            let encoded = encode_lease_record(&next)?;

            match self.storage.lease_cas(&key, prev_raw.as_deref(), &encoded) {
                Ok(()) => {
                    return Ok(LeaseGrant {
                        namespace: namespace.to_owned(),
                        resource: resource.to_owned(),
                        holder: holder.to_owned(),
                        generation,
                        token,
                        expires_at_ms,
                    });
                }
                Err(MembusError::TxConflict(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(format!(
            "lease acquire contention on '{resource}'"
        )))
    }

    /// Extend an active lease matching `(holder, generation, token)`.
    pub fn renew_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        validate_lease_inputs(namespace, resource, holder, ttl_ms)?;
        let key = encode_kv_key(namespace, resource.as_bytes());

        for _ in 0..16 {
            let prev_raw = self.storage.lease_get(&key)?;
            let Some(bytes) = &prev_raw else {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' does not exist"
                )));
            };
            let prev = decode_lease_record(bytes)?;
            let now = self.monotonic_now_ms();

            if prev.released {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' is inactive (already released at generation {})",
                    prev.generation
                )));
            }
            if prev.expires_at_ms <= now {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' is inactive (expired at generation {})",
                    prev.generation
                )));
            }
            if !lease_matches_active(&prev, holder, generation, token, now) {
                return Err(MembusError::LeaseHeld {
                    resource: resource.to_owned(),
                    holder: prev.holder,
                    generation: prev.generation,
                    expires_at_ms: prev.expires_at_ms,
                });
            }

            let expires_at_ms = now.saturating_add(ttl_ms);
            let next = LeaseRecord {
                holder: prev.holder.clone(),
                generation: prev.generation,
                token: prev.token.clone(),
                expires_at_ms,
                released: false,
            };
            let encoded = encode_lease_record(&next)?;

            match self.storage.lease_cas(&key, prev_raw.as_deref(), &encoded) {
                Ok(()) => {
                    return Ok(LeaseGrant {
                        namespace: namespace.to_owned(),
                        resource: resource.to_owned(),
                        holder: next.holder,
                        generation: next.generation,
                        token: next.token,
                        expires_at_ms,
                    });
                }
                Err(MembusError::TxConflict(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(format!(
            "lease renew contention on '{resource}'"
        )))
    }

    /// Explicitly release an active lease while preserving monotonic `generation` history.
    pub fn release_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<(), MembusError> {
        validate_namespace(namespace)?;
        validate_key(resource.as_bytes())?;
        validate_key(holder.as_bytes())?;
        let key = encode_kv_key(namespace, resource.as_bytes());

        for _ in 0..16 {
            let prev_raw = self.storage.lease_get(&key)?;
            let Some(bytes) = &prev_raw else {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' does not exist"
                )));
            };
            let prev = decode_lease_record(bytes)?;
            let now = self.monotonic_now_ms();

            if prev.released {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' is inactive (already released at generation {})",
                    prev.generation
                )));
            }
            if prev.expires_at_ms <= now {
                return Err(MembusError::NotFound(format!(
                    "lease '{resource}' is inactive (expired at generation {})",
                    prev.generation
                )));
            }
            if !lease_matches_active(&prev, holder, generation, token, now) {
                return Err(MembusError::LeaseHeld {
                    resource: resource.to_owned(),
                    holder: prev.holder,
                    generation: prev.generation,
                    expires_at_ms: prev.expires_at_ms,
                });
            }

            let next = LeaseRecord {
                holder: prev.holder,
                generation: prev.generation,
                token: String::new(),
                expires_at_ms: 0,
                released: true,
            };
            let encoded = encode_lease_record(&next)?;

            match self.storage.lease_cas(&key, prev_raw.as_deref(), &encoded) {
                Ok(()) => return Ok(()),
                Err(MembusError::TxConflict(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(format!(
            "lease release contention on '{resource}'"
        )))
    }

    /// Inspect public metadata for the currently active lease (never exposes secret `token`).
    pub fn inspect_lease(
        &self,
        namespace: &str,
        resource: &str,
    ) -> Result<Option<LeaseInfo>, MembusError> {
        validate_namespace(namespace)?;
        validate_key(resource.as_bytes())?;
        let key = encode_kv_key(namespace, resource.as_bytes());
        let Some(bytes) = self.storage.lease_get(&key)? else {
            return Ok(None);
        };
        let rec = decode_lease_record(&bytes)?;
        if rec.released || rec.expires_at_ms <= self.monotonic_now_ms() {
            return Ok(None);
        }
        Ok(Some(LeaseInfo {
            namespace: namespace.to_owned(),
            resource: resource.to_owned(),
            holder: rec.holder,
            generation: rec.generation,
            expires_at_ms: rec.expires_at_ms,
        }))
    }

    /// Validate whether `(holder, generation, token)` currently holds an active unexpired lease.
    pub fn validate_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<bool, MembusError> {
        validate_namespace(namespace)?;
        validate_key(resource.as_bytes())?;
        let key = encode_kv_key(namespace, resource.as_bytes());
        let Some(bytes) = self.storage.lease_get(&key)? else {
            return Ok(false);
        };
        let rec = decode_lease_record(&bytes)?;
        Ok(lease_matches_active(
            &rec,
            holder,
            generation,
            token,
            self.monotonic_now_ms(),
        ))
    }
}

fn validate_lease_inputs(
    namespace: &str,
    resource: &str,
    holder: &str,
    ttl_ms: u64,
) -> Result<(), MembusError> {
    validate_namespace(namespace)?;
    validate_key(resource.as_bytes())?;
    validate_key(holder.as_bytes())?;
    if ttl_ms == 0 || ttl_ms > MAX_LEASE_TTL_MS {
        return Err(MembusError::InvalidArgument(format!(
            "lease ttl_ms {ttl_ms} must be in 1..={MAX_LEASE_TTL_MS}"
        )));
    }
    Ok(())
}

fn lease_matches_active(
    rec: &LeaseRecord,
    holder: &str,
    generation: u64,
    token: &str,
    now_ms: u64,
) -> bool {
    if rec.released || rec.expires_at_ms <= now_ms {
        return false;
    }
    if rec.holder != holder || rec.generation != generation {
        return false;
    }
    let a: [u8; 32] = Sha256::digest(rec.token.as_bytes()).into();
    let b: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    bool::from(a.ct_eq(&b))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn encode_lease_record(rec: &LeaseRecord) -> Result<Vec<u8>, MembusError> {
    serde_json::to_vec(rec)
        .map_err(|err| MembusError::Storage(format!("failed to encode lease record: {err}")))
}

fn decode_lease_record(bytes: &[u8]) -> Result<LeaseRecord, MembusError> {
    serde_json::from_slice(bytes)
        .map_err(|err| MembusError::Corruption(format!("corrupt lease record: {err}")))
}
