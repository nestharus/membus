use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::storage::{MetaCasGuard, StorageEngine};
use crate::types::{
    validate_namespace, CollectionDescriptor, ExpectedVersion, MembusError, MigrationState,
};

const CAT_CURRENT_PREFIX: &[u8] = b"cat:cur:";
const CAT_VERSION_PREFIX: &[u8] = b"cat:ver:";
const CAT_MIGRATION_PREFIX: &[u8] = b"cat:mig:";
const CAT_VERIFIED_PREFIX: &[u8] = b"cat:mig_ver:";

/// Maximum permitted raw schema size in bytes (1 MiB).
pub const MAX_SCHEMA_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingMigrationTarget {
    target_version: u32,
    target_sha256: [u8; 32],
    #[serde(with = "crate::types::base64_bytes")]
    target_schema_bytes: Vec<u8>,
}

/// CAS-fenced versioned collection, store-profile, schema & migration catalog over `meta` CF.
pub struct CollectionCatalog {
    storage: Arc<dyn StorageEngine>,
}

impl CollectionCatalog {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
    }

    /// Register or upgrade a collection descriptor with optimistic version fencing,
    /// retaining every historical version immutably for schema lineage audits.
    pub fn register_collection(
        &self,
        expected_version: Option<u32>,
        descriptor: CollectionDescriptor,
    ) -> Result<CollectionDescriptor, MembusError> {
        validate_namespace(&descriptor.namespace)?;
        if descriptor.schema_version == 0 {
            return Err(MembusError::InvalidArgument(
                "schema_version must be >= 1".to_owned(),
            ));
        }
        validate_schema_bytes(&descriptor.schema_bytes, &descriptor.schema_sha256)?;
        if descriptor.migration_state != MigrationState::Active {
            return Err(MembusError::InvalidArgument(
                "register_collection requires MigrationState::Active".to_owned(),
            ));
        }

        let cur_key = current_key(&descriptor.namespace);
        let ver_key = version_key(&descriptor.namespace, descriptor.schema_version);

        let prev_raw = self.storage.meta_get(&cur_key)?;
        let prev_desc: Option<CollectionDescriptor> = match &prev_raw {
            Some(bytes) => Some(decode_descriptor(bytes)?),
            None => None,
        };

        match (expected_version, &prev_desc) {
            (None, None) => {}
            (None, Some(existing)) => {
                return Err(MembusError::Conflict {
                    namespace: descriptor.namespace,
                    key: cur_key,
                    expected: ExpectedVersion::Absent,
                    actual: Some(u64::from(existing.schema_version)),
                });
            }
            (Some(exp), None) => {
                return Err(MembusError::Conflict {
                    namespace: descriptor.namespace,
                    key: cur_key,
                    expected: ExpectedVersion::Version(u64::from(exp)),
                    actual: None,
                });
            }
            (Some(exp), Some(existing)) => {
                if existing.schema_version != exp {
                    return Err(MembusError::Conflict {
                        namespace: descriptor.namespace,
                        key: cur_key,
                        expected: ExpectedVersion::Version(u64::from(exp)),
                        actual: Some(u64::from(existing.schema_version)),
                    });
                }
                if existing.migration_state != MigrationState::Active {
                    return Err(MembusError::InvalidArgument(format!(
                        "collection '{}' has an active migration in progress",
                        descriptor.namespace
                    )));
                }
                if descriptor.schema_version <= existing.schema_version {
                    return Err(MembusError::InvalidArgument(format!(
                        "new schema_version {} must exceed current version {}",
                        descriptor.schema_version, existing.schema_version
                    )));
                }
            }
        }

        let encoded = encode_descriptor(&descriptor)?;
        self.storage.meta_cas_batch(
            &[
                MetaCasGuard {
                    key: cur_key.clone(),
                    expected: prev_raw,
                },
                MetaCasGuard {
                    key: ver_key.clone(),
                    expected: None,
                },
            ],
            &[(cur_key, encoded.clone()), (ver_key, encoded)],
            &[],
            true,
        )?;

        Ok(descriptor)
    }

    pub fn get_collection(
        &self,
        namespace: &str,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        validate_namespace(namespace)?;
        let Some(raw) = self.storage.meta_get(&current_key(namespace))? else {
            return Ok(None);
        };
        Ok(Some(decode_descriptor(&raw)?))
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionDescriptor>, MembusError> {
        let pairs = self.storage.meta_scan_prefix(CAT_CURRENT_PREFIX)?;
        let mut out = Vec::with_capacity(pairs.len());
        for (_k, v) in pairs {
            out.push(decode_descriptor(&v)?);
        }
        Ok(out)
    }

    pub fn get_collection_version(
        &self,
        namespace: &str,
        version: u32,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        validate_namespace(namespace)?;
        let Some(raw) = self.storage.meta_get(&version_key(namespace, version))? else {
            return Ok(None);
        };
        Ok(Some(decode_descriptor(&raw)?))
    }

    /// Return the persisted `verified_through` sequence recorded by the last committed migration.
    pub fn last_verified_through(&self, namespace: &str) -> Result<Option<u64>, MembusError> {
        validate_namespace(namespace)?;
        let Some(raw) = self.storage.meta_get(&verified_key(namespace))? else {
            return Ok(None);
        };
        if raw.len() != 8 {
            return Err(MembusError::Corruption(
                "corrupt migration verified_through watermark".to_owned(),
            ));
        }
        Ok(Some(u64::from_be_bytes(raw.try_into().unwrap())))
    }

    /// Transition an `Active` collection into `MigrationState::Migrating`.
    pub fn begin_migration(
        &self,
        namespace: &str,
        expected_version: u32,
        target_version: u32,
        target_sha256: [u8; 32],
        target_schema_bytes: Vec<u8>,
    ) -> Result<CollectionDescriptor, MembusError> {
        validate_namespace(namespace)?;
        validate_schema_bytes(&target_schema_bytes, &target_sha256)?;

        let cur_key = current_key(namespace);
        let mig_key = migration_key(namespace);
        let prev_raw = self.storage.meta_get(&cur_key)?.ok_or_else(|| {
            MembusError::NotFound(format!("collection '{namespace}' not found"))
        })?;
        let mut current = decode_descriptor(&prev_raw)?;

        if current.schema_version != expected_version {
            return Err(MembusError::Conflict {
                namespace: namespace.to_owned(),
                key: cur_key,
                expected: ExpectedVersion::Version(u64::from(expected_version)),
                actual: Some(u64::from(current.schema_version)),
            });
        }
        if current.migration_state != MigrationState::Active {
            return Err(MembusError::InvalidArgument(format!(
                "collection '{namespace}' is already migrating"
            )));
        }
        if target_version <= current.schema_version {
            return Err(MembusError::InvalidArgument(format!(
                "target_version {target_version} must exceed current schema_version {}",
                current.schema_version
            )));
        }

        current.migration_state = MigrationState::Migrating {
            target_version,
            target_sha256,
            verified_through: 0,
        };
        let target_payload = serde_json::to_vec(&PendingMigrationTarget {
            target_version,
            target_sha256,
            target_schema_bytes,
        })
        .map_err(|err| MembusError::Storage(format!("encode migration target failed: {err}")))?;
        let cur_encoded = encode_descriptor(&current)?;

        self.storage.meta_cas_batch(
            &[
                MetaCasGuard {
                    key: cur_key.clone(),
                    expected: Some(prev_raw),
                },
                MetaCasGuard {
                    key: mig_key.clone(),
                    expected: None,
                },
            ],
            &[(cur_key, cur_encoded), (mig_key, target_payload)],
            &[],
            true,
        )?;

        Ok(current)
    }

    /// Monotonically advance `verified_through` on an active migration.
    pub fn advance_migration_verification(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        validate_namespace(namespace)?;
        let cur_key = current_key(namespace);
        let prev_raw = self.storage.meta_get(&cur_key)?.ok_or_else(|| {
            MembusError::NotFound(format!("collection '{namespace}' not found"))
        })?;
        let mut current = decode_descriptor(&prev_raw)?;

        match current.migration_state {
            MigrationState::Migrating {
                target_version: active_target,
                target_sha256,
                verified_through: prev_verified,
            } if active_target == target_version => {
                if verified_through < prev_verified {
                    return Err(MembusError::InvalidArgument(format!(
                        "verified_through {verified_through} cannot regress below {prev_verified}"
                    )));
                }
                current.migration_state = MigrationState::Migrating {
                    target_version: active_target,
                    target_sha256,
                    verified_through,
                };
                let cur_encoded = encode_descriptor(&current)?;
                self.storage.meta_cas_batch(
                    &[MetaCasGuard {
                        key: cur_key.clone(),
                        expected: Some(prev_raw),
                    }],
                    &[(cur_key, cur_encoded)],
                    &[],
                    true,
                )?;
                Ok(current)
            }
            _ => Err(MembusError::InvalidArgument(format!(
                "collection '{namespace}' has no active migration targeting version {target_version}"
            ))),
        }
    }

    /// Commit an in-flight migration, re-verifying schema SHA-256, checking monotonic
    /// `verified_through`, persisting the watermark, and archiving the promoted version.
    pub fn commit_migration(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        validate_namespace(namespace)?;
        let cur_key = current_key(namespace);
        let mig_key = migration_key(namespace);
        let ver_wm_key = verified_key(namespace);
        let prev_raw = self.storage.meta_get(&cur_key)?.ok_or_else(|| {
            MembusError::NotFound(format!("collection '{namespace}' not found"))
        })?;
        let mut current = decode_descriptor(&prev_raw)?;

        match current.migration_state {
            MigrationState::Migrating {
                target_version: active_target,
                target_sha256,
                verified_through: prev_verified,
            } if active_target == target_version => {
                if verified_through < prev_verified {
                    return Err(MembusError::InvalidArgument(format!(
                        "commit verified_through {verified_through} regressed below {prev_verified}"
                    )));
                }
                let mig_raw = self.storage.meta_get(&mig_key)?.ok_or_else(|| {
                    MembusError::Corruption(format!(
                        "missing pending migration payload for '{namespace}'"
                    ))
                })?;
                let pending: PendingMigrationTarget =
                    serde_json::from_slice(&mig_raw).map_err(|err| {
                        MembusError::Corruption(format!("corrupt migration payload: {err}"))
                    })?;
                if pending.target_version != target_version || pending.target_sha256 != target_sha256
                {
                    return Err(MembusError::Corruption(
                        "migration target metadata mismatch".to_owned(),
                    ));
                }
                // Re-verify persisted pending schema bytes against the sealed target digest.
                let actual_sha: [u8; 32] = Sha256::digest(&pending.target_schema_bytes).into();
                if actual_sha != target_sha256 {
                    return Err(MembusError::Corruption(
                        "persisted migration target schema_bytes failed SHA-256 verification"
                            .to_owned(),
                    ));
                }

                current.schema_version = target_version;
                current.schema_sha256 = target_sha256;
                current.schema_bytes = pending.target_schema_bytes;
                current.migration_state = MigrationState::Active;

                let ver_key = version_key(namespace, target_version);
                let cur_encoded = encode_descriptor(&current)?;

                self.storage.meta_cas_batch(
                    &[
                        MetaCasGuard {
                            key: cur_key.clone(),
                            expected: Some(prev_raw),
                        },
                        MetaCasGuard {
                            key: mig_key.clone(),
                            expected: Some(mig_raw),
                        },
                        MetaCasGuard {
                            key: ver_key.clone(),
                            expected: None,
                        },
                    ],
                    &[
                        (cur_key, cur_encoded.clone()),
                        (ver_key, cur_encoded),
                        (ver_wm_key, verified_through.to_be_bytes().to_vec()),
                    ],
                    &[mig_key],
                    true,
                )?;
                Ok(current)
            }
            _ => Err(MembusError::InvalidArgument(format!(
                "collection '{namespace}' has no active migration targeting version {target_version}"
            ))),
        }
    }

    /// Abort an in-flight migration cleanly, restoring `MigrationState::Active`.
    pub fn abort_migration(
        &self,
        namespace: &str,
        target_version: u32,
    ) -> Result<CollectionDescriptor, MembusError> {
        validate_namespace(namespace)?;
        let cur_key = current_key(namespace);
        let mig_key = migration_key(namespace);
        let prev_raw = self.storage.meta_get(&cur_key)?.ok_or_else(|| {
            MembusError::NotFound(format!("collection '{namespace}' not found"))
        })?;
        let mut current = decode_descriptor(&prev_raw)?;

        match current.migration_state {
            MigrationState::Migrating {
                target_version: active_target,
                ..
            } if active_target == target_version => {
                let mig_raw = self.storage.meta_get(&mig_key)?;
                current.migration_state = MigrationState::Active;
                let cur_encoded = encode_descriptor(&current)?;
                self.storage.meta_cas_batch(
                    &[
                        MetaCasGuard {
                            key: cur_key.clone(),
                            expected: Some(prev_raw),
                        },
                        MetaCasGuard {
                            key: mig_key.clone(),
                            expected: mig_raw,
                        },
                    ],
                    &[(cur_key, cur_encoded)],
                    &[mig_key],
                    true,
                )?;
                Ok(current)
            }
            _ => Err(MembusError::InvalidArgument(format!(
                "collection '{namespace}' has no active migration targeting version {target_version}"
            ))),
        }
    }
}

fn current_key(namespace: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(CAT_CURRENT_PREFIX.len() + namespace.len());
    k.extend_from_slice(CAT_CURRENT_PREFIX);
    k.extend_from_slice(namespace.as_bytes());
    k
}

fn version_key(namespace: &str, version: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(CAT_VERSION_PREFIX.len() + namespace.len() + 5);
    k.extend_from_slice(CAT_VERSION_PREFIX);
    k.extend_from_slice(namespace.as_bytes());
    k.push(b':');
    k.extend_from_slice(&version.to_be_bytes());
    k
}

fn migration_key(namespace: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(CAT_MIGRATION_PREFIX.len() + namespace.len());
    k.extend_from_slice(CAT_MIGRATION_PREFIX);
    k.extend_from_slice(namespace.as_bytes());
    k
}

fn verified_key(namespace: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(CAT_VERIFIED_PREFIX.len() + namespace.len());
    k.extend_from_slice(CAT_VERIFIED_PREFIX);
    k.extend_from_slice(namespace.as_bytes());
    k
}

fn validate_schema_bytes(schema_bytes: &[u8], expected: &[u8; 32]) -> Result<(), MembusError> {
    if schema_bytes.len() > MAX_SCHEMA_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "schema_bytes length {} exceeds maximum {MAX_SCHEMA_BYTES}",
            schema_bytes.len()
        )));
    }
    let actual: [u8; 32] = Sha256::digest(schema_bytes).into();
    if actual != *expected {
        return Err(MembusError::InvalidArgument(
            "schema_sha256 does not match SHA-256 of schema_bytes".to_owned(),
        ));
    }
    Ok(())
}

fn encode_descriptor(desc: &CollectionDescriptor) -> Result<Vec<u8>, MembusError> {
    serde_json::to_vec(desc)
        .map_err(|err| MembusError::Storage(format!("encode collection descriptor failed: {err}")))
}

fn decode_descriptor(raw: &[u8]) -> Result<CollectionDescriptor, MembusError> {
    serde_json::from_slice(raw)
        .map_err(|err| MembusError::Corruption(format!("corrupt collection descriptor: {err}")))
}
