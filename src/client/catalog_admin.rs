use crate::protocol::{Request, Response};
use crate::types::{
    Ack, CollectionDescriptor, HealthStatus, MembusError, NamespaceParityDigest, ParityDigestMode,
    ReplicatedApplyResult, ReplicatedDelta, SlotRole, SlotRoleState,
};

use super::core::Client;

impl Client {
    pub async fn register_collection(
        &self,
        expected_version: Option<u32>,
        descriptor: CollectionDescriptor,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::RegisterCollection {
                expected_version,
                descriptor,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for register_collection: {other:?}"
            ))),
        }
    }

    pub async fn get_collection(
        &self,
        namespace: &str,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        let resp = self
            .call(Request::GetCollection {
                namespace: namespace.to_owned(),
            })
            .await?;
        match resp {
            Response::OptCollection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_collection: {other:?}"
            ))),
        }
    }

    pub async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>, MembusError> {
        let resp = self.call(Request::ListCollections).await?;
        match resp {
            Response::Collections(list) => Ok(list),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for list_collections: {other:?}"
            ))),
        }
    }

    pub async fn get_collection_version(
        &self,
        namespace: &str,
        version: u32,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        let resp = self
            .call(Request::GetCollectionVersion {
                namespace: namespace.to_owned(),
                version,
            })
            .await?;
        match resp {
            Response::OptCollection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_collection_version: {other:?}"
            ))),
        }
    }

    pub async fn begin_migration(
        &self,
        namespace: &str,
        expected_version: u32,
        target_version: u32,
        target_sha256: [u8; 32],
        target_schema_bytes: Vec<u8>,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::BeginMigration {
                namespace: namespace.to_owned(),
                expected_version,
                target_version,
                target_sha256,
                target_schema_bytes,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for begin_migration: {other:?}"
            ))),
        }
    }

    pub async fn commit_migration(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::CommitMigration {
                namespace: namespace.to_owned(),
                target_version,
                verified_through,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_migration: {other:?}"
            ))),
        }
    }

    pub async fn abort_migration(
        &self,
        namespace: &str,
        target_version: u32,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::AbortMigration {
                namespace: namespace.to_owned(),
                target_version,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for abort_migration: {other:?}"
            ))),
        }
    }

    pub async fn advance_migration_verification(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::AdvanceMigrationVerification {
                namespace: namespace.to_owned(),
                target_version,
                verified_through,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for advance_migration_verification: {other:?}"
            ))),
        }
    }

    pub async fn enter_reverse_shadow(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::EnterReverseShadow {
                namespace: namespace.to_owned(),
                target_version,
                verified_through,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for enter_reverse_shadow: {other:?}"
            ))),
        }
    }

    pub async fn rollback_migration(
        &self,
        namespace: &str,
        rollback_version: u32,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::RollbackMigration {
                namespace: namespace.to_owned(),
                rollback_version,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for rollback_migration: {other:?}"
            ))),
        }
    }

    pub async fn contract_migration(
        &self,
        namespace: &str,
        expected_version: u32,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::ContractMigration {
                namespace: namespace.to_owned(),
                expected_version,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for contract_migration: {other:?}"
            ))),
        }
    }

    pub async fn namespace_digest(
        &self,
        namespace: &str,
        key_prefix: &[u8],
        mode: ParityDigestMode,
    ) -> Result<NamespaceParityDigest, MembusError> {
        let resp = self
            .call(Request::NamespaceDigest {
                namespace: namespace.to_owned(),
                key_prefix: key_prefix.to_vec(),
                mode,
            })
            .await?;
        match resp {
            Response::NamespaceDigest(d) => Ok(d),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for namespace_digest: {other:?}"
            ))),
        }
    }

    pub async fn transition_slot_role(
        &self,
        expected_role: SlotRole,
        expected_role_epoch: u64,
        new_role: SlotRole,
    ) -> Result<SlotRoleState, MembusError> {
        let resp = self
            .call(Request::TransitionSlotRole {
                expected_role,
                expected_role_epoch,
                new_role,
            })
            .await?;
        match resp {
            Response::SlotRole(s) => Ok(s),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for transition_slot_role: {other:?}"
            ))),
        }
    }

    pub async fn apply_replicated_delta(
        &self,
        delta: ReplicatedDelta,
        ack: Ack,
        route_origin: Option<String>,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        let resp = self
            .call(Request::ApplyReplicatedDelta {
                delta,
                ack,
                route_origin,
            })
            .await?;
        match resp {
            Response::ReplicatedApply(r) => Ok(r),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for apply_replicated_delta: {other:?}"
            ))),
        }
    }

    pub async fn checkpoint(&self, dest_dir: &str) -> Result<u64, MembusError> {
        let resp = self
            .call(Request::Checkpoint {
                dest_dir: dest_dir.to_owned(),
            })
            .await?;
        match resp {
            Response::Checkpoint { seq } => Ok(seq),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for checkpoint: {other:?}"
            ))),
        }
    }

    pub async fn health(&self) -> Result<HealthStatus, MembusError> {
        let resp = self.call(Request::Health).await?;
        match resp {
            Response::Health(status) => Ok(status),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for health: {other:?}"
            ))),
        }
    }

    /// Query extended v2 server health including active snapshots, active streams,
    /// and deferred secondary search synchronization gauges.
    pub async fn health_v2(&self) -> Result<HealthStatus, MembusError> {
        let resp = self.call(Request::HealthV2).await?;
        match resp {
            Response::HealthV2(status) => Ok(status),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for health_v2: {other:?}"
            ))),
        }
    }
}
