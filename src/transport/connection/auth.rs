use crate::auth::AuthStore;
use crate::protocol::Request;
use crate::transport::helpers::{authorize_index_mutations, authorize_search_op};
use crate::types::{AuthMode, MembusError, Permission, Principal};

pub(super) fn authorize_request(
    auth: &AuthStore,
    principal: Option<&Principal>,
    req: &Request,
) -> Result<(), MembusError> {
    if auth.mode() == AuthMode::Disabled {
        return Ok(());
    }
    match req {
        Request::Authenticate { .. } => Ok(()),
        Request::Get { .. }
        | Request::GetMany { .. }
        | Request::Put { .. }
        | Request::PutIndexed { .. }
        | Request::Remove { .. }
        | Request::RemoveIndexed { .. }
        | Request::CommitTx { .. }
        | Request::CommitIndexedTx { .. }
        | Request::CommitIndexedTxWithLeases { .. } => authorize_kv_tx_request(auth, principal, req),
        Request::OpenSnapshot
        | Request::CloseSnapshot { .. }
        | Request::Health
        | Request::HealthV2
        | Request::ListCollections => auth.require_authenticated(principal),
        Request::ScanPrefix {
            namespace, prefix, ..
        } => auth.authorize(principal, namespace, prefix, Permission::Read),
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            ..
        } => auth.authorize_range(principal, namespace, start_key, end_key, Permission::Read),
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            ..
        }
        | Request::SubscribeLive {
            namespace,
            key_prefix,
            ..
        } => auth.authorize(principal, namespace, key_prefix, Permission::Read),
        Request::Checkpoint { .. }
        | Request::TransitionSlotRole { .. }
        | Request::PutBlob { .. }
        | Request::GetBlob { .. }
        | Request::QueryOrderedIndex { .. }
        | Request::QueryNeighbors { .. }
        | Request::BfsTraverse { .. }
        | Request::PutCasBlob { .. }
        | Request::GetCasBlob { .. }
        | Request::HasCasBlob { .. }
        | Request::AcquireLease { .. }
        | Request::RenewLease { .. }
        | Request::ReleaseLease { .. }
        | Request::InspectLease { .. }
        | Request::ValidateLease { .. }
        | Request::RegisterCollection { .. }
        | Request::GetCollection { .. }
        | Request::GetCollectionVersion { .. }
        | Request::BeginMigration { .. }
        | Request::AdvanceMigrationVerification { .. }
        | Request::EnterReverseShadow { .. }
        | Request::RollbackMigration { .. }
        | Request::ContractMigration { .. }
        | Request::CommitMigration { .. }
        | Request::AbortMigration { .. }
        | Request::NamespaceDigest { .. }
        | Request::ApplyReplicatedDelta { .. }
        | Request::IndexDocument { .. }
        | Request::DeleteSearchDocument { .. }
        | Request::RevokeSearchPrefix { .. }
        | Request::Search(_) => authorize_domain_request(auth, principal, req),
    }
}

fn authorize_kv_tx_request(
    auth: &AuthStore,
    principal: Option<&Principal>,
    req: &Request,
) -> Result<(), MembusError> {
    match req {
        Request::Get { namespace, key, .. } => {
            auth.authorize(principal, namespace, key, Permission::Read)
        }
        Request::GetMany {
            namespace, keys, ..
        } => {
            if keys.is_empty() {
                auth.authorize(principal, namespace, b"", Permission::Read)
            } else {
                for key in keys {
                    auth.authorize(principal, namespace, key, Permission::Read)?;
                }
                Ok(())
            }
        }
        Request::Put { namespace, key, .. } | Request::Remove { namespace, key, .. } => {
            auth.authorize(principal, namespace, key, Permission::Write)
        }
        Request::PutIndexed {
            namespace,
            key,
            indexes,
            search_op,
            ..
        } => {
            auth.authorize(principal, namespace, key, Permission::Write)?;
            if let Some(idx) = indexes {
                authorize_index_mutations(auth, principal, namespace, idx)?;
            }
            if let Some(op) = search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::RemoveIndexed {
            namespace,
            key,
            search_op,
            ..
        } => {
            auth.authorize(principal, namespace, key, Permission::Write)?;
            if let Some(op) = search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::CommitTx {
            guards, mutations, ..
        } => {
            auth.require_authenticated(principal)?;
            for g in guards {
                auth.authorize(principal, &g.namespace, &g.key, Permission::Read)?;
            }
            for m in mutations {
                auth.authorize(principal, &m.namespace, &m.key, Permission::Write)?;
            }
            Ok(())
        }
        Request::CommitIndexedTx {
            guards, mutations, ..
        } => {
            auth.require_authenticated(principal)?;
            for g in guards {
                auth.authorize(principal, &g.namespace, &g.key, Permission::Read)?;
            }
            for m in mutations {
                auth.authorize(
                    principal,
                    &m.mutation.namespace,
                    &m.mutation.key,
                    Permission::Write,
                )?;
                if let Some(idx) = &m.indexes {
                    authorize_index_mutations(auth, principal, &m.mutation.namespace, idx)?;
                }
                if let Some(op) = &m.search_op {
                    authorize_search_op(auth, principal, op)?;
                }
            }
            Ok(())
        }
        Request::CommitIndexedTxWithLeases {
            guards,
            lease_guards,
            mutations,
            ..
        } => {
            auth.require_authenticated(principal)?;
            for g in guards {
                auth.authorize(principal, &g.namespace, &g.key, Permission::Read)?;
            }
            for lg in lease_guards {
                auth.authorize(
                    principal,
                    &lg.namespace,
                    lg.resource.as_bytes(),
                    Permission::Write,
                )?;
            }
            for m in mutations {
                auth.authorize(
                    principal,
                    &m.mutation.namespace,
                    &m.mutation.key,
                    Permission::Write,
                )?;
                if let Some(idx) = &m.indexes {
                    authorize_index_mutations(auth, principal, &m.mutation.namespace, idx)?;
                }
                if let Some(op) = &m.search_op {
                    authorize_search_op(auth, principal, op)?;
                }
            }
            Ok(())
        }
        _ => Err(MembusError::Unauthorized(
            "unhandled request authorization".to_owned(),
        )),
    }
}

fn authorize_domain_request(
    auth: &AuthStore,
    principal: Option<&Principal>,
    req: &Request,
) -> Result<(), MembusError> {
    match req {
        Request::Checkpoint { .. } | Request::TransitionSlotRole { .. } => {
            auth.authorize_admin(principal, None)
        }
        Request::PutBlob { .. } => auth.authorize(principal, "*", b"", Permission::Write),
        Request::GetBlob { .. } => auth.authorize(principal, "*", b"", Permission::Read),
        Request::QueryOrderedIndex { namespace, .. }
        | Request::QueryNeighbors { namespace, .. }
        | Request::BfsTraverse { namespace, .. }
        | Request::GetCasBlob { namespace, .. }
        | Request::HasCasBlob { namespace, .. }
        | Request::GetCollection { namespace }
        | Request::GetCollectionVersion { namespace, .. } => {
            auth.authorize(principal, namespace, b"", Permission::Read)
        }
        Request::PutCasBlob { namespace, .. } => {
            auth.authorize(principal, namespace, b"", Permission::Write)
        }
        Request::AcquireLease {
            namespace,
            resource,
            ..
        }
        | Request::RenewLease {
            namespace,
            resource,
            ..
        }
        | Request::ReleaseLease {
            namespace,
            resource,
            ..
        } => auth.authorize(principal, namespace, resource.as_bytes(), Permission::Write),
        Request::InspectLease {
            namespace,
            resource,
        }
        | Request::ValidateLease {
            namespace,
            resource,
            ..
        } => auth.authorize(principal, namespace, resource.as_bytes(), Permission::Read),
        Request::RegisterCollection { descriptor, .. } => {
            auth.authorize_admin(principal, Some(&descriptor.namespace))
        }
        Request::BeginMigration { namespace, .. }
        | Request::AdvanceMigrationVerification { namespace, .. }
        | Request::EnterReverseShadow { namespace, .. }
        | Request::RollbackMigration { namespace, .. }
        | Request::ContractMigration { namespace, .. }
        | Request::CommitMigration { namespace, .. }
        | Request::AbortMigration { namespace, .. }
        | Request::RevokeSearchPrefix { namespace, .. } => {
            auth.authorize_admin(principal, Some(namespace))
        }
        Request::NamespaceDigest {
            namespace,
            key_prefix,
            ..
        } => auth.authorize(principal, namespace, key_prefix, Permission::Read),
        Request::ApplyReplicatedDelta { delta, .. } => {
            auth.authorize(principal, &delta.namespace, &delta.key, Permission::Write)?;
            if let Some(idx) = &delta.indexes {
                authorize_index_mutations(auth, principal, &delta.namespace, idx)?;
            }
            if let Some(op) = &delta.search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::IndexDocument { doc, .. } => {
            auth.authorize(principal, &doc.namespace, doc.doc_id.as_bytes(), Permission::Write)
        }
        Request::DeleteSearchDocument {
            namespace, doc_id, ..
        } => auth.authorize(principal, namespace, doc_id.as_bytes(), Permission::Write),
        Request::Search(query) => {
            let ns = query.namespace.as_deref().unwrap_or("*");
            auth.authorize(principal, ns, b"", Permission::Read)
        }
        _ => Err(MembusError::Unauthorized(
            "unhandled request authorization".to_owned(),
        )),
    }
}
