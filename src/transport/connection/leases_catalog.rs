use std::sync::Arc;

use crate::catalog::CollectionCatalog;
use crate::protocol::{Request, Response};
use crate::transport::connection::state::ConnectionState;
use crate::types::{AuthMode, CollectionDescriptor, MembusError, Permission, Principal};

pub(super) async fn dispatch_lease(conn: &Arc<ConnectionState>, req: Request) -> Response {
    match req {
        Request::AcquireLease {
            namespace,
            resource,
            holder,
            ttl_ms,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.acquire_lease(&namespace, &resource, &holder, ttl_ms)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("acquire_lease failed: {e}"))));
            match result {
                Ok(grant) => Response::Lease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::RenewLease {
            namespace,
            resource,
            holder,
            generation,
            token,
            ttl_ms,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.renew_lease(&namespace, &resource, &holder, generation, &token, ttl_ms)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("renew_lease failed: {e}"))));
            match result {
                Ok(grant) => Response::Lease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::ReleaseLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.release_lease(&namespace, &resource, &holder, generation, &token)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("release_lease failed: {e}"))));
            match result {
                Ok(()) => Response::Unit,
                Err(e) => Response::Error(e),
            }
        }
        Request::InspectLease {
            namespace,
            resource,
        } => {
            let leases = Arc::clone(&conn.ctx.leases);
            let result =
                tokio::task::spawn_blocking(move || leases.inspect_lease(&namespace, &resource))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("inspect_lease failed: {e}")))
                    });
            match result {
                Ok(grant) => Response::OptLease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::ValidateLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                leases.validate_lease(&namespace, &resource, &holder, generation, &token)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("validate_lease failed: {e}"))));
            match result {
                Ok(valid) => Response::Bool(valid),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in lease dispatch".to_owned(),
        )),
    }
}

async fn run_catalog_write<F>(
    conn: &Arc<ConnectionState>,
    op_name: &'static str,
    f: F,
) -> Response
where
    F: FnOnce(&CollectionCatalog) -> Result<CollectionDescriptor, MembusError> + Send + 'static,
{
    let ctx = conn.ctx.clone();
    let catalog = Arc::clone(&conn.ctx.catalog);
    let result = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(None)?;
        f(catalog.as_ref())
    })
    .await
    .unwrap_or_else(|e| Err(MembusError::Storage(format!("{op_name} failed: {e}"))));
    match result {
        Ok(desc) => Response::Collection(desc),
        Err(e) => Response::Error(e),
    }
}

pub(super) async fn dispatch_catalog(
    conn: &Arc<ConnectionState>,
    principal: Option<&Principal>,
    req: Request,
) -> Response {
    match req {
        Request::RegisterCollection {
            expected_version,
            descriptor,
        } => {
            run_catalog_write(conn, "register_collection", move |catalog| {
                catalog.register_collection(expected_version, descriptor)
            })
            .await
        }
        Request::GetCollection { namespace } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || catalog.get_collection(&namespace))
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!("get_collection failed: {e}")))
                });
            match result {
                Ok(desc) => Response::OptCollection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::ListCollections => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || catalog.list_collections())
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!(
                        "list_collections failed: {e}"
                    )))
                });
            match result {
                Ok(mut list) => {
                    if conn.ctx.auth.mode() == AuthMode::Enforced {
                        list.retain(|desc| {
                            conn.ctx
                                .auth
                                .authorize(principal, &desc.namespace, b"", Permission::Read)
                                .is_ok()
                        });
                    }
                    Response::Collections(list)
                }
                Err(e) => Response::Error(e),
            }
        }
        Request::GetCollectionVersion { namespace, version } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                catalog.get_collection_version(&namespace, version)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "get_collection_version failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::OptCollection(desc),
                Err(e) => Response::Error(e),
            }
        }
        _ => dispatch_migration(conn, req).await,
    }
}

async fn dispatch_migration(conn: &Arc<ConnectionState>, req: Request) -> Response {
    match req {
        Request::BeginMigration {
            namespace,
            expected_version,
            target_version,
            target_sha256,
            target_schema_bytes,
        } => {
            run_catalog_write(conn, "begin_migration", move |catalog| {
                catalog.begin_migration(
                    &namespace,
                    expected_version,
                    target_version,
                    target_sha256,
                    target_schema_bytes,
                )
            })
            .await
        }
        Request::CommitMigration {
            namespace,
            target_version,
            verified_through,
        } => {
            run_catalog_write(conn, "commit_migration", move |catalog| {
                catalog.commit_migration(&namespace, target_version, verified_through)
            })
            .await
        }
        Request::AbortMigration {
            namespace,
            target_version,
        } => {
            run_catalog_write(conn, "abort_migration", move |catalog| {
                catalog.abort_migration(&namespace, target_version)
            })
            .await
        }
        Request::AdvanceMigrationVerification {
            namespace,
            target_version,
            verified_through,
        } => {
            run_catalog_write(conn, "advance_migration_verification", move |catalog| {
                catalog.advance_migration_verification(&namespace, target_version, verified_through)
            })
            .await
        }
        Request::EnterReverseShadow {
            namespace,
            target_version,
            verified_through,
        } => {
            run_catalog_write(conn, "enter_reverse_shadow", move |catalog| {
                catalog.enter_reverse_shadow(&namespace, target_version, verified_through)
            })
            .await
        }
        Request::RollbackMigration {
            namespace,
            rollback_version,
        } => {
            run_catalog_write(conn, "rollback_migration", move |catalog| {
                catalog.rollback_migration(&namespace, rollback_version)
            })
            .await
        }
        Request::ContractMigration {
            namespace,
            expected_version,
        } => {
            run_catalog_write(conn, "contract_migration", move |catalog| {
                catalog.contract_migration(&namespace, expected_version)
            })
            .await
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in catalog dispatch".to_owned(),
        )),
    }
}
