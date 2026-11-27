use crate::protocol::{Request, Response};
use crate::types::{Ack, LeaseGrant, LeaseInfo, MembusError};

use super::core::Client;

impl Client {
    pub async fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        let resp = self
            .call(Request::PutBlob {
                digest: digest.to_vec(),
                data: data.to_vec(),
                ack,
            })
            .await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_blob: {other:?}"
            ))),
        }
    }

    pub async fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        let resp = self
            .call(Request::GetBlob {
                digest: digest.to_vec(),
            })
            .await?;
        match resp {
            Response::Blob(b) => Ok(b),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_blob: {other:?}"
            ))),
        }
    }

    pub async fn put_cas_blob(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError> {
        let resp = self
            .call(Request::PutCasBlob {
                namespace: namespace.to_owned(),
                data: data.to_vec(),
                ack,
            })
            .await?;
        match resp {
            Response::CasBlobDigest(digest) => Ok(digest),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn get_cas_blob(
        &self,
        namespace: &str,
        sha256: [u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError> {
        let resp = self
            .call(Request::GetCasBlob {
                namespace: namespace.to_owned(),
                sha256,
            })
            .await?;
        match resp {
            Response::Blob(data) => Ok(data),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn has_cas_blob(
        &self,
        namespace: &str,
        sha256: [u8; 32],
    ) -> Result<bool, MembusError> {
        let resp = self
            .call(Request::HasCasBlob {
                namespace: namespace.to_owned(),
                sha256,
            })
            .await?;
        match resp {
            Response::Bool(exists) => Ok(exists),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for has_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn acquire_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        let resp = self
            .call(Request::AcquireLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                ttl_ms,
            })
            .await?;
        match resp {
            Response::Lease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for acquire_lease: {other:?}"
            ))),
        }
    }

    pub async fn renew_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        let resp = self
            .call(Request::RenewLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
                ttl_ms,
            })
            .await?;
        match resp {
            Response::Lease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for renew_lease: {other:?}"
            ))),
        }
    }

    pub async fn release_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<(), MembusError> {
        let resp = self
            .call(Request::ReleaseLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for release_lease: {other:?}"
            ))),
        }
    }

    pub async fn inspect_lease(
        &self,
        namespace: &str,
        resource: &str,
    ) -> Result<Option<LeaseInfo>, MembusError> {
        let resp = self
            .call(Request::InspectLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
            })
            .await?;
        match resp {
            Response::OptLease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for inspect_lease: {other:?}"
            ))),
        }
    }

    pub async fn validate_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<bool, MembusError> {
        let resp = self
            .call(Request::ValidateLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Bool(v) => Ok(v),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for validate_lease: {other:?}"
            ))),
        }
    }
}
