//! Participation boundary, not an authority issuer or a recovery promotion API.
//!
//! An external authority must outlive and be excluded from the selected Store rollback.
//! Its permit serializes replacement against the ENTIRE effect and durable settlement.
//! No implementation is inferred from a copied epoch, lease validation or image label.
use crate::{
    atomic::AtomicRequest,
    client::{Client, Deadline, Error, MutationReply, Transport},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub store_identity: String,
    pub authority_incarnation: String,
    pub generation: u64,
    /// Qualified selected complete history, including reconciliation AFTER capture.
    pub history_digest: [u8; 32],
    /// Exact externally admitted writer/effect-sink population, not a discovery claim.
    pub participants: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectContext {
    pub grant: Grant,
    pub participant: String,
    /// Immutable canonical original bytes; includes namespace, ID, epoch, horizon and Ack.
    pub original: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Settlement {
    Committed { version: u64, ack: u8 },
    Refused,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub context: EffectContext,
    pub settlement: Settlement,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorityError {
    Unsupported,
    Unavailable,
    Stale,
    Unsettled,
    Invalid,
}

/// Implementations must reject unknown participation and retain admitted uncertainty.
/// A live permit excludes grant replacement at every participating writer/effect sink.
/// Dropping it without settlement MUST retain UNSETTLED, including process/channel loss.
/// These semantics need a qualified independent owner; they cannot be supplied by the image.
pub trait EffectPermit {
    /// Persist/read back this exact outcome under the still-held effect fence.
    /// An error/lost reply leaves the original context unsettled; never reissue it as new.
    fn settle(self, outcome: Settlement) -> Result<Receipt, AuthorityError>;
}
pub trait EffectAuthority {
    type Permit: EffectPermit;
    /// Atomic current-grant check, original custody admission, and effect-fence acquisition.
    /// Returning a historical validation followed by an unlocked effect violates this contract.
    fn enter(&mut self, context: &EffectContext) -> Result<Self::Permit, AuthorityError>;
}
#[derive(Debug)]
pub enum ServingError {
    Authority(AuthorityError),
    Provider(Error),
    /// Provider may have effected the original, or an external receipt is missing/inexact.
    /// Preserve this exact original context and all known provider receipts.
    Unsettled {
        context: EffectContext,
        provider: Option<MutationReply>,
        cause: Option<Error>,
    },
}
/// One explicit participating atomic publisher. This grants no legacy/raw/timer/OS authority.
/// No ordinary resident activation or quarantine removal is available here.
pub struct AtomicPublisher<A: EffectAuthority> {
    authority: A,
    grant: Grant,
    participant: String,
}
impl<A: EffectAuthority> AtomicPublisher<A> {
    pub fn new(authority: A, grant: Grant, participant: String) -> Result<Self, AuthorityError> {
        let valid = |s: &str| !s.is_empty() && s.len() <= crate::MAX_ID && !s.contains('\0');
        if !valid(&grant.store_identity)
            || !valid(&grant.authority_incarnation)
            || grant.generation == 0
            || grant.history_digest == [0; 32]
            || grant.participants.is_empty()
            || grant.participants.len() > 128
            || !valid(&participant)
            || !grant.participants.iter().any(|p| p == &participant)
        {
            return Err(AuthorityError::Invalid);
        }
        let mut names = std::collections::HashSet::new();
        if grant
            .participants
            .iter()
            .any(|p| !valid(p) || !names.insert(p))
        {
            return Err(AuthorityError::Invalid);
        }
        Ok(Self {
            authority,
            grant,
            participant,
        })
    }
    #[allow(clippy::result_large_err)]
    pub fn publish<T: Transport>(
        &mut self,
        client: &mut Client<T>,
        original: &AtomicRequest,
        deadline: Deadline,
    ) -> Result<MutationReply, ServingError> {
        let context = EffectContext {
            grant: self.grant.clone(),
            participant: self.participant.clone(),
            original: original.to_bytes(),
        };
        let permit = self
            .authority
            .enter(&context)
            .map_err(ServingError::Authority)?;
        match client.publish_atomic(original, deadline) {
            Ok(reply) => {
                let outcome = Settlement::Committed {
                    version: reply.version,
                    ack: reply.achieved as u8,
                };
                match permit.settle(outcome.clone()) {
                    Ok(receipt) if receipt.context == context && receipt.settlement == outcome => {
                        Ok(reply)
                    }
                    _ => Err(ServingError::Unsettled {
                        context,
                        provider: Some(reply),
                        cause: None,
                    }),
                }
            }
            Err(error) => {
                // Only typed pre-admission refusals establish no NEW provider effect.
                // Prior admitted uncertainty belongs to external custody and may not be erased.
                let definitive = matches!(
                    &error,
                    Error::Invalid(_) | Error::Atomic(_) | Error::Refused(_)
                );
                if definitive {
                    if let Ok(receipt) = permit.settle(Settlement::Refused) {
                        if receipt.context == context && receipt.settlement == Settlement::Refused {
                            return Err(ServingError::Provider(error));
                        }
                    }
                } else {
                    drop(permit);
                }
                Err(ServingError::Unsettled {
                    context,
                    provider: None,
                    cause: Some(error),
                })
            }
        }
    }
}
