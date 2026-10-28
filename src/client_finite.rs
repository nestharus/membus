//! Explicit finite-hint intent custody. No automatic retry or endpoint resolver.
use super::*;
use crate::subscription_contract::*;
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum HintCreationState {Unsent,Unknown{last_receipt:Option<HintReceipt>},Known(HintReceipt),Cancelled}
#[derive(Debug)]
pub struct HintAttempt {
    namespace:String,prefix:String,endpoint:String,options:HintOptions,epoch:u64,
    state:HintCreationState,
}
impl HintAttempt {
    pub fn state(&self)->HintCreationState {self.state}
    pub fn namespace(&self)->&str {&self.namespace}
    pub fn endpoint(&self)->&str {&self.endpoint}
    pub fn options(&self)->HintOptions {self.options}
}
impl<T:Transport> Client<T> {
    /// Require an exact affirmative profile response; never falls back to change/1.
    pub fn handshake_finite_hints(io:T,mut options:Options,additional:AdditionalFeatures,until:Instant)->Result<Self> {
        options.subscriptions=true;
        Self::handshake_inner(io,options,additional,until,2)
    }
    pub fn prepare_finite_hints(&self,namespace:&str,prefix:&str,endpoint:&str,options:HintOptions)->Result<HintAttempt> {
        if !self.finite_hints {return Err(Error::Invalid("finite hints not negotiated"));}
        identifier(namespace)?;identifier(endpoint)?;
        if prefix.len()>MAX_ID {return Err(Error::Invalid("prefix length"));}
        options.validate().map_err(|_|Error::Invalid("unsupported finite hint options"))?;
        Ok(HintAttempt{namespace:namespace.into(),prefix:prefix.into(),endpoint:endpoint.into(),options,epoch:self.negotiated.epoch,state:HintCreationState::Unsent})
    }
    /// Intent becomes Unknown before the first transport effect. A malformed or
    /// lost reply keeps it there; callers must retain this object, never recreate.
    pub fn create_finite_hints(&mut self,attempt:&mut HintAttempt,operation_id:&str,d:Deadline)->Result<HintReceipt> {
        if !self.finite_hints || attempt.state!=HintCreationState::Unsent {return Err(Error::Invalid("creation requires an unsent finite intent"));}
        self.check_hint_epoch(attempt)?;
        let mut p=self.prefix(operation_id,&attempt.namespace,d)?;
        p.extend(attempt.options.profile.to_le_bytes());p.extend(attempt.options.lifetime_ms.to_le_bytes());p.extend(attempt.options.write_ms.to_le_bytes());
        lp(&mut p,&attempt.options.incarnation);lp(&mut p,&attempt.options.recipient);lp(&mut p,attempt.prefix.as_bytes());lp(&mut p,attempt.endpoint.as_bytes());
        let id=self.begin_exchange(p.len(),d.local)?;let bytes=frame(FINITE_CREATE,id,&p);
        attempt.state=HintCreationState::Unknown{last_receipt:None};
        let body=match self.exchange_bytes(FINITE_CREATE,id,&bytes,d.local) {
            Ok(body)=>body,
            Err(Error::Refused(refusal)) if [2,3,4,5,6,7].contains(&refusal.code)=>{attempt.state=HintCreationState::Unsent;return Err(Error::Refused(refusal));}
            Err(e)=>return Err(e),
        };
        let decoded=HintReceipt::decode(&body).map_err(|_|Error::Protocol("finite creation receipt"));
        let result=decoded.and_then(|r|{
            if r.binding.epoch!=attempt.epoch || r.binding.incarnation!=attempt.options.incarnation || r.binding.recipient!=attempt.options.recipient || r.generation!=1 || r.authority_until_ms==0 {return Err(Error::Protocol("finite creation binding"));}Ok(r)
        });
        match result {Ok(r)=>{attempt.state=HintCreationState::Known(r);Ok(r)},Err(e)=>{self.usable=false;Err(e)}}
    }
    fn check_hint_epoch(&self,attempt:&HintAttempt)->Result<()> {
        if self.negotiated.epoch!=attempt.epoch {return Err(Error::EpochChanged{expected:attempt.epoch,actual:self.negotiated.epoch});}Ok(())
    }
    pub fn renew_finite_hints(&mut self,attempt:&mut HintAttempt,operation_id:&str,d:Deadline)->Result<HintReceipt> {
        self.control_finite_hints(attempt,operation_id,d,false)?.ok_or(Error::Protocol("missing renewal receipt"))
    }
    pub fn cancel_finite_hints(&mut self,attempt:&mut HintAttempt,operation_id:&str,d:Deadline)->Result<()> {
        self.control_finite_hints(attempt,operation_id,d,true).map(|_|())
    }
    fn control_finite_hints(&mut self,attempt:&mut HintAttempt,operation_id:&str,d:Deadline,cancel:bool)->Result<Option<HintReceipt>> {
        if !self.finite_hints {return Err(Error::Invalid("finite hints not negotiated"));}self.check_hint_epoch(attempt)?;
        let HintCreationState::Known(previous)=attempt.state else{return Err(Error::Invalid("control requires exact known receipt"));};
        let mut p=self.prefix(operation_id,&attempt.namespace,d)?;lp(&mut p,&previous.bytes());
        let op=if cancel {FINITE_CANCEL}else{FINITE_RENEW};let id=self.begin_exchange(p.len(),d.local)?;let bytes=frame(op,id,&p);
        attempt.state=HintCreationState::Unknown{last_receipt:Some(previous)};
        let body=match self.exchange_bytes(op,id,&bytes,d.local) {
            Ok(b)=>b,Err(Error::Refused(r))=>{attempt.state=HintCreationState::Known(previous);return Err(Error::Refused(r));},Err(e)=>return Err(e)
        };
        let result=if cancel {
            if body.is_empty(){Ok(None)}else{Err(Error::Protocol("finite cancel reply"))}
        }else{
            HintReceipt::decode(&body).map_err(|_|Error::Protocol("finite renewal receipt")).and_then(|r|{
                if r.binding!=previous.binding || previous.generation.checked_add(1)!=Some(r.generation) || r.authority_until_ms<previous.authority_until_ms {return Err(Error::Protocol("finite renewal binding"));}Ok(Some(r))
            })
        };
        match result {Ok(Some(r))=>{attempt.state=HintCreationState::Known(r);Ok(Some(r))},Ok(None)=>{attempt.state=HintCreationState::Cancelled;Ok(None)},Err(e)=>{self.usable=false;Err(e)}}
    }
}
