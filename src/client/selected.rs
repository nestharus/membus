//! Additive selected-view operation; no new HELLO layout or fallback capture.
use super::*;
use crate::selected_view::{AcquisitionBudget, SelectedView, Selection};

impl<T: Transport> Client<T> {
    /// Acquire one bounded selection, then use scan_prefix/scan_range with the
    /// exact same bounds and snapshot_release. Unsupported peers refuse opcode
    /// 32. A lost/malformed reply can leave an unknown token until resident TTL;
    /// operation IDs do not deduplicate opens. Never retry automatically.
    pub fn snapshot_open_selected(
        &mut self,
        ns: &str,
        selection: Selection<'_>,
        budget: AcquisitionBudget,
        op: &str,
        d: Deadline,
    ) -> Result<SelectedView> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        identifier(op)?;
        identifier(ns)?;
        if !selection.valid() {
            return Err(Error::Invalid("selection bounds"));
        }
        let (kind, lower, upper) = match selection {
            Selection::Prefix(p) => (0, p, None),
            Selection::Range { lower, upper } => (1, lower, Some(upper)),
        };
        let payload_len = 8
            + 4
            + op.len()
            + 4
            + ns.len()
            + 1
            + 4
            + lower.len()
            + upper.map_or(0, |u| 4 + u.len())
            + 24;
        let mut bytes = crate::codec::frame_header(crate::SNAPSHOT_OPEN_SELECTED, 0, payload_len);
        bytes.extend(d.unix_ms.to_le_bytes());
        lp(&mut bytes, op.as_bytes());
        lp(&mut bytes, ns.as_bytes());
        bytes.push(kind);
        lp(&mut bytes, lower.as_bytes());
        if let Some(upper) = upper {
            lp(&mut bytes, upper.as_bytes());
        }
        bytes.extend(budget.max_candidates.to_le_bytes());
        bytes.extend(budget.max_metadata_bytes.to_le_bytes());
        bytes.extend(budget.max_retained_bytes.to_le_bytes());
        self.m3_frame_response(crate::SNAPSHOT_OPEN_SELECTED, bytes, d.local, |this, b| {
            let mut c = Cursor::new(&b);
            let view = SelectedView {
                snapshot: parse(c.bytes(16))?
                    .try_into()
                    .map_err(|_| Error::Protocol("selected token"))?,
                population: parse(c.u64())?,
                candidates: parse(c.u64())?,
                metadata_bytes: parse(c.u64())?,
                retained_bytes: parse(c.u64())?,
            };
            parse(c.end())?;
            let base = (256 + ns.len() + lower.len() + upper.map_or(0, str::len)) as u64;
            if u64::from_le_bytes(view.snapshot[..8].try_into().unwrap()) != this.negotiated.epoch
                || view.snapshot[8..] == [0; 8]
                || !view.candidates.is_multiple_of(2)
                || view.candidates > budget.max_candidates
                || view.metadata_bytes > budget.max_metadata_bytes
                || view.metadata_bytes
                    < view
                        .candidates
                        .checked_mul(128)
                        .ok_or(Error::Protocol("selected work"))?
                || view.population > view.candidates / 2
                || view.retained_bytes > budget.max_retained_bytes
                || view.retained_bytes
                    < base
                        .checked_add(
                            view.population
                                .checked_mul(129)
                                .ok_or(Error::Protocol("selected charge"))?,
                        )
                        .ok_or(Error::Protocol("selected charge"))?
            {
                return Err(Error::Protocol("selected acquisition"));
            }
            Ok(view)
        })
    }
}
