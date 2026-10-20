//! Health fields and resident heartbeat representation consumed by fleet health reporting.
use super::Health2;

impl Health2 {
    /// The key=value list HEALTH2 returns.
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut f: Vec<(String, String)> = vec![
            ("state_bytes".into(), self.state_bytes.to_string()),
            (
                "state_limit_bytes".into(),
                self.state_limit_bytes.to_string(),
            ),
            ("state_high_water".into(), self.state_high_water.to_string()),
            ("read_only".into(), self.read_only.to_string()),
            ("storage_error".into(), self.storage_error.to_string()),
            ("expiry_degraded".into(), self.expiry_degraded.to_string()),
            ("dedup_entries".into(), self.dedup_entries.to_string()),
            ("dedup_cap".into(), self.dedup_cap.to_string()),
            ("dedup_high_water".into(), self.dedup_high_water.to_string()),
            ("keys".into(), self.keys.to_string()),
            ("index_cap".into(), self.index_cap.to_string()),
            ("quota".into(), self.quota.to_string()),
            (
                "namespace_high_water".into(),
                self.namespace_high_water.to_string(),
            ),
            ("chunks_open".into(), self.chunks_open.to_string()),
            ("chunks_closed".into(), self.chunks_closed.to_string()),
            ("retained_bytes".into(), self.retained_total.to_string()),
            ("pinned_bytes".into(), self.pinned_bytes.to_string()),
            (
                "pinned_high_water".into(),
                self.pinned_high_water.to_string(),
            ),
            ("retired_chunks".into(), self.retired_chunks.to_string()),
            ("retired_bytes".into(), self.retired_bytes.to_string()),
            ("retire_blocked".into(), self.retire_blocked.to_string()),
            (
                "retire_blocked_oldest_ms".into(),
                self.retire_blocked_oldest_ms.to_string(),
            ),
            (
                "retire_blocked_high_water".into(),
                self.retire_blocked_high_water.to_string(),
            ),
        ];
        if let Some(error) = &self.catalogue_rewrite_error {
            f.push(("catalogue_rewrite_error".into(), error.clone()));
        }
        for (ns, n) in &self.namespaces {
            f.push((format!("namespace.{ns}"), n.to_string()));
        }
        for (class, b) in &self.retained_bytes {
            f.push((format!("retained_bytes.{class}"), b.to_string()));
        }
        f
    }
}
/// The resident heartbeat line (src/main.rs writes it every beat): the fields fleet's check reads, then the chunk
/// retirement flags `heartbeat_warn` may name and three counts (CHUNK-RETIREMENT.md section 6). The new flags are
/// written 0 or 1, which fleet reads as a count (above 0 flags), so a beat stays under 512 B.
#[allow(clippy::too_many_arguments)]
pub fn heartbeat_json(
    pid: u32,
    epoch: u64,
    unix_ms: u128,
    writes: u64,
    bytes: u64,
    attempts: u64,
    attempted: u64,
    failures: u64,
    h: &Health2,
) -> String {
    format!(
        "{{\"pid\":{pid},\"epoch\":{epoch},\"unix_ms\":{unix_ms},\"heartbeat_writes\":{writes},\"heartbeat_bytes\":{bytes},\"heartbeat_attempts\":{attempts},\"heartbeat_bytes_attempted\":{attempted},\"heartbeat_failures\":{failures},\"state_bytes\":{},\"state_limit_bytes\":{},\"state_high_water\":{},\"read_only\":{},\"storage_error\":{},\"expiry_degraded\":{},\"dedup_high_water\":{},\"namespace_high_water\":{},\"pinned_high_water\":{},\"retire_blocked_high_water\":{},\"dedup_entries\":{},\"retained_bytes\":{},\"retired_bytes\":{}}}\n",
        h.state_bytes,
        h.state_limit_bytes,
        h.state_high_water,
        h.read_only as u8,
        h.storage_error as u8,
        h.expiry_degraded as u8,
        h.dedup_high_water as u8,
        h.namespace_high_water as u8,
        h.pinned_high_water as u8,
        h.retire_blocked_high_water as u8,
        h.dedup_entries,
        h.retained_total,
        h.retired_bytes
    )
}
