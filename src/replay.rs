//! Replay, legacy import and ordered startup recovery before runtime assembly.
use crate::chunks;
use crate::codec::crc32;
use crate::codec::mutation_bytes;
use crate::codec::parse_record;
use crate::codec::MAX_ID;
use crate::log_io::sync_file;
use crate::now_ms;
use crate::routing::stripe;
use crate::store::dedup_identity;
use crate::store::fingerprint;
use crate::store::Dedup;
use crate::store::Flags;
use crate::store::Key;
use crate::store::Meta;
use crate::store::State;
use crate::test_seam::TestSeam;
use crate::timers;
use crate::Class;
use crate::Config;
use crate::Mutation;
use crate::Outcome;
use crate::POLICY_NS;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn recovery_sync(path: &Path, config: &Config) -> io::Result<()> {
    if config.fail_recovery_sync {
        return Err(io::Error::other(format!(
            "test injected recovery sync failure: {}",
            path.display()
        )));
    }
    OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| sync_file(&file))
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("recovery data sync {}: {e}", path.display()),
            )
        })
}

fn corrupt(text: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text)
}
fn recovery_sync_dir(path: &Path, config: &Config) -> io::Result<()> {
    #[cfg(test)]
    if let Some(hook) = &config.recovery_dir_sync { hook(path)?; }
    #[cfg(not(test))]
    let _ = config;
    crate::log_io::sync_dir(path).map_err(|e| io::Error::new(e.kind(),
        format!("recovery directory sync {}: {e}", path.display())))
}
/// What replay rebuilt, before any file is changed.
struct Replayed {
    committed: HashMap<(String, String), Arc<crate::value_disk::Entry>>,
    chunk_of: HashMap<(String, String), u64>,
    dedup: HashMap<(String, String), Dedup>,
    next: u64,
    persisted: u64,
    chunks: chunks::ChunkStore,
    truncate: Vec<(PathBuf, u64)>,
    report: chunks::ReplayReport,
    storage_errors: Vec<String>,
    cat_last_ms: u64,
}
impl Replayed {
    fn apply(&mut self, m: Mutation, v: u64, at: u64, chunk: u64, now: u64, config: &Config, location: crate::value_disk::Location) {
        self.next = self.next.max(v + 1);
        let k = (m.ns.clone(), m.key.clone());
        if self.committed.get(&k).is_none_or(|old| old.version < v) {
            self.committed.insert(
                k.clone(),
                Arc::new(crate::value_disk::Entry::new(&m, v, location)),
            );
            self.chunk_of.insert(k, chunk);
        }
        if now.saturating_sub(at) <= config.retry_ms && !timers::is_progress(&m.ns, &m.key) {
            self.dedup.insert(
                dedup_identity(&m),
                Dedup {
                    fingerprint: fingerprint(&m),
                    at,
                    outcome: Some(Outcome {
                        code: 0,
                        version: v,
                        ack: m.ack,
                    }),
                },
            );
        }
    }
    /// Replay one log file. `open_at_crash` allows a torn tail to be truncated; otherwise it refuses start.
    #[expect(
        clippy::too_many_arguments,
        reason = "Preserve the established file replay boundary."
    )]
    fn replay_file(
        &mut self,
        p: &Path,
        si: usize,
        chunk: u64,
        open_at_crash: bool,
        horizons: &HashMap<(String, String), u64>,
        config: &Config,
        now: u64,
    ) -> io::Result<u64> {
        let mut f = File::open(p)?;
        let reader = Arc::new(std::sync::Mutex::new(File::open(p)?));
        let len = f.metadata()?.len();
        let name = p.display().to_string();
        let mut offset = 0u64;
        while offset < len {
            match chunks::read_frame_at(&mut f, b"MBK1", offset, len, &name)? {
                chunks::Frame::Torn(reason) => {
                    if !open_at_crash {
                        return Err(corrupt(format!(
                            "interior-corrupt {name} offset {offset}: torn tail in a closed chunk"
                        )));
                    }
                    self.truncate.push((p.to_owned(), offset));
                    self.report.stripes[si].truncations.push((
                        name.clone(),
                        offset,
                        len - offset,
                        reason.into(),
                    ));
                    break;
                }
                chunks::Frame::Whole(b, n) => {
                    let (m, v, at) = parse_record(&b).map_err(|_| {
                        corrupt(format!("interior-corrupt {name} offset {offset}: payload"))
                    })?;
                    if stripe(&m.ns, &m.key) != si {
                        return Err(corrupt(format!(
                            "interior-corrupt {name} offset {offset}: wrong stripe"
                        )));
                    }
                    let k = (m.ns.clone(), m.key.clone());
                    self.chunks.register(chunk, &k, v);
                    self.report.stripes[si].records += 1;
                    self.report.stripes[si].bytes += n;
                    if horizons.get(&k).is_some_and(|h| v <= *h) {
                        // Anti-resurrection: a retired newer version's horizon hides this older one.
                        self.report.stripes[si].horizons_applied += 1;
                        self.next = self.next.max(v + 1);
                    } else {
                        self.apply(m, v, at, chunk, now, config, crate::value_disk::Location {
                            class: self.chunks.table.get(&chunk).map_or(Class::FOREVER, |c| c.class),
                            file: reader.clone(), offset, record_len: n,
                        });
                    }
                    offset += n;
                }
            }
        }
        Ok(offset)
    }
}
/// The legacy stripe logs a folder holds: the state root before the import, legacy-<epoch>/ after it.
fn legacy_files(dir: &Path) -> Vec<(usize, PathBuf)> {
    (0..4)
        .map(|i| (i, dir.join(format!("stripe-{i}.log"))))
        .filter(|(_, p)| p.exists())
        .collect()
}
/// What the legacy logs hold: latest record locations and the dedup window, with no retained payloads.
struct Legacy {
    committed: HashMap<Key, Arc<crate::value_disk::Entry>>,
    dedup: HashMap<Key, Dedup>,
    next: u64,
    /// Each key's latest record: version, stripe, location.
    latest: HashMap<Key, (u64, usize, crate::value_disk::Location)>,
    /// Torn tails left in place: file, offset, bytes.
    torn: Vec<(String, u64, u64)>,
    records: u64,
}
fn merge_legacy_dedup(
    target: &mut HashMap<Key, Dedup>,
    legacy: HashMap<Key, Dedup>,
) -> io::Result<()> {
    for (k, d) in legacy {
        match target.entry(k) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(d);
            }
            std::collections::hash_map::Entry::Occupied(e) => {
                let current = e.get();
                if current.fingerprint != d.fingerprint
                    || current.outcome != d.outcome
                    || current.at != d.at
                {
                    // Origin cannot be inferred from an ID prefix or replay order. Neither
                    // the backup nor a possibly later record may silently replace the other.
                    return Err(corrupt("legacy retry identity conflicts with imported or later record; original files retained".into()));
                }
            }
        }
    }
    Ok(())
}
fn read_legacy(files: &[(usize, PathBuf)], config: &Config, now: u64) -> io::Result<Legacy> {
    let mut l = Legacy {
        committed: HashMap::new(),
        dedup: HashMap::new(),
        next: 1,
        latest: HashMap::new(),
        torn: Vec::new(),
        records: 0,
    };
    for (si, p) in files {
        let mut f = File::open(p)?;
        let reader = Arc::new(std::sync::Mutex::new(File::open(p)?));
        let len = f.metadata()?.len();
        let name = p.display().to_string();
        let mut offset = 0u64;
        while offset < len {
            match chunks::read_frame_at(&mut f, b"MBK1", offset, len, &name)? {
                chunks::Frame::Torn(_) => {
                    l.torn.push((name.clone(), offset, len - offset));
                    break;
                }
                chunks::Frame::Whole(b, n) => {
                    let (m, v, at) = parse_record(&b).map_err(|_| {
                        corrupt(format!("interior-corrupt {name} offset {offset}: payload"))
                    })?;
                    if stripe(&m.ns, &m.key) != *si {
                        return Err(corrupt(format!(
                            "interior-corrupt {name} offset {offset}: wrong stripe"
                        )));
                    }
                    l.next = l.next.max(v + 1);
                    let k = (m.ns.clone(), m.key.clone());
                    if l.committed.get(&k).is_none_or(|old| old.version < v) {
                        let location = crate::value_disk::Location {
                            class: Class::FOREVER, file: reader.clone(), offset, record_len: n,
                        };
                        l.committed.insert(
                            k.clone(),
                            Arc::new(crate::value_disk::Entry::new(&m, v, location.clone())),
                        );
                        l.latest.insert(k, (v, *si, location));
                    }
                    if now.saturating_sub(at) <= config.retry_ms
                        && !timers::is_progress(&m.ns, &m.key)
                    {
                        l.dedup.insert(
                            dedup_identity(&m),
                            Dedup {
                                fingerprint: fingerprint(&m),
                                at,
                                outcome: Some(Outcome {
                                    code: 0,
                                    version: v,
                                    ack: m.ack,
                                }),
                            },
                        );
                    }
                    l.records += 1;
                    offset += n;
                }
            }
        }
    }
    Ok(l)
}
/// The policies an import applies before it writes: `state/import-policies.txt`, one `NS<TAB>PREFIX<TAB>CLASS` a
/// line (CLASS forever, latest or expire:MS; PREFIX may be empty); `#` starts a comment line.
fn import_policies(bytes: &[u8], config: &Config) -> io::Result<Vec<(String, String, Class)>> {
    let text =
        std::str::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut out = Vec::new();
    let mut check = chunks::Policies::default();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let bad = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("import-policies.txt line {}: {line}", n + 1),
            )
        };
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 3 {
            return Err(bad());
        }
        let class = match parts[2] {
            "forever" => Class::FOREVER,
            "latest" => Class::LATEST,
            c => Class::expire(
                c.strip_prefix("expire:")
                    .and_then(|ms| ms.parse().ok())
                    .filter(|ms| *ms >= config.retry_ms)
                    .ok_or_else(bad)?,
            ),
        };
        chunks::policy_key(parts[0], parts[1]).map_err(|_| bad())?;
        if check.would_exceed(parts[0], parts[1], class) {
            return Err(bad());
        }
        check.set(parts[0], parts[1], Some(class));
        out.push((parts[0].to_owned(), parts[1].to_owned(), class));
    }
    Ok(out)
}
/// Writes the import's chunk files and catalogue records into its own folder.
struct ImportWriter {
    cs: chunks::ChunkStore,
    open: HashMap<(usize, Class), (u64, File, u64)>,
    chunk_bytes: u64,
    utc: u64,
    next: u64,
    written: u64,
}
impl ImportWriter {
    fn write(
        &mut self,
        stripe: usize,
        class: Class,
        policy: &str,
        payload: &[u8],
    ) -> io::Result<()> {
        let mut record = b"MBK1".to_vec();
        record.extend((payload.len() as u32).to_le_bytes());
        record.extend((!(payload.len() as u32)).to_le_bytes());
        record.extend(crc32(payload).to_le_bytes());
        record.extend(payload);
        if self
            .open
            .get(&(stripe, class))
            .is_some_and(|(_, _, b)| *b > 0 && b + record.len() as u64 > self.chunk_bytes)
        {
            self.close(stripe, class)?;
        }
        if !self.open.contains_key(&(stripe, class)) {
            let id = self.cs.next_id;
            self.cs.next_id += 1;
            let path = chunks::chunk_path(&self.cs.dir, class, stripe, id);
            self.cs.log(
                self.utc,
                chunks::Rec::Open {
                    seq: id,
                    stripe: stripe as u8,
                    class,
                    policy: policy.to_owned(),
                    next: self.next,
                },
            )?;
            fs::create_dir_all(path.parent().unwrap())?;
            let file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)?;
            self.open.insert((stripe, class), (id, file, 0));
        }
        let (_, file, bytes) = self.open.get_mut(&(stripe, class)).unwrap();
        file.write_all(&record)?;
        *bytes += record.len() as u64;
        self.written += record.len() as u64;
        Ok(())
    }
    fn close(&mut self, stripe: usize, class: Class) -> io::Result<()> {
        if let Some((id, file, bytes)) = self.open.remove(&(stripe, class)) {
            sync_file(&file)?;
            self.cs.log(
                self.utc,
                chunks::Rec::Close {
                    seq: id,
                    bytes,
                    next: self.next,
                },
            )?;
        }
        Ok(())
    }
    fn close_all(&mut self) -> io::Result<()> {
        let open: Vec<(usize, Class)> = self.open.keys().copied().collect();
        for (stripe, class) in open {
            self.close(stripe, class)?;
        }
        Ok(())
    }
}
/// Move the legacy logs into legacy-<epoch>/, unmodified, and set them read-only (5.6, step 1). Run again after a
/// stop between the switch and the move.
fn move_legacy(path: &Path, import: &chunks::Import, config: &Config) -> io::Result<()> {
    let to = path.join(&import.legacy_dir);
    fs::create_dir_all(&to)?;
    for (name, size, sha) in &import.files {
        let from = path.join(name);
        if from.exists() {
            if to.join(name).exists() {
                return Err(corrupt(format!(
                    "import input at source and backup; both retained: {name}"
                )));
            }
            if fs::metadata(&from)?.len() != *size || chunks::sha256_file(&from)? != *sha {
                return Err(corrupt(format!(
                    "import input changed before backup move; source retained: {name}"
                )));
            }
            fs::rename(&from, to.join(name))?;
        }
        let dest = to.join(name);
        if dest.exists() {
            // Persist the destination before the source removal. Retry these barriers even
            // when an earlier attempt already renamed the input but failed before completion.
            recovery_sync_dir(&to, config)?;
            recovery_sync_dir(path, config)?;
            let mut permissions = fs::metadata(&dest)?.permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&dest, permissions)?;
        }
    }
    Ok(())
}
/// The one-time import (CHUNK-RETIREMENT.md 5.6): each key's latest legacy record, at its version and time, into
/// chunks by class under chunks.import/, checked against the legacy replay before the switch. None when there is
/// nothing to import; Err (legacy logs untouched, chunks.import/ removed) on any difference.
fn import_legacy(
    path: &Path,
    config: &Config,
    clock: &chunks::Clock,
    epoch: u64,
) -> io::Result<Option<(chunks::Import, String)>> {
    use chunks::Rec;
    let files = legacy_files(path);
    if files.is_empty() {
        return Ok(None);
    }
    let chunks_dir = path.join("chunks");
    let catalogue = chunks_dir.join("catalogue.log");
    let records = if catalogue.exists() {
        chunks::read_catalogue(&catalogue)?.0
    } else {
        Vec::new()
    };
    // Imported already (a stop before the move is finished at replay), or chunks written beside the legacy logs
    // (they stay one forever chunk, as CR1 reads them).
    if records
        .iter()
        .any(|r| matches!(r.rec, Rec::Import(_) | Rec::Open { .. }))
    {
        return Ok(None);
    }
    if chunks_dir.exists() {
        // Missing or torn catalogue evidence does not establish that existing chunk files are empty.
        // A fresh directory (optionally with an empty catalogue) is the only safe import destination.
        for entry in fs::read_dir(&chunks_dir)? {
            let entry = entry?;
            if entry.file_name() != "catalogue.log"
                || !entry.file_type()?.is_file()
                || entry.metadata()?.len() != 0
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    "legacy import refused: existing chunks have no open or import catalogue record"));
            }
        }
    }
    let utc = clock.utc_ms();
    let mut listed = Vec::new();
    for (_, p) in &files {
        // The import can later graft original retry outcomes from its immutable
        // backup. Cover those source records before publishing/making it read-only.
        recovery_sync(p, config)?;
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        listed.push((name, fs::metadata(p)?.len(), chunks::sha256_file(p)?));
    }
    // The same bytes select classes and supply their Import.files provenance. On
    // Windows keep the input open without write/delete sharing through the switch;
    // incompatible outstanding writers refuse before constructing the replacement.
    let policy_path = path.join("import-policies.txt");
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(1); // FILE_SHARE_READ only
    }
    let mut policy_input = match options.open(&policy_path) {
        Ok(file) => Some(file),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let mut policy_bytes = Vec::new();
    if let Some(file) = &mut policy_input {
        file.read_to_end(&mut policy_bytes)?;
        let mut hash = chunks::Sha256::new();
        hash.update(&policy_bytes);
        listed.push((
            "import-policies.txt".into(),
            policy_bytes.len() as u64,
            hash.hex(),
        ));
    }
    let selected_policies = import_policies(&policy_bytes, config)?;
    for (name, _, _) in &listed {
        if path
            .join(format!("legacy-{epoch}"))
            .join(name)
            .try_exists()?
        {
            return Err(corrupt(format!(
                "import backup input already exists; source retained: {name}"
            )));
        }
    }
    let legacy = read_legacy(&files, config, now_ms())?;
    let dir = path.join("chunks.import");
    if dir.exists() {
        // An import cut short starts again from the untouched legacy logs.
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let built = (|| -> io::Result<(chunks::Import, u64)> {
        let mut cs = chunks::ChunkStore::new(dir.clone());
        cs.cat = Some(chunks::Catalogue::open(&dir.join("catalogue.log"))?);
        let mut w = ImportWriter {
            cs,
            open: HashMap::new(),
            chunk_bytes: config.chunk_bytes,
            utc,
            next: legacy.next,
            written: 0,
        };
        let mut policies = chunks::Policies::default();
        let mut used_ids: HashSet<Key> = legacy.dedup.keys().cloned().collect();
        for (_, _, location) in legacy.latest.values() {
            let payload = location.payload()?;
            let (m, _, _) =
                parse_record(&payload).map_err(|_| corrupt("legacy import record".into()))?;
            used_ids.insert(dedup_identity(&m));
        }
        let mut synthetic = HashMap::new();
        // The policies first, as policy records at new versions, so each imported key takes its class.
        for (n, (ns, prefix, class)) in selected_policies.into_iter().enumerate() {
            policies.set(&ns, &prefix, Some(class));
            let key =
                chunks::policy_key(&ns, &prefix).map_err(|_| io::Error::other("policy key"))?;
            let mut salt = 0u64;
            let op_id = loop {
                let candidate = if salt == 0 {
                    format!("import-policy/{epoch}/{n}")
                } else {
                    format!("import-policy/{epoch}/{n}/{salt}")
                };
                if candidate.len() > MAX_ID {
                    return Err(corrupt("import identity too large".into()));
                }
                if used_ids.insert((ns.clone(), candidate.clone())) {
                    break candidate;
                }
                salt = salt
                    .checked_add(1)
                    .ok_or_else(|| corrupt("import identity exhausted".into()))?;
            };
            let m = Mutation {
                ns: POLICY_NS.into(),
                key: key.clone(),
                op_id,
                deadline: 0,
                ack: 2,
                expected: None,
                bytes: chunks::policy_value(class),
                remove: false,
            };
            let version = w.next;
            w.next += 1;
            synthetic.insert(
                dedup_identity(&m),
                Dedup {
                    fingerprint: fingerprint(&m),
                    at: utc,
                    outcome: Some(Outcome {
                        code: 0,
                        version,
                        ack: 2,
                    }),
                },
            );
            let (c, label) = policies.resolve(POLICY_NS, &key);
            w.write(
                stripe(POLICY_NS, &key),
                c,
                &label,
                &mutation_bytes(&m, version, utc),
            )?;
        }
        let mut latest: Vec<_> = legacy.latest.iter().collect();
        latest.sort_by_key(|(_, (v, _, _))| *v);
        let mut leave_out = config.import_inject_difference;
        for ((ns, key), (_, si, location)) in latest {
            if std::mem::take(&mut leave_out) {
                continue;
            }
            let (class, label) = policies.resolve(ns, key);
            w.write(*si, class, &label, &location.payload()?)?;
        }
        w.close_all()?;
        // Compare the two indexed replays one value at a time, before publishing the import.
        let mut rp = Replayed::empty(dir.clone());
        replay_into(&mut rp, path, &dir, false, config, clock)?;
        let mut first: Option<String> = None;
        for (k, v) in &legacy.committed {
            match rp.committed.get(k) {
                Some(x)
                    if x.version == v.version
                        && x.read()?.bytes == v.read()?.bytes
                        && x.tombstone == v.tombstone
                        && x.ack == v.ack => {}
                Some(x) => {
                    first.get_or_insert(format!(
                        "{}/{}: imported version {} tombstone {} {} B, legacy version {} tombstone {} {} B",
                        k.0, k.1, x.version, x.tombstone, x.len, v.version, v.tombstone, v.len
                    ));
                }
                None => {
                    first.get_or_insert(format!(
                        "{}/{}: missing from the import (legacy version {})",
                        k.0, k.1, v.version
                    ));
                }
            }
        }
        for k in rp.committed.keys() {
            if k.0 != POLICY_NS && !legacy.committed.contains_key(k) {
                first.get_or_insert(format!("{}/{}: in the import only", k.0, k.1));
            }
        }
        let live = |c: &HashMap<Key, Arc<crate::value_disk::Entry>>| {
            let mut counts = std::collections::BTreeMap::new();
            for (k, v) in c {
                if !v.tombstone && k.0 != POLICY_NS {
                    *counts.entry(k.0.clone()).or_insert(0usize) += 1;
                }
            }
            counts
        };
        let (a, b) = (live(&legacy.committed), live(&rp.committed));
        if a != b {
            first.get_or_insert(format!(
                "namespace live counts: legacy {a:?}, imported {b:?}"
            ));
        }
        if rp.next != w.next || w.next < legacy.next {
            first.get_or_insert(format!(
                "next: imported {}, legacy {}, written {}",
                rp.next, legacy.next, w.next
            ));
        }
        for (k, d) in &rp.dedup {
            if synthetic.get(k).is_some_and(|s| {
                s.fingerprint == d.fingerprint && s.outcome == d.outcome && s.at == d.at
            }) && !legacy.dedup.contains_key(k)
            {
                continue;
            }
            match legacy.dedup.get(k) {
                Some(l)
                    if l.fingerprint == d.fingerprint && l.outcome == d.outcome && l.at == d.at => {
                }
                _ => {
                    first.get_or_insert(format!(
                        "dedup {}/{}: differs from the legacy replay",
                        k.0, k.1
                    ));
                }
            }
        }
        // Latest-value copying omits superseded operations. Check the complete retry
        // projection with the unchanged backup that startup will actually graft.
        merge_legacy_dedup(&mut rp.dedup, legacy.dedup.clone())?;
        for (k, d) in &legacy.dedup {
            if !rp.dedup.get(k).is_some_and(|x| {
                x.fingerprint == d.fingerprint && x.outcome == d.outcome && x.at == d.at
            }) {
                first.get_or_insert(format!("legacy retry {}/{} missing or changed", k.0, k.1));
            }
        }
        if let Some(e) = rp.storage_errors.first() {
            first.get_or_insert(format!("storage error in the import: {e}"));
        }
        if let Some(f) = first {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("import refused, legacy logs left in place: {f}"),
            ));
        }
        // In particular, a newly supplied file cannot silently turn an originally
        // absent input into an unrecorded policy choice during construction.
        if policy_input.is_none() && policy_path.try_exists()? {
            return Err(corrupt(
                "import policy input appeared during construction; source retained".into(),
            ));
        }
        let import = chunks::Import {
            files: listed,
            keys: legacy.latest.len() as u64,
            bytes: w.written,
            epoch,
            legacy_dir: format!("legacy-{epoch}"),
        };
        w.cs.log(utc, Rec::Import(import.clone()))?;
        Ok((import, w.written))
    })();
    let (import, written) = match built {
        Ok(x) => x,
        Err(e) => {
            let _ = fs::remove_dir_all(&dir);
            return Err(e);
        }
    };
    // The switch: chunks.import/ becomes chunks/; then the legacy logs move, unmodified and read-only.
    if chunks_dir.exists() {
        fs::rename(&chunks_dir, path.join(format!("chunks.pre-import-{epoch}")))?;
        recovery_sync_dir(path, config)?;
    }
    fs::rename(&dir, &chunks_dir)?;
    recovery_sync_dir(path, config)?;
    // Release the Windows source guard only after the switch. Backup movement
    // checks the named bytes again; arbitrary filesystem writers are not fenced
    // by the cooperative state-directory owner lock.
    drop(policy_input);
    move_legacy(path, &import, config)?;
    let torn: Vec<String> = legacy
        .torn
        .iter()
        .map(|(f, o, b)| format!("{f} offset {o} ({b} B torn tail, left in place)"))
        .collect();
    let line = format!(
        "imported {} keys ({written} B) of {} legacy records from {} legacy logs into chunks/; legacy logs in {}{}",
        import.keys,
        legacy.records,
        files.len(),
        import.legacy_dir,
        if torn.is_empty() { String::new() } else { format!("; {}", torn.join("; ")) }
    );
    Ok(Some((import, line)))
}
impl Replayed {
    fn empty(dir: PathBuf) -> Self {
        Replayed {
            committed: HashMap::new(),
            chunk_of: HashMap::new(),
            dedup: HashMap::new(),
            next: 1,
            persisted: 0,
            chunks: chunks::ChunkStore::new(dir),
            truncate: Vec::new(),
            report: chunks::ReplayReport::default(),
            storage_errors: Vec::new(),
            cat_last_ms: 0,
        }
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Keep replay observations with startup failures."
)]
fn replay(
    path: &Path,
    config: &Config,
    clock: &chunks::Clock,
) -> Result<Replayed, (io::Error, chunks::ReplayReport)> {
    let dir = path.join("chunks");
    let mut rp = Replayed::empty(dir.clone());
    let result = replay_into(&mut rp, path, &dir, true, config, clock);
    // Keep observations already collected by replay in both successful and
    // refused startup reports, even when a later file prevents replay finishing.
    rp.report.storage_errors = rp.storage_errors.clone();
    match result {
        Ok(()) => Ok(rp),
        Err(e) => Err((e, rp.report)),
    }
}
/// Replay the chunks in `dir`; with `legacy`, also the state's legacy logs (before an import, as one forever chunk;
/// after it, for dedup alone within 24 h).
fn replay_into(
    rp: &mut Replayed,
    path: &Path,
    dir: &Path,
    legacy: bool,
    config: &Config,
    clock: &chunks::Clock,
) -> io::Result<()> {
    use chunks::{ChunkState, Rec};
    let now = now_ms();
    let dir = dir.to_owned();
    fs::create_dir_all(&dir)?;
    let (records, torn) = chunks::read_catalogue(&dir.join("catalogue.log"))?;
    let (records, incomplete_retiring) = chunks::committed_records(records);
    rp.chunks.incomplete_retiring = incomplete_retiring;
    rp.report.catalogue_truncation = torn.map(|(o, b, r)| (o, b, r.to_string()));
    let mut next_hw = 0u64;
    let mut horizons: Vec<(String, String, u64)> = Vec::new();
    for r in &records {
        rp.cat_last_ms = rp.cat_last_ms.max(r.at_ms);
        match &r.rec {
            Rec::Open {
                seq,
                stripe,
                class,
                policy,
                next,
            } => {
                next_hw = next_hw.max(*next);
                rp.chunks.next_id = rp.chunks.next_id.max(seq + 1);
                rp.chunks.table.insert(
                    *seq,
                    chunks::ChunkInfo {
                        id: *seq,
                        stripe: *stripe as usize,
                        class: *class,
                        policy: policy.clone(),
                        open_ms: r.at_ms,
                        close_ms: None,
                        close_mono: None,
                        bytes: 0,
                        path: chunks::chunk_path(&dir, *class, *stripe as usize, *seq),
                        state: ChunkState::Open,
                        keys: HashMap::new(),
                        blocked_since: None,
                        horizons: Vec::new(),
                        expired: Vec::new(),
                        retired_ms: None,
                    },
                );
            }
            Rec::Close { seq, bytes, next } => {
                next_hw = next_hw.max(*next);
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Closed;
                    c.close_ms = Some(r.at_ms);
                    c.bytes = *bytes;
                }
            }
            Rec::Retiring {
                seq,
                next,
                horizons: h,
                expired,
            } => {
                next_hw = next_hw.max(*next);
                horizons.extend(h.iter().cloned());
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Retiring;
                    c.horizons = h.clone();
                    c.expired = expired.clone();
                    c.retired_ms = Some(r.at_ms);
                }
                if !expired.is_empty() {
                    rp.chunks.summaries.add(r.at_ms, expired.clone());
                }
            }
            Rec::Retired { seq } => {
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Retired;
                }
            }
            Rec::Horizons(h) => horizons.extend(h.iter().cloned()),
            Rec::Summary { retired_ms, keys } => {
                rp.chunks.summaries.add(*retired_ms, keys.clone());
            }
            Rec::Next(n) => next_hw = next_hw.max(*n),
            Rec::Resume => {}
            Rec::RetiringPart { .. } => unreachable!("committed_records folds all parts"),
            Rec::Import(i) => {
                rp.chunks.import = Some(i.clone());
                rp.chunks.import_at = r.at_ms;
            }
        }
    }
    let horizon_map = chunks::horizon_set(&horizons);
    rp.chunks.horizons = horizon_map.clone();
    rp.report.horizons = horizon_map.len();
    // Chunk files on disk must each have an open record that is not retired.
    for (_, _, seq, p) in chunks::list_chunk_files(&dir)? {
        let known = rp
            .chunks
            .table
            .get(&seq)
            .is_some_and(|c| c.state != ChunkState::Retired && c.path == p);
        if !known {
            rp.storage_errors
                .push(format!("chunk file with no open record: {}", p.display()));
        }
    }
    // Legacy stripe logs (before the import): read as one forever chunk that never retires.
    let legacy_logs: Vec<PathBuf> = (0..4)
        .map(|i| path.join(format!("stripe-{i}.log")))
        .collect();
    if legacy && legacy_logs.iter().any(|p| p.exists()) && rp.chunks.import.is_none() {
        rp.chunks.table.insert(
            chunks::LEGACY_CHUNK,
            chunks::ChunkInfo {
                id: chunks::LEGACY_CHUNK,
                stripe: 0,
                class: Class::FOREVER,
                policy: "(legacy stripe logs)".into(),
                open_ms: 0,
                close_ms: Some(0),
                close_mono: None,
                bytes: 0,
                path: path.to_owned(),
                state: ChunkState::Closed,
                keys: HashMap::new(),
                blocked_since: None,
                horizons: Vec::new(),
                expired: Vec::new(),
                retired_ms: None,
            },
        );
        let mut total = 0;
        for (i, p) in legacy_logs.iter().enumerate() {
            if !p.exists() {
                continue;
            }
            // The legacy logs were open at any crash, so a torn tail is truncated as before.
            let n = rp.replay_file(p, i, chunks::LEGACY_CHUNK, true, &horizon_map, config, now)?;
            rp.report.stripes[i].legacy_logs += 1;
            rp.report.legacy.push(p.display().to_string());
            total += n;
        }
        rp.persisted += total;
        if let Some(c) = rp.chunks.table.get_mut(&chunks::LEGACY_CHUNK) {
            c.bytes = total;
        }
    }
    let ids: Vec<u64> = rp
        .chunks
        .table
        .keys()
        .copied()
        .filter(|id| *id != chunks::LEGACY_CHUNK)
        .collect();
    for id in ids {
        let c = rp.chunks.table[&id].clone();
        match c.state {
            ChunkState::Retired => rp.report.stripes[c.stripe].retired_skipped += 1,
            ChunkState::Retiring => {
                // Finished at start; its records were already out of the index when it began retiring.
                let len = fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
                rp.persisted += len;
                if let Some(info) = rp.chunks.table.get_mut(&id) {
                    info.bytes = len;
                }
            }
            ChunkState::Open | ChunkState::Closed => {
                if !c.path.exists() {
                    if c.state == ChunkState::Open {
                        // The open record was flushed and the crash came before the file: an empty chunk.
                        rp.report.empty_closed.push(id);
                        continue;
                    }
                    rp.storage_errors.push(format!(
                        "chunk file missing with no retiring record: {}",
                        c.path.display()
                    ));
                    continue;
                }
                let open_at_crash = c.state == ChunkState::Open;
                let n = rp.replay_file(
                    &c.path,
                    c.stripe,
                    id,
                    open_at_crash,
                    &horizon_map,
                    config,
                    now,
                )?;
                rp.report.stripes[c.stripe].chunks += 1;
                if !open_at_crash && n != c.bytes {
                    return Err(corrupt(format!(
                        "interior-corrupt {}: closed chunk holds {n} bytes, its close recorded {}",
                        c.path.display(),
                        c.bytes
                    )));
                }
                rp.persisted += n;
                if let Some(info) = rp.chunks.table.get_mut(&id) {
                    info.bytes = n;
                }
            }
        }
    }
    if let (true, Some(import)) = (legacy, rp.chunks.import.clone()) {
        // A rename may have completed before its directory sync failed. No surviving source
        // names does not prove those barriers completed: repeat them before trusting the backup.
        move_legacy(path, &import, config)?;
        let ldir = path.join(&import.legacy_dir);
        let mut changed = Vec::new();
        for (name, size, sha) in &import.files {
            let p = ldir.join(name);
            let same = fs::metadata(&p).is_ok_and(|m| m.len() == *size)
                && chunks::sha256_file(&p).is_ok_and(|s| s == *sha);
            if !same {
                changed.push(format!(
                    "legacy file changed or missing since the import: {}",
                    p.display()
                ));
            }
        }
        let window = clock.utc_ms().saturating_sub(rp.chunks.import_at) < config.retry_ms;
        if !changed.is_empty() {
            rp.storage_errors.extend(changed);
        } else if window {
            // For 24 h after the import the legacy logs give their dedup entries alone, never values (5.6).
            let files = legacy_files(&ldir);
            let l = read_legacy(&files, config, now)?;
            merge_legacy_dedup(&mut rp.dedup, l.dedup)?;
            rp.report.legacy = files
                .iter()
                .map(|(_, p)| format!("{} (dedup only)", p.display()))
                .collect();
        }
        rp.report.import = Some(format!(
            "import of epoch {} ({} keys, {} B); legacy dedup replay {}",
            import.epoch,
            import.keys,
            import.bytes,
            if window {
                "on (within 24 h of the import)"
            } else {
                "off (24 h passed)"
            }
        ));
    }
    rp.next = rp.next.max(next_hw);
    Ok(())
}

pub(super) struct Recovered {
    pub(super) clock: chunks::Clock,
    pub(super) flags: Arc<Flags>,
    pub(super) chunks: chunks::ChunkStore,
    pub(super) next: u64,
    pub(super) committed: HashMap<Key, Arc<crate::value_disk::Entry>>,
    pub(super) state: State,
    pub(super) timers: timers::Shared,
    pub(super) policies: chunks::Policies,
    pub(super) epoch: u64,
    pub(super) report: chunks::ReplayReport,
}
pub(super) fn recover(path: &Path, config: &Config) -> io::Result<Recovered> {
    let clock = chunks::Clock::new(config.clock.clone());
    let start_ms = now_ms();
    let refuse = |e: io::Error, mut report: chunks::ReplayReport| -> io::Error {
        report.start_ms = start_ms;
        report.ready_ms = 0;
        report.stage = "refused".into();
        report.refusal = Some(e.to_string());
        // The replay record is written before exit; its own failure does not hide the refusal.
        let _ = report.write(path);
        e
    };
    // Admit the next epoch before import moves inputs or replay repairs tails.
    // Only an absent file means a new store; malformed/unreadable state refuses.
    let epoch_path = path.join("epoch");
    let previous = match fs::read(&epoch_path) {
        Ok(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().unwrap()),
        Ok(_) => {
            return Err(refuse(
                io::Error::new(io::ErrorKind::InvalidData, "invalid epoch"),
                chunks::ReplayReport::default(),
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => {
            return Err(refuse(
                io::Error::new(e.kind(), format!("read epoch: {e}")),
                chunks::ReplayReport::default(),
            ));
        }
    };
    let epoch = previous.checked_add(1).ok_or_else(|| {
        refuse(
            io::Error::other("epoch exhausted"),
            chunks::ReplayReport::default(),
        )
    })?;
    let imported = match import_legacy(path, config, &clock, epoch) {
        Ok(x) => x,
        Err(e) => {
            let report = chunks::ReplayReport {
                import: Some(format!("refused: {e}")),
                ..Default::default()
            };
            return Err(refuse(e, report));
        }
    };
    let mut rp = match replay(path, config, &clock) {
        Ok(rp) => rp,
        Err((e, report)) => return Err(refuse(e, report)),
    };
    if let Some((_, line)) = imported {
        rp.report.import = Some(format!(
            "{line}; {}",
            rp.report.import.take().unwrap_or_default()
        ));
    }
    rp.chunks.recount_index_bytes();
    if rp.committed.len() > config.index_cap
        || rp.committed.keys().map(crate::store::index_charge).sum::<usize>()
            + rp.dedup
                .iter()
                .map(|(k, _)| crate::store::dedup_charge(k))
                .sum::<usize>()
            + rp.chunks.index_bytes + config.memory_margin()
            > config.capacity
        || rp.dedup.len() > config.dedup_cap
    {
        let report = rp.report.clone();
        return Err(refuse(
            io::Error::other("replayed state exceeds configured limits"),
            report,
        ));
    }
    if !rp.truncate.is_empty() || rp.report.catalogue_truncation.is_some() {
        rp.report.start_ms = start_ms;
        rp.report.ready_ms = 0;
        rp.report.stage = "truncation_pending".into();
        // Preserve what replay found before its source bytes disappear. A later successful
        // start can replace replay.json; the bounded replay.log retains this non-ready stage.
        rp.report.write(path)?;
    }
    for (p, offset) in &rp.truncate {
        OpenOptions::new().write(true).open(p)?.set_len(*offset)?;
    }
    let cat_path = path.join("chunks").join("catalogue.log");
    if let Some((offset, _, _)) = &rp.report.catalogue_truncation {
        OpenOptions::new()
            .write(true)
            .open(&cat_path)?
            .set_len(*offset)?;
    }
    TestSeam::checkpoint("MEMBUS_TEST_PAUSE_REPLAY_TRUNCATED");
    #[cfg(test)]
    if config.stop_after_replay_truncation {
        return Err(io::Error::other("test stop after replay truncation"));
    }
    // A complete record records the requested acknowledgement, not proof of a
    // covering live sync. Establish that prerequisite before any recovered
    // value/outcome is exposed or an open chunk's Close is published. Closed
    // chunks are included: an older startup may have closed one without it.
    let mut recovered: Vec<PathBuf> = rp
        .chunks
        .table
        .values()
        .filter(|c| {
            c.bytes != 0
                && matches!(
                    c.state,
                    chunks::ChunkState::Open | chunks::ChunkState::Closed
                )
                && c.id != chunks::LEGACY_CHUNK
                && c.path.exists()
        })
        .map(|c| c.path.clone())
        .collect();
    recovered.extend(legacy_files(path).into_iter().map(|(_, p)| p));
    recovered.sort();
    recovered.dedup();
    for p in recovered {
        if let Err(e) = recovery_sync(&p, config) {
            return Err(refuse(e, rp.report.clone()));
        }
    }
    let mut cs = std::mem::replace(&mut rp.chunks, chunks::ChunkStore::new(path.join("chunks")));
    let mut catalogue = chunks::Catalogue::open(&cat_path)?;
    catalogue.last_ms = rp.cat_last_ms;
    cs.cat = Some(catalogue);
    let utc = clock.utc_ms();
    let mono = clock.mono_ms();
    let next = rp.next;
    let mut persisted_bytes = rp.persisted;
    // A crash between `retiring` and `retired` is finished here; the delete is idempotent.
    let retiring: Vec<u64> = cs
        .table
        .values()
        .filter(|c| c.state == chunks::ChunkState::Retiring)
        .map(|c| c.id)
        .collect();
    for id in retiring {
        let (p, bytes) = (cs.table[&id].path.clone(), cs.table[&id].bytes);
        match fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => {
                cs.table.get_mut(&id).unwrap().blocked_since = Some(utc);
                continue;
            }
        }
        cs.log(utc, chunks::Rec::Retired { seq: id })?;
        let c = cs.table.get_mut(&id).unwrap();
        c.state = chunks::ChunkState::Retired;
        c.keys.clear();
        cs.retired_chunks += 1;
        cs.retired_bytes += bytes;
        persisted_bytes -= bytes;
        rp.report.retirements_finished.push(id);
    }
    // An open record whose file was never made retires at once: it holds nothing.
    for id in rp.report.empty_closed.clone() {
        cs.log(utc, chunks::Rec::Retired { seq: id })?;
        if let Some(c) = cs.table.get_mut(&id) {
            c.state = chunks::ChunkState::Retired;
        }
    }
    // Chunks open at the crash close now, at their replayed (and truncated) length.
    let open: Vec<u64> = cs
        .table
        .values()
        .filter(|c| c.state == chunks::ChunkState::Open)
        .map(|c| c.id)
        .collect();
    for id in open {
        let bytes = cs.table[&id].bytes;
        if bytes == 0 {
            // Nothing whole was written (or its torn tail went): the empty file retires, so no empty chunk stays.
            fs::remove_file(&cs.table[&id].path).or_else(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
            cs.log(utc, chunks::Rec::Retired { seq: id })?;
            cs.table.get_mut(&id).unwrap().state = chunks::ChunkState::Retired;
            rp.report.empty_closed.push(id);
            continue;
        }
        cs.log(
            utc,
            chunks::Rec::Close {
                seq: id,
                bytes,
                next,
            },
        )?;
        let c = cs.table.get_mut(&id).unwrap();
        c.state = chunks::ChunkState::Closed;
        c.close_ms = Some(utc);
        c.close_mono = Some(mono);
        rp.report.closed_at_start.push((id, bytes));
    }
    // Retirement pauses when the clock is in doubt (5.4).
    let beat = fs::read_to_string(path.join("heartbeat.json"))
        .ok()
        .and_then(|t| {
            t.split_once("\"unix_ms\":").map(|(_, r)| {
                r.chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
            })
        })
        .and_then(|d| d.parse::<u64>().ok());
    if utc < rp.cat_last_ms {
        cs.degraded = Some(format!(
            "UTC {utc} is earlier than the catalogue's last record {}",
            rp.cat_last_ms
        ));
    } else if beat.is_some_and(|b| utc > b + 86_400_000) {
        cs.degraded = Some(format!(
            "UTC {utc} is more than 24 h past the heartbeat's last beat {}",
            beat.unwrap()
        ));
    }
    rp.report.expiry_degraded = cs.degraded.clone();
    let flags = Arc::new(Flags::default());
    flags
        .expiry_degraded
        .store(cs.degraded.is_some(), Ordering::SeqCst);
    if !rp.storage_errors.is_empty() {
        flags.storage_error.store(true, Ordering::SeqCst);
    }
    let epoch_tmp = path.join("epoch.tmp");
    {
        let mut f = File::create(&epoch_tmp)?;
        f.write_all(&epoch.to_le_bytes())?;
        f.sync_all()?;
    }
    fs::rename(epoch_tmp, epoch_path)?;
    recovery_sync_dir(path, config).map_err(|e| refuse(e, rp.report.clone()))?;
    let mut committed = rp.committed;
    cs.summaries.expire(utc, config.summary_ms);
    // A tombstone no other retained chunk holds a record of leaves the index at start (5.3).
    let mut tomb_candidates = HashSet::new();
    let tombs: Vec<Key> = committed
        .iter()
        .filter(|(_, v)| v.tombstone)
        .map(|(k, _)| k.clone())
        .collect();
    for k in tombs {
        let chunk = rp.chunk_of.get(&k).copied().unwrap_or(0);
        let others: Vec<u64> = cs.holders.others(&k, chunk).collect();
        if others.is_empty() {
            committed.remove(&k);
        } else if others.iter().all(|c| {
            *c != chunks::LEGACY_CHUNK && cs.table.get(c).is_some_and(|i| i.class.finite())
        }) {
            tomb_candidates.insert(k);
        }
    }
    let metadata: HashMap<_, _> = committed
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                Meta {
                    version: v.version,
                    tombstone: v.tombstone,
                    bytes: crate::store::index_charge(k),
                    chunk: rp.chunk_of.get(k).copied().unwrap_or(0),
                },
            )
        })
        .collect();
    // The namespace count is live keys; the index count adds tombstones and expiry summaries (5.3).
    let mut namespace_counts = HashMap::new();
    for ((ns, _), m) in metadata.iter() {
        if !m.tombstone {
            *namespace_counts.entry(ns.clone()).or_insert(0) += 1;
        }
    }
    let keys = metadata.len() + cs.summaries.charged;
    let committed_bytes = metadata.values().map(|m| m.bytes).sum();
    let dedup = rp.dedup;
    let dedup_entries = dedup.len();
    let dedup_bytes = dedup.keys().map(crate::store::dedup_charge).sum();
    let mut expiry: Vec<_> = dedup.iter().map(|(k, d)| (k.clone(), d.at)).collect();
    expiry.sort_by_key(|(_, at)| *at);
    // Only control records need decoding into runtime state. Ordinary values stay on disk.
    let control: HashMap<_,_> = committed.iter().filter(|(k,_)| k.0 == timers::NS)
        .map(|(k,v)| Ok((k.clone(), v.read()?))).collect::<io::Result<_>>()?;
    let timers = timers::State::restore(&control)?;
    let mut policies = chunks::Policies::default();
    for ((ns, key), v) in committed.iter() {
        if ns == chunks::POLICY_NS && !v.tombstone {
            if let Some((pns, prefix, class)) = chunks::parse_policy(key, &v.read()?.bytes) {
                policies.set(&pns, &prefix, Some(class));
            }
        }
    }
    let state = State {
        pending: HashMap::new(),
        metadata,
        namespace_counts: namespace_counts.clone(),
        keys,
        committed_bytes,
        dedup_bytes,
        dedup,
        policy_publishing: HashSet::new(),
        retired: VecDeque::new(),
        retired_set: HashSet::new(),
        expiry: expiry.into(),
        next,
        reserved_bytes: 0,
        memory_reserved: 0,
        stripe_reserved: [0; 4],
        stripe_items: [0; 4],
        persisted_bytes,
        pending_disk: 0,
        read_only: flags.storage_error.load(Ordering::SeqCst),
        warming: false,
        tomb_candidates,
    };
    let mut report = rp.report;
    report.epoch = epoch;
    report.start_ms = start_ms;
    report.dedup_entries = dedup_entries;
    report.keys = keys;
    report.namespace_counts = namespace_counts
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    Ok(Recovered {
        clock,
        flags,
        chunks: cs,
        next,
        committed,
        state,
        timers,
        policies,
        epoch,
        report,
    })
}

#[cfg(test)]
#[path = "replay/durability_tests.rs"]
mod durability_tests;
