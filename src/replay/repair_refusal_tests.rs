//! Real write-sharing denials during repair retain pending evidence and record refusal.
use super::*;
use std::{
    ffi::c_void,
    os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
    ptr,
};

#[link(name = "kernel32")]
extern "system" {
    fn CreateFileMappingW(
        file: *mut c_void,
        attributes: *const c_void,
        protection: u32,
        high: u32,
        low: u32,
        name: *const u16,
    ) -> *mut c_void;
    fn MapViewOfFile(
        mapping: *mut c_void,
        access: u32,
        high: u32,
        low: u32,
        bytes: usize,
    ) -> *mut c_void;
    fn UnmapViewOfFile(view: *const c_void) -> i32;
    fn CloseHandle(handle: *mut c_void) -> i32;
}

struct ReadMapping(*mut c_void);
impl ReadMapping {
    fn from_file(file: &File) -> Self {
        // Map the existing file read-only with no write-sharing restriction.
        unsafe {
            let mapping =
                CreateFileMappingW(file.as_raw_handle(), ptr::null(), 0x02, 0, 0, ptr::null());
            assert!(!mapping.is_null(), "{}", io::Error::last_os_error());
            let view = MapViewOfFile(mapping, 0x04, 0, 0, 0);
            let error = io::Error::last_os_error();
            CloseHandle(mapping);
            assert!(!view.is_null(), "{error}");
            Self(view)
        }
    }
}
impl Drop for ReadMapping {
    fn drop(&mut self) {
        // The view owns its lifetime independently of the file and mapping handles.
        unsafe {
            UnmapViewOfFile(self.0);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Denial {
    DataWrite,
    CatalogueAcquisition,
    CatalogueTruncate,
}

fn repair_refusal(denial: Denial) {
    let f = Fixture::new();
    let config = Config {
        clock: Some(chunks::ManualClock::new(1_000_000)),
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    };
    let mutation = Mutation {
        ns: "caller".into(),
        key: "key".into(),
        op_id: "retained".into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: b"kept".to_vec(),
        remove: false,
    };
    let store = Store::open(&f.0, config.clone()).unwrap();
    let outcome = store.mutate(mutation.clone());
    assert_eq!(outcome.code, 0);
    drop(store);
    f.unlock();
    let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let data = &files[0].3;
    let cat = f.0.join("chunks/catalogue.log");
    let original = fs::read(data).unwrap();
    let catalogue = fs::read(&cat).unwrap();
    OpenOptions::new()
        .append(true)
        .open(data)
        .unwrap()
        .write_all(b"MBK")
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&cat)
        .unwrap()
        .write_all(b"MBC")
        .unwrap();
    let torn_data = fs::read(data).unwrap();
    let torn_cat = fs::read(&cat).unwrap();
    let target = if denial == Denial::DataWrite {
        data
    } else {
        &cat
    };
    let mapping_denial = denial == Denial::CatalogueTruncate;
    let early = denial == Denial::CatalogueAcquisition;
    let held = OpenOptions::new()
        .read(true)
        .share_mode(if mapping_denial { 1 | 2 | 4 } else { 1 | 4 })
        .open(target)
        .unwrap();
    let mapping = mapping_denial.then(|| ReadMapping::from_file(&held));
    let expected = if mapping_denial {
        // The compatible writer opens successfully. The mapped view blocks truncation.
        let control = f.0.join("truncate-control");
        fs::write(&control, &torn_cat).unwrap();
        let control_reader = File::open(&control).unwrap();
        let _control_mapping = ReadMapping::from_file(&control_reader);
        let writer = OpenOptions::new().write(true).open(control).unwrap();
        writer.set_len(catalogue.len() as u64).unwrap_err()
    } else {
        OpenOptions::new().write(true).open(target).unwrap_err()
    };
    assert_eq!(
        expected.raw_os_error(),
        Some(if mapping_denial { 1224 } else { 32 })
    );
    let error = Store::open(&f.0, config.clone())
        .err()
        .expect("held repair target must refuse");
    assert_eq!(error.raw_os_error(), expected.raw_os_error());
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    assert!(!f.0.join("epoch.tmp").exists());
    assert_eq!(fs::read(&cat).unwrap(), torn_cat);
    assert_eq!(
        fs::read(data).unwrap(),
        if mapping_denial {
            &original
        } else {
            &torn_data
        }
        .as_slice()
    );
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains("\"ready_ms\":0"), "{report}");
    assert!(
        report.contains(&format!("\"refusal\":{:?}", error.to_string())),
        "{report}"
    );
    assert!(report.contains("\"epoch\":2"), "{report}");
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    let lines: Vec<_> = history.lines().collect();
    assert_eq!(*lines.last().unwrap(), report.trim_end());
    let prior = lines[lines.len() - 2];
    if early {
        assert!(prior.contains("\"stage\":\"ready\""), "{prior}");
        assert!(report.contains("\"catalogue_truncation\":null"), "{report}");
        assert!(!report.contains("\"truncations\":[{"), "{report}");
        assert!(!report.contains("\"records\":1"), "{report}");
    } else {
        assert!(
            prior.contains("\"stage\":\"truncation_pending\""),
            "{prior}"
        );
        for observation in [prior, report.as_str()] {
            assert!(
                observation.contains("\"truncations\":[{\"chunk\""),
                "{observation}"
            );
            assert!(
                observation.contains(&format!("\"offset\":{},\"bytes\":3", original.len())),
                "{observation}"
            );
            assert!(
                observation.contains(&format!(
                    "\"catalogue_truncation\":{{\"offset\":{},\"bytes\":3",
                    catalogue.len()
                )),
                "{observation}"
            );
        }
    }
    drop(mapping);
    drop(held);
    f.unlock();
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.get("caller", "key").unwrap().bytes, b"kept");
    assert_eq!(store.mutate(mutation), outcome);
    let report = store.replay_report();
    assert_eq!(report.stage, "ready");
    assert_eq!(
        report.catalogue_truncation.unwrap().0,
        catalogue.len() as u64
    );
    assert_eq!(
        report
            .stripes
            .iter()
            .map(|stripe| stripe.truncations.len())
            .sum::<usize>(),
        usize::from(!mapping_denial)
    );
    assert_eq!(
        report.closed_at_start,
        vec![(files[0].2, original.len() as u64)]
    );
    assert_eq!(fs::read(data).unwrap(), original);
    assert!(fs::read(&cat).unwrap().starts_with(&catalogue));
    let (records, torn) = chunks::read_catalogue(&cat).unwrap();
    assert!(torn.is_none());
    assert_eq!(records.len(), 2);
    assert!(matches!(records[1].rec, chunks::Rec::Close { seq, bytes, .. }
        if seq == files[0].2 && bytes == original.len() as u64));
}

#[test]
fn data_tail_write_denial_records_refusal_before_either_repair_or_epoch() {
    repair_refusal(Denial::DataWrite);
}
#[test]
fn catalogue_acquisition_denial_leaves_both_tails_unscanned() {
    repair_refusal(Denial::CatalogueAcquisition);
}
#[test]
fn catalogue_tail_write_denial_records_refusal_without_undoing_prior_data_repair() {
    repair_refusal(Denial::CatalogueTruncate);
}
