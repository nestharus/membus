//! Read-only validation of native receiving receipts against an independent input.
//! Writes only a new recovered-value file; it never opens or mutates a Store.
#[path = "../tools/catalogue_receiver.rs"]
mod catalogue;
#[path = "receiving/json.rs"]
mod json;
use json::{n, obj, s, J};
use membus::{chunks, GET, POLICY_GET, PUT};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
};
type R<T> = Result<T, String>;
fn bytes(path: &Path, max: u64) -> R<Vec<u8>> {
    let mut out = Vec::new();
    fs::File::open(path)
        .map_err(|_| "selected input unavailable")?
        .take(max + 1)
        .read_to_end(&mut out)
        .map_err(|_| "selected input read failed")?;
    if out.len() as u64 > max {
        return Err("selected input exceeds bound".into());
    }
    Ok(out)
}
fn receipt(path: &Path) -> R<J> {
    let body = bytes(path, 8 << 20)?;
    json::parse(std::str::from_utf8(&body).map_err(|_| "receipt is not UTF-8")?)
}
fn digest(bytes: &[u8]) -> String {
    let mut hash = chunks::Sha256::new();
    hash.update(bytes);
    hash.hex()
}
fn hex(value: &str) -> R<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err("odd hexadecimal receipt".into());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|part| {
            let nibble = |b: u8| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(String::from("noncanonical hexadecimal receipt")),
            };
            Ok((nibble(part[0])? << 4) | nibble(part[1])?)
        })
        .collect()
}
fn wide(bytes: &[u8]) -> R<u64> {
    Ok(u64::from_le_bytes(
        bytes.try_into().map_err(|_| "short wire wide value")?,
    ))
}
fn word(bytes: &[u8]) -> R<u32> {
    Ok(u32::from_le_bytes(
        bytes.try_into().map_err(|_| "short wire word")?,
    ))
}
fn wire(bytes: &[u8], op: u8) -> R<&[u8]> {
    if bytes.len() < 19
        || word(&bytes[..4])? as usize != bytes.len() - 4
        || bytes[4..8] != [1, 0, 0, 0]
        || bytes[8] != op | 0x80
        || wide(&bytes[9..17])? != 17
        || bytes[17..19] != [0, 0]
    {
        return Err("wire frame/version/operation/request/status differs".into());
    }
    Ok(&bytes[19..])
}
fn get(reply: &[u8], version: u64, input: &[u8]) -> R<()> {
    let payload = wire(reply, GET)?;
    if payload.len() < 14
        || payload[0] != 1
        || wide(&payload[1..9])? != version
        || payload[9] != 2
        || word(&payload[10..14])? as usize != input.len()
        || payload[14..] != *input
    {
        return Err("received complete value/version/durable ACK differs".into());
    }
    Ok(())
}
fn policy(reply: &[u8], prefix: &str) -> R<()> {
    let payload = wire(reply, POLICY_GET)?;
    let mut selected = vec![1, 0];
    selected.extend((prefix.len() as u32).to_le_bytes());
    selected.extend(prefix.as_bytes());
    selected.push(1);
    selected.extend(0u64.to_le_bytes());
    if payload != selected {
        return Err("singleton namespace Latest-prefix reply differs".into());
    }
    Ok(())
}
fn execute() -> R<J> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 8 {
        return Err("expected INPUT STAGE RECEIVE CATALOGUE_STAGE CATALOGUE_RECEIVE SOURCE_HEAD INPUT_SHA256 NEW_RECOVERED".into());
    }
    let input_path = Path::new(&args[0]);
    let input = bytes(input_path, 64 << 20)?;
    let stage = receipt(Path::new(&args[1]))?;
    let receive = receipt(Path::new(&args[2]))?;
    let expected_head = args[5].to_str().ok_or("source head is not UTF-8")?;
    let expected_hash = args[6].to_str().ok_or("input hash is not UTF-8")?;
    if digest(&input) != expected_hash
        || stage.get("input_sha256").s()? != expected_hash
        || receive.get("input_sha256").s()? != expected_hash
        || stage.get("source_head").s()? != expected_head
        || receive.get("source_head").s()? != expected_head
    {
        return Err("actual input or selected source correspondence differs".into());
    }
    let version = stage.get("version").number()?;
    let namespace = stage.get("namespace").s()?;
    let key = stage.get("key").s()?;
    if stage.get("operation").s()? != "stage"
        || receive.get("operation").s()? != "receive"
        || stage.get("ack").number()? != 2
        || receive.get("ack").number()? != 2
        || receive.get("version").number()? != version
        || receive.get("namespace").s()? != namespace
        || receive.get("key").s()? != key
        || receive.get("ack_reply_hex").s()? != ""
    {
        return Err("stage/receive association differs".into());
    }
    let ack = hex(stage.get("ack_reply_hex").s()?)?;
    let ack_payload = wire(&ack, PUT)?;
    if ack_payload.len() != 9 || wide(&ack_payload[..8])? != version || ack_payload[8] != 2 {
        return Err("actual staged acknowledgement differs".into());
    }
    for capture in [&stage, &receive] {
        if let J::Str(phase) = capture.get("catalogue_phase") {
            if phase != "before_orderly_reopen"
                || capture.get("policy_scope").s()? != "singleton_namespace_latest_prefix"
                || capture.get("policy_prefix").s()? != key
            {
                return Err("receipt catalogue phase or policy scope differs".into());
            }
        } else if !matches!(capture.get("catalogue_phase"), J::Null) {
            return Err("receipt catalogue phase is not a string".into());
        }
        if hex(capture.get("value_hex").s()?)? != input {
            return Err("complete captured value differs".into());
        }
        get(
            &hex(capture.get("received_reply_hex").s()?)?,
            version,
            &input,
        )?;
        get(
            &hex(capture.get("reopened_reply_hex").s()?)?,
            version,
            &input,
        )?;
        policy(&hex(capture.get("policy_reply_hex").s()?)?, key)?;
    }
    let mut counts = Vec::new();
    for (path, capture) in [(&args[3], &stage), (&args[4], &receive)] {
        let path = Path::new(path);
        let snapshot = bytes(path, 64 << 20)?;
        if digest(&snapshot) != capture.get("catalogue_sha256").s()? {
            return Err("catalogue snapshot hash differs from retained receipt".into());
        }
        let records =
            catalogue::decode(&snapshot).map_err(|_| "independent catalogue decode refused")?;
        if matches!(capture.get("catalogue_phase"), J::Str(_))
            && capture.get("decoded_records").number()? != records.len() as u64
        {
            return Err("catalogue snapshot count differs from retained receipt".into());
        }
        if !records.iter().any(|record| {
            matches!(&record.rec, chunks::Rec::Open { class, policy, .. }
            if *class == chunks::Class::LATEST && policy == &format!("{namespace}/{key}"))
        }) {
            return Err("selected Latest-prefix Open missing from catalogue snapshot".into());
        }
        counts.push(n(records.len() as u64));
    }
    let reply = hex(receive.get("received_reply_hex").s()?)?;
    let recovered = &wire(&reply, GET)?[14..];
    let recovered_path = Path::new(&args[7]);
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(recovered_path)
        .map_err(|_| "new recovered output refused")?;
    output
        .write_all(recovered)
        .map_err(|_| "recovered output write failed")?;
    output
        .sync_all()
        .map_err(|_| "recovered output flush failed")?;
    Ok(obj(vec![
        ("protocol", s("native-storage-readback/v1")), ("input_bytes", n(input.len() as u64)),
        ("input_sha256", s(expected_hash)), ("source_head", s(expected_head)),
        ("actual_ack_version", n(version)), ("ack", n(2)), ("namespace", s(namespace)),
        ("Latest_prefix", s(key)), ("policy_namespace_is_singleton", J::Bool(true)),
        ("four_complete_GET_reply_values_equal_input", J::Bool(true)),
        ("stage_PUT_ack_frame_decoded", J::Bool(true)), ("separate_receive_PUT_ack_is_absent", J::Bool(true)),
        ("catalogue_snapshot_decoded_counts", J::Arr(counts)),
        ("stage_catalogue_phase", s(stage.get("catalogue_phase").s().unwrap_or("legacy_post_return_hash_only"))),
        ("receive_catalogue_phase", s(receive.get("catalogue_phase").s().unwrap_or("legacy_post_return_hash_only"))),
        ("scope", s("Supplementary ordinary stage/receive and orderly reopen only; no recovery-replacement, cold or new policy validation qualification")),
        ("recovered_file", s(recovered_path.to_string_lossy())),
    ]))
}
fn main() {
    match execute() {
        Ok(value) => println!("{}", value.dump()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
