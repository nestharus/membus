//! Explicit fresh-state staging and native receiving of a caller-owned engineering value. Uses the
//! public wire dispatcher in the selected library; no installed pointer, server or network is used.
use membus::{chunks, *};
use std::{fs, io, path::Path, process::ExitCode};
#[path = "../../tools/catalogue_receiver.rs"]
mod receiver;
const HEAD: &str = match option_env!("MEMBUS_BUILD_HEAD") {
    Some(s) => s,
    None => "unrecorded",
};
fn fail(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
fn quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
fn request(op: u8, ns: &str, key: &str) -> Vec<u8> {
    let mut b = vec![1, 0, 0, 0, op];
    b.extend(17u64.to_le_bytes());
    b.extend((now_ms() + 30_000).to_le_bytes());
    lp(&mut b, format!("receiving/{key}").as_bytes());
    lp(&mut b, ns.as_bytes());
    b
}
fn call(s: &Store, op: u8, ns: &str, key: &str, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut b = request(op, ns, key);
    b.extend(payload);
    let reply = s.dispatch(&b);
    if reply.len() < 19
        || u32::from_le_bytes(reply[..4].try_into().unwrap()) as usize != reply.len() - 4
        || reply[4..8] != [1, 0, 0, 0]
        || reply[8] != op | 0x80
        || u64::from_le_bytes(reply[9..17].try_into().unwrap()) != 17
    {
        return Err(fail("receiving wire correspondence failed"));
    }
    let code = u16::from_le_bytes(reply[17..19].try_into().unwrap());
    if code != 0 {
        return Err(fail(format!("wire operation {op} refused code {code}")));
    }
    Ok(reply)
}
fn read(s: &Store, ns: &str, key: &str, expected: &[u8], version: u64) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    lp(&mut payload, key.as_bytes());
    let reply = call(s, GET, ns, key, &payload)?;
    // Decode directly at the receiving boundary rather than calling Store::get as the oracle.
    let p = &reply[19..];
    if p.len() < 14
        || p[0] != 1
        || u64::from_le_bytes(p[1..9].try_into().unwrap()) != version
        || p[9] != 2
        || u32::from_le_bytes(p[10..14].try_into().unwrap()) as usize != expected.len()
        || p[14..] != *expected
    {
        return Err(fail(
            "received value/version/ack differs from independently supplied input",
        ));
    }
    Ok(reply)
}
fn policy(s: &Store, ns: &str, key: &str) -> io::Result<Vec<u8>> {
    let reply = call(s, POLICY_GET, ns, key, &[])?;
    let mut expected = 1u16.to_le_bytes().to_vec();
    lp(&mut expected, key.as_bytes());
    expected.push(1);
    expected.extend(0u64.to_le_bytes());
    if reply[19..] != expected {
        return Err(fail(
            "namespace policy table must contain only the selected Latest prefix row (including no unrelated rows)",
        ));
    }
    Ok(reply)
}
fn run() -> io::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if !((a.len() == 6 && a[0] == "stage") || (a.len() == 7 && a[0] == "receive")) {
        return Err(fail("usage: storage-receive stage FRESH_STATE NS KEY INPUT EXPECTED_HEAD | receive STATE NS KEY INPUT EXPECTED_HEAD EXPECTED_VERSION"));
    }
    if a[5] != HEAD {
        return Err(fail(format!(
            "source selection refused: compiled {HEAD}, expected {}",
            a[5]
        )));
    }
    let state = Path::new(&a[1]);
    let input = Path::new(&a[4]);
    if fs::metadata(input)?.len() > MAX_VALUE as u64 {
        return Err(fail("input exceeds value bound"));
    }
    let expected = fs::read(input)?;
    if a[0] == "stage" {
        fs::create_dir(state)?;
    } else if !state.join("chunks/catalogue.log").is_file() {
        return Err(fail("existing received state required"));
    }
    let cfg = Config {
        flush_ms: 0,
        ..Default::default()
    };
    let s = Store::open(state, cfg.clone())?;
    let (version, ack_reply) = if a[0] == "stage" {
        let mut p = Vec::new();
        lp(&mut p, a[3].as_bytes());
        p.push(1);
        p.extend(0u64.to_le_bytes());
        call(&s, POLICY_SET, &a[2], &a[3], &p)?;
        let mut p = Vec::new();
        lp(&mut p, a[3].as_bytes());
        p.push(2);
        lp(&mut p, &expected);
        let reply = call(&s, PUT, &a[2], &a[3], &p)?;
        if reply.len() != 28 || reply[27] != 2 {
            return Err(fail("durable acknowledgement shape"));
        }
        (u64::from_le_bytes(reply[19..27].try_into().unwrap()), reply)
    } else {
        (
            a[6].parse::<u64>()
                .map_err(|_| fail("expected version required"))?,
            Vec::new(),
        )
    };
    let effective = policy(&s, &a[2], &a[3])?;
    let received = read(&s, &a[2], &a[3], &expected, version)?;
    let cat = state.join("chunks/catalogue.log");
    let (catalogue_snapshot, records) = receiver::capture(&cat)?;
    let mut hash = chunks::Sha256::new();
    hash.update(&catalogue_snapshot);
    let catalogue_sha256 = hash.hex();
    if !records.iter().any(|r| {
        matches!(&r.rec,chunks::Rec::Open { class,policy,.. }
        if *class==Class::LATEST && policy==&format!("{}/{}",a[2],a[3]))
    }) {
        return Err(fail("selected receiving chunk representation missing"));
    }
    drop(s);
    let reopened = Store::open(state, cfg)?;
    let retained = read(&reopened, &a[2], &a[3], &expected, version)?;
    policy(&reopened, &a[2], &a[3])?;
    drop(reopened);
    let mut input_hash = chunks::Sha256::new();
    input_hash.update(&expected);
    println!("{{\"operation\":{},\"source_head\":{},\"transport\":\"native public library wire dispatcher\",\"namespace\":{},\"key\":{},\"version\":{},\"ack\":2,\"input_sha256\":{},\"catalogue_sha256\":{},\"decoded_records\":{},\"catalogue_phase\":\"before_orderly_reopen\",\"policy_scope\":\"singleton_namespace_latest_prefix\",\"policy_prefix\":{},\"ack_reply_hex\":{},\"policy_reply_hex\":{},\"received_reply_hex\":{},\"reopened_reply_hex\":{},\"value_hex\":{}}}",
        quote(&a[0]),quote(HEAD),quote(&a[2]),quote(&a[3]),version,quote(&input_hash.hex()),
        quote(&catalogue_sha256),records.len(),quote(&a[3]),quote(&hex(&ack_reply)),quote(&hex(&effective)),
        quote(&hex(&received)),quote(&hex(&retained)),quote(&hex(&expected)));
    Ok(())
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
