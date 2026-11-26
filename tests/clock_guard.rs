//! Source guard for accidental server wall-clock bypasses, with source locations.
use std::{fs, path::Path};

#[derive(Debug)]
struct Token<'a> {
    text: &'a str,
    line: usize,
}

// Ignore comments and literals, including raw strings, without a parsing dependency.
fn tokens(source: &str) -> Vec<Token<'_>> {
    let b = source.as_bytes();
    let (mut i, mut line) = (0, 1);
    let mut out = Vec::new();
    while i < b.len() {
        let start = i;
        if b[i].is_ascii_whitespace() {
            i += 1;
        } else if b[i..].starts_with(b"//") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < b.len() && depth != 0 {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else {
            let mut raw = i + usize::from(b[i] == b'r');
            while raw < b.len() && b[raw] == b'#' {
                raw += 1;
            }
            if b[i] == b'r' && raw < b.len() && b[raw] == b'"' {
                let hashes = raw - i - 1;
                i = raw + 1;
                while i < b.len() {
                    if b[i] == b'"'
                        && b.get(i + 1..i + 1 + hashes)
                            .is_some_and(|s| s.iter().all(|c| *c == b'#'))
                    {
                        i += 1 + hashes;
                        break;
                    }
                    i += 1;
                }
            } else if b[i] == b'"' {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i = (i + 2).min(b.len());
                    } else if b[i] == b'"' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
            } else if b[i] == b'\''
                && (b.get(i + 2) == Some(&b'\'') || b.get(i + 1) == Some(&b'\\'))
            {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i = (i + 2).min(b.len());
                    } else if b[i] == b'\'' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
            } else {
                i += 1;
                if b[start].is_ascii_alphabetic() || b[start] == b'_' {
                    while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                        i += 1;
                    }
                }
                // Punctuation outside ASCII cannot be part of the searched identifier.
                while !source.is_char_boundary(i) {
                    i += 1;
                }
                out.push(Token {
                    text: &source[start..i],
                    line,
                });
            }
        }
        line += b[start..i].iter().filter(|c| **c == b'\n').count();
    }
    out
}

fn violations(path: &str, source: &str) -> Vec<String> {
    // These run in client/CLI processes, or contain test fixtures only.
    if matches!(path, "main.rs" | "bin/storage-receive.rs")
        || path.starts_with("cli/")
        || path.starts_with("client/")
        || path.starts_with("unit_tests/")
        || path.ends_with("tests.rs")
    {
        return Vec::new();
    }
    let t = tokens(source);
    let mut allowed = vec![false; t.len()];
    for i in 0..t.len() {
        let test = t.get(i..i + 7).is_some_and(|s| {
            s.iter()
                .map(|x| x.text)
                .eq(["#", "[", "cfg", "(", "test", ")", "]"])
        });
        let clock = path == "store/clock.rs"
            && t.get(i..i + 3)
                .is_some_and(|s| s.iter().map(|x| x.text).eq(["impl", "Clock", "{"]));
        if !test && !clock {
            continue;
        }
        let mut depth = 0;
        for j in (i + if test { 7 } else { 2 })..t.len() {
            allowed[j] = true;
            match t[j].text {
                "{" => depth += 1,
                "}" => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                ";" if depth == 0 => break,
                _ => {}
            }
        }
    }
    (0..t.len())
        .filter(|&i| {
            t[i].text == "now_ms"
                && t.get(i + 1).is_some_and(|x| x.text == "(")
                && !allowed[i]
                && !(path == "lib.rs" && i > 0 && t[i - 1].text == "fn")
        })
        .map(|i| {
            format!(
                "src/{path}:{}: use the store Clock instead of now_ms()",
                t[i].line
            )
        })
        .collect()
}

#[test]
fn server_time_reads_use_clock() {
    fn walk(root: &Path, dir: &Path, found: &mut Vec<String>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(root, &p, found);
            } else if p.extension().is_some_and(|x| x == "rs") {
                let relative = p
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                found.extend(violations(&relative, &fs::read_to_string(p).unwrap()));
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    walk(&root, &root, &mut found);
    found.sort();
    assert!(
        found.is_empty(),
        "server wall-clock bypasses:\n{}",
        found.join("\n")
    );
}

#[test]
fn guard_reports_multiline_qualified_calls_and_ignores_test_items_and_literals() {
    let s = "// now_ms()\nconst S: &str = r#\"now_ms()\"#;\n#[cfg(test)] mod tests { fn f() { now_ms(); } }\nfn production() { crate::now_ms\n (); }\n";
    assert_eq!(
        violations("store.rs", s),
        vec!["src/store.rs:4: use the store Clock instead of now_ms()"]
    );
    assert!(violations("lib.rs", "pub fn now_ms() -> u64 { 0 }").is_empty());
    assert!(violations(
        "store/clock.rs",
        "impl Clock { fn utc_ms(&self) -> u64 { crate::now_ms() } }"
    )
    .is_empty());
    assert_eq!(
        violations(
            "store/clock.rs",
            "impl Other { fn utc_ms(&self) -> u64 { crate::now_ms() } }"
        )
        .len(),
        1
    );
}
