//! Bounded JSON reader. No dependencies, duplicate keys and nonfinite numbers rejected.
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq)]
pub enum J {
    Null,
    Bool(bool),
    /// Validated finite JSON number spelling; exact admission precedes conversion.
    Decimal(String),
    UInt(u64),
    Str(String),
    Arr(Vec<J>),
    Obj(BTreeMap<String, J>),
}
impl J {
    pub fn get(&self, k: &str) -> &J {
        if let J::Obj(m) = self {
            m.get(k).unwrap_or(&J::Null)
        } else {
            &J::Null
        }
    }
    pub fn s(&self) -> Result<&str, String> {
        if let J::Str(v) = self {
            Ok(v)
        } else {
            Err("expected string".into())
        }
    }
    pub fn number(&self) -> Result<u64, String> {
        if let J::Decimal(token) = self {
            return exact_decimal_integer(token)
                .ok_or_else(|| "expected nonnegative integer".into());
        }
        if let J::UInt(n) = self {
            return Ok(*n);
        }
        Err("expected nonnegative integer".into())
    }
    pub fn dump(&self) -> String {
        match self {
            J::Null => "null".into(),
            J::Bool(b) => b.to_string(),
            J::Decimal(token) => token.clone(),
            J::UInt(n) => n.to_string(),
            J::Str(s) => quote(s),
            J::Arr(a) => format!("[{}]", a.iter().map(J::dump).collect::<Vec<_>>().join(",")),
            J::Obj(o) => format!(
                "{{{}}}",
                o.iter()
                    .map(|(k, v)| format!("{}:{}", quote(k), v.dump()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}
pub fn obj(p: Vec<(&str, J)>) -> J {
    J::Obj(p.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}
pub fn s(v: impl Into<String>) -> J {
    J::Str(v.into())
}
pub fn n(v: u64) -> J {
    J::UInt(v)
}
fn quote(s: &str) -> String {
    let mut o = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if c < ' ' => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}
pub fn parse(s: &str) -> Result<J, String> {
    if s.len() > 8 * 1024 * 1024 {
        return Err("JSON too large".into());
    }
    let mut p = P {
        c: s.chars().collect(),
        i: 0,
    };
    let v = p.v(0)?;
    p.ws();
    if p.i != p.c.len() {
        return Err("trailing JSON".into());
    }
    Ok(v)
}
struct P {
    c: Vec<char>,
    i: usize,
}
impl P {
    fn ws(&mut self) {
        while self.i < self.c.len() && matches!(self.c[self.i], ' ' | '\r' | '\n' | '\t') {
            self.i += 1
        }
    }
    fn take(&mut self) -> Result<char, String> {
        let c = self.c.get(self.i).copied().ok_or("unexpected end")?;
        self.i += 1;
        Ok(c)
    }
    fn expect(&mut self, c: char) -> Result<(), String> {
        if self.take()? == c {
            Ok(())
        } else {
            Err("unexpected JSON character".into())
        }
    }
    fn hex(&mut self) -> Result<u32, String> {
        let mut n = 0;
        for _ in 0..4 {
            n = n * 16 + self.take()?.to_digit(16).ok_or("bad unicode")?
        }
        Ok(n)
    }
    fn string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut s = String::new();
        loop {
            match self.take()? {
                '"' => return Ok(s),
                '\\' => match self.take()? {
                    '"' => s.push('"'),
                    '\\' => s.push('\\'),
                    '/' => s.push('/'),
                    'b' => s.push('\u{8}'),
                    'f' => s.push('\u{c}'),
                    'n' => s.push('\n'),
                    'r' => s.push('\r'),
                    't' => s.push('\t'),
                    'u' => {
                        let mut n = self.hex()?;
                        if (0xd800..=0xdbff).contains(&n) {
                            self.expect('\\')?;
                            self.expect('u')?;
                            let lo = self.hex()?;
                            if !(0xdc00..=0xdfff).contains(&lo) {
                                return Err("bad surrogate".into());
                            }
                            n = 0x10000 + ((n - 0xd800) << 10) + (lo - 0xdc00);
                        }
                        s.push(char::from_u32(n).ok_or("bad unicode scalar")?)
                    }
                    _ => return Err("bad escape".into()),
                },
                c if c < ' ' => return Err("control in string".into()),
                c => s.push(c),
            }
        }
    }
    fn v(&mut self, d: usize) -> Result<J, String> {
        if d > 64 {
            return Err("JSON nesting too deep".into());
        }
        self.ws();
        match self.c.get(self.i).copied().ok_or("empty JSON")? {
            '"' => Ok(J::Str(self.string()?)),
            '{' => {
                self.i += 1;
                self.ws();
                let mut m = BTreeMap::new();
                if self.c.get(self.i) == Some(&'}') {
                    self.i += 1;
                    return Ok(J::Obj(m));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    self.ws();
                    self.expect(':')?;
                    let v = self.v(d + 1)?;
                    if m.insert(k, v).is_some() {
                        return Err("duplicate key".into());
                    }
                    self.ws();
                    match self.take()? {
                        '}' => return Ok(J::Obj(m)),
                        ',' => (),
                        _ => return Err("bad object".into()),
                    }
                }
            }
            '[' => {
                self.i += 1;
                self.ws();
                let mut a = Vec::new();
                if self.c.get(self.i) == Some(&']') {
                    self.i += 1;
                    return Ok(J::Arr(a));
                }
                loop {
                    a.push(self.v(d + 1)?);
                    self.ws();
                    match self.take()? {
                        ']' => return Ok(J::Arr(a)),
                        ',' => (),
                        _ => return Err("bad array".into()),
                    }
                }
            }
            't' | 'f' | 'n' => {
                for (word, val) in [
                    ("true", J::Bool(true)),
                    ("false", J::Bool(false)),
                    ("null", J::Null),
                ] {
                    let cs: Vec<_> = word.chars().collect();
                    if self.c[self.i..].starts_with(&cs) {
                        self.i += cs.len();
                        return Ok(val);
                    }
                }
                Err("bad literal".into())
            }
            _ => self.num(),
        }
    }
    fn num(&mut self) -> Result<J, String> {
        let start = self.i;
        if self.c.get(self.i) == Some(&'-') {
            self.i += 1
        }
        match self.c.get(self.i) {
            Some('0') => self.i += 1,
            Some('1'..='9') => {
                self.i += 1;
                while matches!(self.c.get(self.i), Some('0'..='9')) {
                    self.i += 1
                }
            }
            _ => return Err("bad number".into()),
        }
        if self.c.get(self.i) == Some(&'.') {
            self.i += 1;
            let i = self.i;
            while matches!(self.c.get(self.i), Some('0'..='9')) {
                self.i += 1
            }
            if i == self.i {
                return Err("bad fraction".into());
            }
        }
        if matches!(self.c.get(self.i), Some('e' | 'E')) {
            self.i += 1;
            if matches!(self.c.get(self.i), Some('+' | '-')) {
                self.i += 1
            }
            let i = self.i;
            while matches!(self.c.get(self.i), Some('0'..='9')) {
                self.i += 1
            }
            if i == self.i {
                return Err("bad exponent".into());
            }
        }
        let token = self.c[start..self.i].iter().collect::<String>();
        if !token.contains(['-', '.', 'e', 'E']) {
            return token
                .parse::<u64>()
                .map(J::UInt)
                .map_err(|_| "integer overflow".into());
        }
        let n: f64 = token.parse().map_err(|_| "bad number")?;
        if !n.is_finite() {
            return Err("nonfinite number".into());
        }
        Ok(J::Decimal(token))
    }
}
// JSON grammar has already been validated. Work is linear in the spelling and
// never expands an exponent into a potentially enormous allocation.
fn exact_decimal_integer(token: &str) -> Option<u64> {
    let negative = token.starts_with('-');
    let unsigned = token.strip_prefix('-').unwrap_or(token);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let exp_negative = exponent.starts_with('-');
    let magnitude = exponent
        .trim_start_matches(['+', '-'])
        .bytes()
        .fold(0i64, |n, b| {
            n.saturating_mul(10).saturating_add(i64::from(b - b'0'))
        });
    let exp = if exp_negative { -magnitude } else { magnitude };
    let fractional = mantissa.split_once('.').map_or(0, |(_, f)| f.len()) as i64;
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let mut significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Some(0);
    }
    if negative {
        return None;
    }
    let mut scale = exp.saturating_sub(fractional);
    while scale < 0 && significant.ends_with('0') {
        significant = &significant[..significant.len() - 1];
        scale += 1;
    }
    if !(0..=16).contains(&scale) || significant.len() + scale as usize > 16 {
        return None;
    }
    let mut value = significant.parse::<u64>().ok()?;
    for _ in 0..scale {
        value = value.checked_mul(10)?;
    }
    (value <= 9_007_199_254_740_991).then_some(value)
}
