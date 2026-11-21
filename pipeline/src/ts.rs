//! Timestamps: the three forms of rules.md, to integer milliseconds since the epoch.

pub const DAY_MS: i64 = 86_400_000;

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

/// Days from 1970-01-01 to the given civil date (proleptic Gregorian).
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 }
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SS` in `b[..19]`, to seconds since the epoch.
#[inline]
fn datetime_secs(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let (mo, d) = (d2(b, 5)?, d2(b, 8)?);
    let (h, mi, s) = (d2(b, 11)?, d2(b, 14)?, d2(b, 17)?);
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s)
}

/// Remembers the day number of recently seen `YYYY-MM-DD` prefixes. A window holds
/// only a few weeks of dates, so nearly every lookup is a hit.
pub struct Dates {
    seen: [(u64, u16); 128],
    days: [i64; 128],
}

impl Dates {
    pub fn new() -> Self {
        // No date starts with a zero byte, so the empty entries never match.
        Dates { seen: [(0, 0); 128], days: [0; 128] }
    }

    /// Days since the epoch of the date in `b[..10]`, or `None` if it is not a real date.
    #[inline]
    fn days(&mut self, b: &[u8]) -> Option<i64> {
        let key = (u64::from_le_bytes(b[..8].try_into().unwrap()), u16::from_le_bytes([b[8], b[9]]));
        let slot = ((b[8] & 15) as usize * 10 + (b[9] & 15) as usize) & 127;
        if self.seen[slot] == key {
            return Some(self.days[slot]);
        }
        if b[4] != b'-' || b[7] != b'-' {
            return None;
        }
        let y = d2(b, 0)? * 100 + d2(b, 2)?;
        let (mo, d) = (d2(b, 5)?, d2(b, 8)?);
        if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
            return None;
        }
        let days = days_from_civil(y, mo, d);
        self.seen[slot] = key;
        self.days[slot] = days;
        Some(days)
    }

    /// `YYYY-MM-DDTHH:MM:SS` in `b[..19]`, to seconds since the epoch.
    #[inline]
    fn datetime_secs(&mut self, b: &[u8]) -> Option<i64> {
        if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let (h, mi, s) = (d2(b, 11)?, d2(b, 14)?, d2(b, 17)?);
        if h > 23 || mi > 59 || s > 59 {
            return None;
        }
        Some(self.days(b)? * 86_400 + h * 3600 + mi * 60 + s)
    }

    /// F1 only (`ingest_ts` is always F1), to whole seconds since the epoch.
    #[inline]
    pub fn parse_f1_secs(&mut self, b: &[u8]) -> Option<i64> {
        if b.len() == 20 && b[19] == b'Z' { self.datetime_secs(b) } else { None }
    }

    /// Same result as [`parse`], faster on the dates it has seen.
    #[inline]
    pub fn parse(&mut self, b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n == 20 && b[19] == b'Z' {
            return Some(self.datetime_secs(b)? * 1000);
        }
        if n == 25 && (b[19] == b'+' || b[19] == b'-') {
            if b[22] != b':' {
                return None;
            }
            let (oh, om) = (d2(b, 20)?, d2(b, 23)?);
            if oh > 23 || om > 59 {
                return None;
            }
            let offset = (oh * 60 + om) * 60;
            let local = self.datetime_secs(b)?;
            return Some((if b[19] == b'+' { local - offset } else { local + offset }) * 1000);
        }
        parse(b)
    }
}

/// Parses F1, F2 or F3. `None` for anything else. A digits-only value too large
/// to matter (over 18 significant digits) is also `None`: it is outside every window.
#[inline]
pub fn parse(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[19] == b'Z' {
        return Some(datetime_secs(b)? * 1000);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') {
        if b[22] != b':' {
            return None;
        }
        let (oh, om) = (d2(b, 20)?, d2(b, 23)?);
        if oh > 23 || om > 59 {
            return None;
        }
        let offset = (oh * 60 + om) * 60;
        let local = datetime_secs(b)?;
        return Some((if b[19] == b'+' { local - offset } else { local + offset }) * 1000);
    }
    if n == 0 {
        return None;
    }
    let mut i = 0;
    while i < n && b[i] == b'0' {
        i += 1;
    }
    if n - i > 18 {
        // Still has to be digits only to be F3 at all; either way it is bad.
        return None;
    }
    let mut v: i64 = 0;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as i64;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Option<i64> {
        parse(s.as_bytes())
    }

    #[test]
    fn the_three_forms_agree() {
        let want = Some(1_791_071_890_000);
        assert_eq!(p("2026-10-03T23:58:10Z"), want);
        assert_eq!(p("2026-10-03T19:58:10-04:00"), want);
        assert_eq!(p("2026-10-04T01:58:10+02:00"), want);
        assert_eq!(p("1791071890000"), want);
        assert_eq!(p("0001791071890000"), want);
        assert_eq!(p("00000001791071890000"), want); // 20 bytes, still F3
        assert_eq!(p("0000000000001791071890000"), want); // 25 bytes, still F3
        assert_eq!(p("2026-10-03T23:58:10-00:00"), want);
        assert_eq!(p("2026-10-03T23:58:10+00:00"), want);
        assert_eq!(p("86400000"), Some(86_400_000));
        assert_eq!(p("0"), Some(0));
        assert_eq!(p("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(p("2028-02-29T00:00:00Z"), Some(1_835_395_200_000));
        assert_eq!(p("2000-02-29T12:00:00+23:59"), p("2000-02-28T12:01:00Z"));
    }

    #[test]
    fn bad_forms() {
        for s in [
            "", " ", "2026-10-03T23:58:10.000Z", "2026-10-03T23:58:10z", "2026-10-03t23:58:10Z",
            "2026-10-03 23:58:10Z", "2026-10-03T23:58:10", "1791071890000.0", "1.79e12", "-5", "+5",
            "2026-13-03T23:58:10Z", "2026-02-30T23:58:10Z", "2027-02-29T23:58:10Z", "2100-02-29T00:00:00Z",
            "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:58:60Z", "2026-00-03T23:58:10Z",
            "2026-10-00T23:58:10Z", "2026-10-03T23:58:10+24:00", "2026-10-03T23:58:10+05:60",
            "2026-10-03T23:58:10+0200", "2026-10-03T23:58:10+02-00", "2026-10-03T23:58:10 02:00",
            "2026-10-03T23:58:10Z\r", "1791071890000\r", " 1791071890000", "1791071890000 ",
            "٢٥٠", "２５０", "２026-10-03T23:58:10Z", "2026-1０-03T23:58:10Z", "2026/10/03T23:58:10Z",
            "999999999999999999999", "0x10", "1_000",
        ] {
            assert_eq!(p(s), None, "{s:?}");
        }
        // Long but all zeros in front: a real number.
        assert_eq!(p("000000000000000000000000000000012"), Some(12));
    }

    /// The cached parser must agree with the plain one on everything, in any order.
    #[test]
    fn cached_agrees() {
        let mut inputs: Vec<String> = Vec::new();
        for y in [0, 1, 1969, 1970, 2024, 2026, 2027, 2028, 2100, 9999] {
            for m in 0..=13 {
                for d in [0, 1, 9, 10, 28, 29, 30, 31, 32] {
                    for tail in ["T00:00:00Z", "T23:59:59Z", "T24:00:00Z", "T12:60:00Z", "T12:00:60Z", "t12:00:00Z", " 12:00:00Z",
                                 "T12:00:00+05:30", "T12:00:00-23:59", "T12:00:00+24:00", "T12:00:00-00:60", "T12:00:00+0530x", "T1a:00:00Z"] {
                        inputs.push(format!("{y:04}-{m:02}-{d:02}{tail}"));
                        inputs.push(format!("{y:04}/{m:02}-{d:02}{tail}"));
                        inputs.push(format!("{y:04}-{m:02}/{d:02}{tail}"));
                    }
                }
            }
        }
        for s in ["", "0", "1791071890000", "00000001791071890000", "0000000000001791071890000", "17910718900001791071890", "x"] {
            inputs.push(s.to_string());
        }
        let mut dates = Dates::new();
        for round in 0..3 {
            for (i, s) in inputs.iter().enumerate() {
                // Different orders, so entries are hit, replaced and hit again.
                let s = if round == 1 { &inputs[inputs.len() - 1 - i] } else { s };
                assert_eq!(dates.parse(s.as_bytes()), parse(s.as_bytes()), "{s:?}");
                let f1 = if s.len() == 20 && s.ends_with('Z') { parse(s.as_bytes()).map(|ms| ms / 1000) } else { None };
                assert_eq!(dates.parse_f1_secs(s.as_bytes()), f1, "{s:?}");
            }
        }
    }

    #[test]
    fn civil_days() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
    }
}
