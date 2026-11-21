//! Timestamps F1 / F2 / F3 to epoch ms, exactly per rules.md. None = bad.

pub const SEC: i64 = 1_000;
pub const MIN: i64 = 60 * SEC;
pub const HOUR: i64 = 60 * MIN;
pub const DAY: i64 = 24 * HOUR;

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => if leap(y) { 29 } else { 28 },
    }
}

/// Days from 1970-01-01 to the first of each month, years 1900..2200, computed at
/// compile time (a plain static: no lazy-init check on the hot path).
const Y0: i64 = 1900;
const NY: usize = 300;
const fn dfc(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}
static MONTH_START: [i32; NY * 12] = {
    let mut v = [0i32; NY * 12];
    let mut i = 0;
    while i < NY * 12 {
        v[i] = dfc(Y0 + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    v
};
static DIM: [u8; NY * 12] = {
    let mut v = [0u8; NY * 12];
    let mut i = 0;
    while i < NY * 12 {
        let y = Y0 + (i / 12) as i64;
        let m = (i % 12) + 1;
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        v[i] = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => if leap { 29 } else { 28 },
        };
        i += 1;
    }
    v
};

#[inline(always)]
fn d(b: u8) -> u32 {
    b.wrapping_sub(b'0') as u32
}

/// Every byte of `x` (already XORed with '0') is a decimal digit 0..=9. Exact: a carry
/// out of an invalid byte can only add a flag next to a byte that is already invalid.
#[inline(always)]
fn all_digits(x: u64, mask: u64) -> bool {
    ((x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080 & mask) == 0
}

#[inline(always)]
fn byte(w: u64, i: u32) -> i64 {
    ((w >> (8 * i)) & 0xff) as i64
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 | 25 if s[4] == b'-' => civil(s),
        0 => None,
        _ => epoch_ms(s),
    }
}

/// F3: digits only, leading zeros fine; too big for i64 is out of every window = bad.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn epoch_ms(s: &[u8]) -> Option<i64> {
    if s.len() == 13 {
        // the common case: 13 digits, two loads, no loop
        let hi = u64::from_le_bytes(s[0..8].try_into().unwrap()) ^ 0x3030_3030_3030_3030;
        let lo = u64::from_le_bytes(s[5..13].try_into().unwrap()) ^ 0x3030_3030_3030_3030;
        if !all_digits(hi, u64::MAX) || !all_digits(lo, u64::MAX) {
            return None;
        }
        let mut v = 0i64;
        for i in 0..8 {
            v = v * 10 + byte(hi, i);
        }
        for i in 3..8 {
            v = v * 10 + byte(lo, i);
        }
        return Some(v);
    }
    if s.len() <= 18 {
        let mut v: i64 = 0;
        let mut bad = 0u32;
        for &c in s {
            let x = d(c);
            bad |= (x > 9) as u32;
            v = v * 10 + x as i64;
        }
        return if bad == 0 { Some(v) } else { None };
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    i64::try_from(v).ok()
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn civil(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS then Z or +HH:MM; three 8-byte words cover bytes 0..20.
    let w0 = u64::from_le_bytes(s[0..8].try_into().unwrap()); //  YYYY-MM-
    let w1 = u64::from_le_bytes(s[8..16].try_into().unwrap()); // DDTHH:MM
    let w2 = u64::from_le_bytes(s[12..20].try_into().unwrap()); // ?:??:SS? = s[12..20]; uses :SS
    const T0: u64 = u64::from_le_bytes(*b"0000-00-");
    const T1: u64 = u64::from_le_bytes(*b"00T00:00");
    const T2: u64 = u64::from_le_bytes([0, 0, 0, 0, b':', b'0', b'0', 0]);
    // digits where the template has '0', exact separators elsewhere (s[19] checked below)
    const D0: u64 = u64::from_le_bytes([0xff, 0xff, 0xff, 0xff, 0, 0xff, 0xff, 0]);
    const D1: u64 = u64::from_le_bytes([0xff, 0xff, 0, 0xff, 0xff, 0, 0xff, 0xff]);
    const D2: u64 = u64::from_le_bytes([0, 0, 0, 0, 0, 0xff, 0xff, 0]); // SS (s[17..19])
    let (x0, x1, x2) = (w0 ^ T0, w1 ^ T1, w2 ^ T2);
    let seps_ok = (x0 & !D0) == 0 && (x1 & !D1) == 0 && (x2 >> 32) & 0xff == 0; // s[16] == ':' 
    if !seps_ok || !all_digits(x0, D0) || !all_digits(x1, D1) || !all_digits(x2, D2) {
        return None;
    }
    let y = byte(x0, 0) * 1000 + byte(x0, 1) * 100 + byte(x0, 2) * 10 + byte(x0, 3);
    let mo = byte(x0, 5) * 10 + byte(x0, 6);
    let dd = byte(x1, 0) * 10 + byte(x1, 1);
    let h = byte(x1, 3) * 10 + byte(x1, 4);
    let mi = byte(x1, 6) * 10 + byte(x1, 7);
    let se = byte(x2, 5) * 10 + byte(x2, 6);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || dd < 1 {
        return None;
    }
    let yi = y - Y0;
    let (day0, dim) = if (0..NY as i64).contains(&yi) {
        let k = yi as usize * 12 + mo as usize - 1;
        (MONTH_START[k] as i64, DIM[k] as i64)
    } else {
        (days_from_civil(y, mo, 1), days_in_month(y, mo))
    };
    if dd > dim {
        return None;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let (a, b, c, e) = (d(s[20]), d(s[21]), d(s[23]), d(s[24]));
        if s[22] != b':' || a > 9 || b > 9 || c > 9 || e > 9 {
            return None;
        }
        let (oh, om) = ((a * 10 + b) as i64, (c * 10 + e) as i64);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * HOUR + om * MIN)
    };
    Some((day0 + dd - 1) * DAY + h * HOUR + mi * MIN + se * SEC - offset)
}

#[cfg(test)]
mod tests {
    use super::parse;
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5", b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z", b"2027-02-29T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10.000Z", b"99999999999999999999999",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        // fast path agrees with days_from_civil across years, including outside the table
        for (y, m, dd) in [(1899, 12, 31), (1900, 1, 1), (1970, 1, 1), (2026, 10, 3), (2028, 2, 29), (2199, 12, 31), (2200, 1, 1), (9999, 12, 31), (0, 3, 1)] {
            let txt = format!("{y:04}-{m:02}-{dd:02}T01:02:03Z");
            let want = super::days_from_civil(y, m, dd) * super::DAY + super::HOUR + 2 * super::MIN + 3 * super::SEC;
            assert_eq!(parse(txt.as_bytes()), Some(want), "{txt}");
        }
        assert_eq!(parse(b"000000000000000000001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse(b"9223372036854775808"), None);
    }
}

#[cfg(test)]
mod fuzz {
    /// The SWAR parser agrees with a plain reimplementation of the rules on random
    /// mutations of valid timestamps (every byte position, every byte class).
    fn reference(s: &[u8]) -> Option<i64> {
        let digits = |r: std::ops::Range<usize>| -> Option<i64> {
            let mut v = 0;
            for &c in &s[r] {
                if !c.is_ascii_digit() {
                    return None;
                }
                v = v * 10 + (c - b'0') as i64;
            }
            Some(v)
        };
        if s.is_empty() {
            return None;
        }
        if s.iter().all(|c| c.is_ascii_digit()) {
            let mut v: u64 = 0;
            for &c in s {
                v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
            }
            return i64::try_from(v).ok();
        }
        if s.len() != 20 && s.len() != 25 {
            return None;
        }
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let (y, mo, d) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
        let (h, mi, se) = (digits(11..13)?, digits(14..16)?, digits(17..19)?);
        if !(1..=12).contains(&mo) || d < 1 || d > super::days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let off = if s.len() == 20 {
            if s[19] != b'Z' {
                return None;
            }
            0
        } else {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let (oh, om) = (digits(20..22)?, digits(23..25)?);
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * super::HOUR + om * super::MIN)
        };
        Some(super::days_from_civil(y, mo, d) * super::DAY + h * super::HOUR + mi * super::MIN + se * super::SEC - off)
    }

    #[test]
    fn swar_matches_reference() {
        let seeds: &[&[u8]] = &[b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00+05:30", b"1999-12-31T23:59:59-10:00", b"1791071890000", b"0001791071890000"];
        let alphabet = b"0123456789:-TZtz+ .\\x00\\xff/";
        let mut r: u64 = 1;
        for _ in 0..400_000 {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let mut s = seeds[(r >> 40) as usize % seeds.len()].to_vec();
            for _ in 0..1 + (r >> 20) % 3 {
                r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let i = (r >> 33) as usize % s.len();
                s[i] = alphabet[(r >> 13) as usize % alphabet.len()];
            }
            assert_eq!(super::parse(&s), reference(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        // every valid day 1900..2200 and some outside the table
        for y in [1899, 1900, 1970, 2000, 2024, 2026, 2027, 2028, 2100, 2199, 2200, 2400] {
            for m in 1..=12 {
                for d in 1..=31 {
                    let t = format!("{y:04}-{m:02}-{d:02}T12:34:56Z");
                    assert_eq!(super::parse(t.as_bytes()), reference(t.as_bytes()), "{t}");
                }
            }
        }
    }
}
