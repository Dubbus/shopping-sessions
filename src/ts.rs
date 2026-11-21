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

/// Days from 1970-01-01 to the first of each month, years 1900..2200 (fast path).
const Y0: i64 = 1900;
const NY: usize = 300;
static MONTH_START: std::sync::LazyLock<Vec<i64>> = std::sync::LazyLock::new(|| {
    let mut v = Vec::with_capacity(NY * 12);
    for y in 0..NY as i64 {
        for m in 1..=12 {
            v.push(days_from_civil(Y0 + y, m, 1));
        }
    }
    v
});

#[inline(always)]
fn d(b: u8) -> u32 {
    b.wrapping_sub(b'0') as u32
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
    // Fixed positions: YYYY-MM-DDTHH:MM:SS then Z or +HH:MM.
    let dg = [d(s[0]), d(s[1]), d(s[2]), d(s[3]), d(s[5]), d(s[6]), d(s[8]), d(s[9]), d(s[11]), d(s[12]), d(s[14]), d(s[15]), d(s[17]), d(s[18])];
    let mut bad = (s[7] != b'-') as u32 | (s[10] != b'T') as u32 | (s[13] != b':') as u32 | (s[16] != b':') as u32;
    for x in dg {
        bad |= (x > 9) as u32;
    }
    if bad != 0 {
        return None;
    }
    let y = (dg[0] * 1000 + dg[1] * 100 + dg[2] * 10 + dg[3]) as i64;
    let mo = (dg[4] * 10 + dg[5]) as i64;
    let dd = (dg[6] * 10 + dg[7]) as i64;
    let h = (dg[8] * 10 + dg[9]) as i64;
    let mi = (dg[10] * 10 + dg[11]) as i64;
    let se = (dg[12] * 10 + dg[13]) as i64;
    if !(1..=12).contains(&mo) || dd < 1 || dd > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
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
    let yi = y - Y0;
    let day0 = if (0..NY as i64).contains(&yi) {
        MONTH_START[yi as usize * 12 + mo as usize - 1]
    } else {
        days_from_civil(y, mo, 1)
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
