//! One-pass CSV scanner: finds ',', '"' and '\n' 8 bytes at a time (SWAR), checks
//! quoting per field as it goes, and calls `f` with the N unquoted fields of every
//! well-formed line. Same dialect as csv.rs (which stays as the reference).

const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const ONES: u64 = 0x0101_0101_0101_0101;

/// High bit set in exactly the bytes of `x` that are zero.
#[inline(always)]
fn zero_bytes(x: u64) -> u64 {
    !(((x & LO7).wrapping_add(LO7)) | x | LO7)
}

#[inline(always)]
fn specials(w: u64) -> u64 {
    zero_bytes(w ^ (ONES * b',' as u64))
        | zero_bytes(w ^ (ONES * b'\n' as u64))
        | zero_bytes(w ^ (ONES * b'"' as u64))
}

#[inline(always)]
#[allow(unused_assignments)]
pub fn for_each<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], mut f: F) {
    let len = buf.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields closed so far on this line
    let mut bad = false;
    let mut fstart = 0usize; // start of current field
    let mut quotes = 0u32;
    let mut first_q = usize::MAX;
    let mut last_q = 0usize;
    let mut line_start = 0usize;

    macro_rules! close_field {
        ($end:expr) => {{
            let end = $end;
            if nf >= N {
                bad = true;
            } else if quotes == 0 {
                fields[nf] = &buf[fstart..end];
            } else if quotes == 2 && first_q == fstart && last_q + 1 == end && end - fstart >= 2 {
                fields[nf] = &buf[fstart + 1..end - 1];
            } else {
                bad = true;
            }
            nf += 1;
            quotes = 0;
            first_q = usize::MAX;
            fstart = end + 1;
        }};
    }

    let mut pos = 0usize;
    while pos < len {
        let w = if pos + 8 <= len {
            u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap())
        } else {
            let mut t = [0u8; 8];
            t[..len - pos].copy_from_slice(&buf[pos..]);
            u64::from_le_bytes(t)
        };
        let mut m = specials(w);
        while m != 0 {
            let i = pos + (m.trailing_zeros() / 8) as usize;
            m &= m - 1;
            match buf[i] {
                b',' => close_field!(i),
                b'"' => {
                    if quotes == 0 {
                        first_q = i;
                    }
                    quotes += 1;
                    last_q = i;
                }
                _ => {
                    // '\n'
                    close_field!(i);
                    if !bad && nf == N && i > line_start {
                        f(&fields);
                    }
                    nf = 0;
                    bad = false;
                    line_start = i + 1;
                }
            }
        }
        pos += 8;
    }
    if line_start < len {
        close_field!(len);
        if !bad && nf == N {
            f(&fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv;

    fn reference<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for line in csv::lines(buf) {
            let mut f = [&b""[..]; N];
            if csv::fields(line, &mut f) {
                out.push(f.iter().map(|x| x.to_vec()).collect());
            }
        }
        out
    }

    fn fast<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for_each::<N, _>(buf, |f| out.push(f.iter().map(|x| x.to_vec()).collect()));
        out
    }

    #[test]
    fn matches_reference() {
        let cases: &[&[u8]] = &[
            b"a,b\n\"u4\",\"u1\"\n\"\",x\nu1,u5\"\nu1,u2,u3\n\nu1\n\"A\tB\",c\n\",x\n\"a\"b\",c\nlast,line",
            b"x,y\n",
            b"",
            b"\n\n",
            b"a,\"\"\"\n,\n\"\",\"\"",
            b"aaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\n\"q\"q,b\nz,\"y\"",
        ];
        for c in cases {
            assert_eq!(fast::<2>(c), reference::<2>(c), "{:?}", String::from_utf8_lossy(c));
        }
        // random fuzz over a tiny alphabet
        let mut s: u64 = 12345;
        for _ in 0..20000 {
            let mut buf = Vec::new();
            for _ in 0..(s % 40) {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push(b"ab,\"\n\r"[(s >> 33) as usize % 6]);
            }
            assert_eq!(fast::<2>(&buf), reference::<2>(&buf), "{:?}", String::from_utf8_lossy(&buf));
            assert_eq!(fast::<3>(&buf), reference::<3>(&buf), "{:?}", String::from_utf8_lossy(&buf));
        }
    }
}
