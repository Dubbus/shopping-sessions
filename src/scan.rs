//! One-pass CSV scanner: finds ',', '"' and '\n' 64 bytes at a time (AVX2 / NEON,
//! SWAR fallback), checks
//! quoting per field as it goes, and calls `f` with the N unquoted fields of every
//! well-formed line. Same dialect as csv.rs (which stays as the reference).

const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const ONES: u64 = 0x0101_0101_0101_0101;

/// High bit set in exactly the bytes of `x` that are zero.
#[allow(dead_code)]
#[inline(always)]
fn zero_bytes(x: u64) -> u64 {
    !(((x & LO7).wrapping_add(LO7)) | x | LO7)
}

#[allow(dead_code)]
#[inline(always)]
fn specials(w: u64) -> u64 {
    zero_bytes(w ^ (ONES * b',' as u64))
        | zero_bytes(w ^ (ONES * b'\n' as u64))
        | zero_bytes(w ^ (ONES * b'"' as u64))
}

/// Bit i set iff byte i of the 64-byte block is ',', '"' or '\n'.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn block_mask(b: &[u8; 64]) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let (c, n, q) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
        let half = |p: *const u8| -> u64 {
            let v = _mm256_loadu_si256(p as *const __m256i);
            let m = _mm256_or_si256(
                _mm256_or_si256(_mm256_cmpeq_epi8(v, c), _mm256_cmpeq_epi8(v, n)),
                _mm256_cmpeq_epi8(v, q),
            );
            _mm256_movemask_epi8(m) as u32 as u64
        };
        return half(b.as_ptr()) | (half(b.as_ptr().add(32)) << 32);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use std::arch::aarch64::*;
        let (c, n, q) = (vdupq_n_u8(b','), vdupq_n_u8(b'\n'), vdupq_n_u8(b'"'));
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let part = |i: usize| {
            let v = vld1q_u8(b.as_ptr().add(16 * i));
            let m = vorrq_u8(vorrq_u8(vceqq_u8(v, c), vceqq_u8(v, n)), vceqq_u8(v, q));
            vandq_u8(m, bits)
        };
        let s01 = vpaddq_u8(part(0), part(1));
        let s23 = vpaddq_u8(part(2), part(3));
        let s = vpaddq_u8(s01, s23);
        let s = vpaddq_u8(s, s);
        return vgetq_lane_u64(vreinterpretq_u64_u8(s), 0);
    }
    #[allow(unreachable_code)]
    {
        let mut m = 0u64;
        for k in 0..8 {
            let w = u64::from_le_bytes(b[8 * k..8 * k + 8].try_into().unwrap());
            let hi = specials(w) >> 7; // one bit at the bottom of each special byte
            m |= (hi.wrapping_mul(0x0102_0408_1020_4080) >> 56) << (8 * k);
        }
        m
    }
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
        let mut m = if pos + 64 <= len {
            block_mask(buf[pos..pos + 64].try_into().unwrap())
        } else {
            let mut t = [0u8; 64];
            t[..len - pos].copy_from_slice(&buf[pos..]);
            block_mask(&t)
        };
        while m != 0 {
            let i = pos + m.trailing_zeros() as usize;
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
        pos += 64;
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
        // SWAR fallback agrees with the SIMD mask
        let mut blk = [0u8; 64];
        let mut s2: u64 = 99;
        for _ in 0..2000 {
            for b in blk.iter_mut() {
                s2 = s2.wrapping_mul(6364136223846793005).wrapping_add(1);
                *b = b"a,\"\n\rz"[(s2 >> 33) as usize % 6];
            }
            let want = blk.iter().enumerate().fold(0u64, |m, (i, &c)| m | (((c == b',' || c == b'"' || c == b'\n') as u64) << i));
            assert_eq!(block_mask(&blk), want);
        }
        // random fuzz over a tiny alphabet
        let mut s: u64 = 12345;
        for _ in 0..20000 {
            let mut buf = Vec::new();
            for _ in 0..(s % 200) {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push(b"ab,\"\n\r"[(s >> 33) as usize % 6]);
            }
            assert_eq!(fast::<2>(&buf), reference::<2>(&buf), "{:?}", String::from_utf8_lossy(&buf));
            assert_eq!(fast::<3>(&buf), reference::<3>(&buf), "{:?}", String::from_utf8_lossy(&buf));
        }
    }
}
