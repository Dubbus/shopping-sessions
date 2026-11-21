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

/// Per 64-byte block: (separators = ',' | '\n', newlines, quotes), bit i = byte i.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn block_masks(b: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let (c, n, q) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
        let half = |p: *const u8| -> (u64, u64, u64) {
            let v = _mm256_loadu_si256(p as *const __m256i);
            let mc = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
            let mn = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, n)) as u32 as u64;
            let mq = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32 as u64;
            (mc | mn, mn, mq)
        };
        let (s0, n0, q0) = half(b.as_ptr());
        let (s1, n1, q1) = half(b.as_ptr().add(32));
        return (s0 | s1 << 32, n0 | n1 << 32, q0 | q1 << 32);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use std::arch::aarch64::*;
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let fold = |m: [uint8x16_t; 4]| -> u64 {
            let s01 = vpaddq_u8(vandq_u8(m[0], bits), vandq_u8(m[1], bits));
            let s23 = vpaddq_u8(vandq_u8(m[2], bits), vandq_u8(m[3], bits));
            let s = vpaddq_u8(s01, s23);
            vgetq_lane_u64(vreinterpretq_u64_u8(vpaddq_u8(s, s)), 0)
        };
        let v = [vld1q_u8(b.as_ptr()), vld1q_u8(b.as_ptr().add(16)), vld1q_u8(b.as_ptr().add(32)), vld1q_u8(b.as_ptr().add(48))];
        let eq = |x: u8| { let d = vdupq_n_u8(x); [vceqq_u8(v[0], d), vceqq_u8(v[1], d), vceqq_u8(v[2], d), vceqq_u8(v[3], d)] };
        let (mc, mn, mq) = (fold(eq(b',')), fold(eq(b'\n')), fold(eq(b'"')));
        return (mc | mn, mn, mq);
    }
    #[allow(unreachable_code)]
    {
        let (mut sep, mut nl, mut qt) = (0u64, 0u64, 0u64);
        for (i, &c) in b.iter().enumerate() {
            sep |= ((c == b',' || c == b'\n') as u64) << i;
            nl |= ((c == b'\n') as u64) << i;
            qt |= ((c == b'"') as u64) << i;
        }
        (sep, nl, qt)
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line.
///
/// Only commas and newlines are visited. Quotes are checked by counting: a line is
/// well-formed iff every field starting with '"' also ends with '"' (length >= 2) and the
/// line holds exactly two quotes per such field, so no quote can sit anywhere else.
#[inline(always)]
pub fn for_each<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], mut f: F) {
    let len = buf.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields seen on this line (may exceed N)
    let mut fstart = 0usize;
    let mut quoted = 0u32; // fields on this line that start and end with '"'
    let mut bad = false;
    let mut line_q = 0u32; // quotes on this line so far
    let mut line_start = 0usize;

    let mut pos = 0usize;
    while pos < len {
        let (mut m, _nl, q) = if pos + 64 <= len {
            block_masks(buf[pos..pos + 64].try_into().unwrap())
        } else {
            let mut t = [0u8; 64];
            t[..len - pos].copy_from_slice(&buf[pos..]);
            let (s, n, q) = block_masks(&t);
            (s, n, q)
        };
        // quotes in this block before bit `from` are already counted
        let mut from = 0u32;
        while m != 0 {
            let bit = m.trailing_zeros();
            m &= m - 1;
            let i = pos + bit as usize;
            // quotes between the previous separator in this block and this one
            let below = if bit == 0 { 0 } else { q & (u64::MAX >> (64 - bit)) };
            line_q += (below >> from).count_ones();
            from = bit + 1;
            // close field [fstart, i)
            let fld = unsafe { buf.get_unchecked(fstart..i) };
            if nf < N {
                if fld.first() == Some(&b'"') {
                    if fld.len() >= 2 && fld[fld.len() - 1] == b'"' {
                        fields[nf] = &fld[1..fld.len() - 1];
                        quoted += 1;
                    } else {
                        bad = true;
                    }
                } else {
                    fields[nf] = fld;
                }
            }
            nf += 1;
            fstart = i + 1;
            if unsafe { *buf.get_unchecked(i) } == b'\n' {
                if !bad && nf == N && line_q == 2 * quoted && i > line_start {
                    f(&fields);
                }
                nf = 0;
                quoted = 0;
                bad = false;
                line_q = 0;
                line_start = i + 1;
            }
        }
        if from < 64 {
            line_q += (q >> from).count_ones();
        }
        pos += 64;
    }
    if line_start < len {
        let fld = &buf[fstart..len];
        if nf < N {
            if fld.first() == Some(&b'"') {
                if fld.len() >= 2 && fld[fld.len() - 1] == b'"' {
                    fields[nf] = &fld[1..fld.len() - 1];
                    quoted += 1;
                } else {
                    bad = true;
                }
            } else {
                fields[nf] = fld;
            }
        }
        nf += 1;
        if !bad && nf == N && line_q == 2 * quoted {
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
            let bit = |p: &dyn Fn(u8) -> bool| blk.iter().enumerate().fold(0u64, |m, (i, &c)| m | ((p(c) as u64) << i));
            let want = (bit(&|c| c == b',' || c == b'\n'), bit(&|c| c == b'\n'), bit(&|c| c == b'"'));
            assert_eq!(block_masks(&blk), want);
        }
        // random fuzz over a tiny alphabet
        let mut s: u64 = 12345;
        for _ in 0..200000 {
            let mut buf = Vec::new();
            for _ in 0..(s % 200) {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push(b"ab,\"\n\r"[(s >> 33) as usize % 6]);
            }
            assert_eq!(fast::<2>(&buf), reference::<2>(&buf), "{:?}", String::from_utf8_lossy(&buf));
            assert_eq!(fast::<3>(&buf), reference::<3>(&buf), "{:?}", String::from_utf8_lossy(&buf));
            assert_eq!(fast::<7>(&buf), reference::<7>(&buf), "{:?}", String::from_utf8_lossy(&buf));
        }
        // long lines crossing many blocks, quotes everywhere
        for _ in 0..3000 {
            let mut buf = Vec::new();
            for _ in 0..(s % 2000) {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push(b"aaaaaaaaab,,\"\"\n"[(s >> 33) as usize % 14]);
            }
            for n in [2usize, 4, 7] {
                match n {
                    2 => assert_eq!(fast::<2>(&buf), reference::<2>(&buf)),
                    4 => assert_eq!(fast::<4>(&buf), reference::<4>(&buf)),
                    _ => assert_eq!(fast::<7>(&buf), reference::<7>(&buf)),
                }
            }
        }
    }
}
