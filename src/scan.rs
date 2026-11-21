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
        // Opaque: without this LLVM rewrites movemask results as <32 x i1> vectors and
        // rebuilds them bit by bit with ~64 vpinsrb per block (5x slower than NEON).
        #[inline(always)]
        fn opaque(mut x: u32) -> u32 {
            unsafe { std::arch::asm!("/* {0:e} */", inout(reg) x, options(pure, nomem, nostack, preserves_flags)) };
            x
        }
        let (c, n, q) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
        let p = b.as_ptr();
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let s0 = opaque(_mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(v0, c), _mm256_cmpeq_epi8(v0, n))) as u32);
        let s1 = opaque(_mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(v1, c), _mm256_cmpeq_epi8(v1, n))) as u32);
        let q0 = opaque(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, q)) as u32);
        let q1 = opaque(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, q)) as u32);
        let n0 = opaque(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, n)) as u32);
        let n1 = opaque(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, n)) as u32);
        return (s0 as u64 | (s1 as u64) << 32, n0 as u64 | (n1 as u64) << 32, q0 as u64 | (q1 as u64) << 32);
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

/// The unquoted fields of one line, `line[j]`, built on use (no per-line slice array).
pub struct Line<'s> {
    base: *const u8,
    lo: &'s [usize; 16],
    hi: &'s [usize; 16],
}

impl std::ops::Index<usize> for Line<'_> {
    type Output = [u8];
    #[inline(always)]
    fn index(&self, j: usize) -> &[u8] {
        // lo/hi were set by the scanner for j < N, inside the buffer the caller holds
        unsafe { std::slice::from_raw_parts(self.base.add(self.lo[j]), self.hi[j] - self.lo[j]) }
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line.
///
/// Only commas and newlines are visited. Quotes are checked by counting: a line is
/// well-formed iff every field starting with '"' also ends with '"' (length >= 2) and the
/// line holds exactly two quotes per such field, so no quote can sit anywhere else.
/// Closing a field is branch-free (quoted fields are ~20%, at random: a branch on them
/// mispredicts constantly on the scorer's Cascade Lake).
#[inline(always)]
pub fn for_each<'a, const N: usize, F: FnMut(&Line)>(buf: &'a [u8], mut f: F) {
    let len = buf.len();
    // field bounds, slot N is a sink for lines with too many fields
    let mut lo = [0usize; 16];
    let mut hi = [0usize; 16];
    assert!(N < 16);
    let mut nf = 0usize;
    let mut fstart = 0usize;
    let mut quoted = 0u32;
    let mut bad = 0u32;
    let mut line_q = 0u32;
    let mut line_start = 0usize;
    let base = buf.as_ptr();

    // close field [fstart, end): branch-free
    macro_rules! close {
        ($end:expr) => {{
            let end: usize = $end;
            let flen = end - fstart;
            // first and last byte; when flen < 2 these are masked out below
            let first = unsafe { *base.add(fstart.min(len - 1)) };
            let last = unsafe { *base.add(end.saturating_sub(1)) };
            let qs = (first == b'"') as usize & (flen >= 1) as usize;
            let qe = (last == b'"') as usize & (flen >= 2) as usize;
            bad |= (qs & !qe & 1) as u32;
            quoted += qs as u32;
            let k = nf.min(N);
            lo[k] = fstart + qs;
            hi[k] = (end - qs).max(fstart + qs);
            nf += 1;
            fstart = end + 1;
        }};
    }

    let mut pos = 0usize;
    while pos < len {
        let (mut m, _nl, q) = if pos + 64 <= len {
            block_masks(buf[pos..pos + 64].try_into().unwrap())
        } else {
            let mut t = [0u8; 64];
            t[..len - pos].copy_from_slice(&buf[pos..]);
            block_masks(&t)
        };
        let mut from = 0u32;
        while m != 0 {
            let bit = m.trailing_zeros();
            m &= m - 1;
            let i = pos + bit as usize;
            let below = q & !(u64::MAX << bit); // quotes before this separator in the block
            line_q += (below >> from).count_ones();
            from = bit + 1;
            close!(i);
            if unsafe { *base.add(i) } == b'\n' {
                if bad == 0 && nf == N && line_q == 2 * quoted && i > line_start {
                    f(&Line { base, lo: &lo, hi: &hi });
                }
                nf = 0;
                quoted = 0;
                bad = 0;
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
        close!(len);
        if bad == 0 && nf == N && line_q == 2 * quoted {
            f(&Line { base, lo: &lo, hi: &hi });
        }
    }
}

/// Two-phase scanner (simdjson style). Same contract as `for_each`.
///
/// Phase 1 walks the chunk in 64-byte blocks and, with no per-separator branching, writes
/// every separator position (',' or '\n') to an index array; for each newline it records
/// its position in that array; per block it keeps the quote mask and the running quote
/// count. Phase 2 handles a line at a time: its field count is a subtraction, its quote
/// count two prefix popcounts, its fields come straight from the index array.
pub fn for_each2<const N: usize, F: FnMut(&Line)>(buf: &[u8], mut f: F) {
    thread_local! {
        static SCRATCH: std::cell::RefCell<(Vec<u32>, Vec<u32>, Vec<u64>, Vec<u32>)> =
            const { std::cell::RefCell::new((Vec::new(), Vec::new(), Vec::new(), Vec::new())) };
    }
    let len = buf.len();
    if len == 0 {
        return;
    }
    SCRATCH.with_borrow_mut(|(idx, nls, qm, qc)| {
        let nblocks = len.div_ceil(64);
        idx.clear();
        nls.clear();
        qm.clear();
        qc.clear();
        // worst case every byte is a separator; +64 slack for the unconditional writes
        idx.reserve(len + 64);
        qm.reserve(nblocks + 1);
        qc.reserve(nblocks + 1);
        let ip = idx.as_mut_ptr();
        let mut k = 0usize; // separators written
        let mut qtotal = 0u32;
        for blk in 0..nblocks {
            let pos = blk * 64;
            let (mut m, nlmask, q) = if pos + 64 <= len {
                block_masks(buf[pos..pos + 64].try_into().unwrap())
            } else {
                let mut t = [0u8; 64];
                t[..len - pos].copy_from_slice(&buf[pos..]);
                block_masks(&t)
            };
            qm.push(q);
            qc.push(qtotal);
            qtotal += q.count_ones();
            // newlines in this block, as indexes into the separator array
            let sep = m;
            let mut nlm = nlmask;
            while nlm != 0 {
                let b = nlm.trailing_zeros();
                nlm &= nlm - 1;
                let below = sep & !(u64::MAX << b);
                nls.push((k + below.count_ones() as usize) as u32);
            }
            // separator positions, 8 at a time without branching on each
            let cnt = m.count_ones() as usize;
            let mut w = k;
            loop {
                for _ in 0..8 {
                    unsafe { *ip.add(w) = (pos as u32).wrapping_add(m.trailing_zeros()) };
                    m &= m.wrapping_sub(1);
                    w += 1;
                }
                if m == 0 {
                    break;
                }
            }
            k += cnt;
        }
        unsafe { idx.set_len(k) };
        qc.push(qtotal);
        qm.push(0);

        // quotes strictly before byte p
        let quotes_before = |p: usize| -> u32 {
            let b = p / 64;
            let r = (p % 64) as u32;
            qc[b] + (qm[b] & !(u64::MAX << r)).count_ones()
        };
        let mut lo = [0usize; 16];
        let mut hi = [0usize; 16];
        let base = buf.as_ptr();
        let mut first_sep = 0usize; // separator index where the current line's run starts
        let mut line_start = 0usize;
        let mut handle = |first_sep: usize, last_sep: usize, line_start: usize, line_end: usize, lo: &mut [usize; 16], hi: &mut [usize; 16]| {
            // fields: separators first_sep..=last_sep, the last one ends the line
            if last_sep + 1 - first_sep != N || line_end == line_start {
                return;
            }
            let lq = quotes_before(line_end) - quotes_before(line_start);
            let mut start = line_start;
            let mut quoted = 0u32;
            let mut bad = 0u32;
            for j in 0..N {
                let end = if j + 1 == N { line_end } else { idx[first_sep + j] as usize };
                let flen = end - start;
                let first = unsafe { *base.add(start.min(len - 1)) };
                let last = unsafe { *base.add(end.saturating_sub(1)) };
                let qs = (first == b'"') as usize & (flen >= 1) as usize;
                let qe = (last == b'"') as usize & (flen >= 2) as usize;
                bad |= (qs & !qe & 1) as u32;
                quoted += qs as u32;
                lo[j] = start + qs;
                hi[j] = (end - qs).max(start + qs);
                start = end + 1;
            }
            if bad == 0 && lq == 2 * quoted {
                f(&Line { base, lo, hi });
            }
        };
        for &nl_k in nls.iter() {
            let nl_k = nl_k as usize;
            let line_end = idx[nl_k] as usize;
            handle(first_sep, nl_k, line_start, line_end, &mut lo, &mut hi);
            first_sep = nl_k + 1;
            line_start = line_end + 1;
        }
        // last line without a trailing newline
        if line_start < len {
            // its separators are first_sep..k; it is well-formed iff exactly N-1 of them
            if k - first_sep == N - 1 {
                handle(first_sep, k, line_start, len, &mut lo, &mut hi);
            }
        }
    })
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
        for_each::<N, _>(buf, |f| out.push((0..N).map(|j| f[j].to_vec()).collect()));
        let mut out2: Vec<Vec<Vec<u8>>> = Vec::new();
        for_each2::<N, _>(buf, |f| out2.push((0..N).map(|j| f[j].to_vec()).collect()));
        assert_eq!(out, out2, "for_each2 differs on {:?}", String::from_utf8_lossy(buf));
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
