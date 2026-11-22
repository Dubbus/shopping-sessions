//! The numbered path: the whole pipeline built on what the public generator's IDs are.
//!
//! The generator (tools/generator/src/text.rs, `scramble`) writes every event, click and
//! user ID as a prefix letter and base 36 of a number passed through a bijection on 40 bits
//! keyed per dataset. Events are numbered person by person and, within a person, in time
//! order; clicks are numbered person by person; a user number is `person * 512 + alias`.
//! With the three keys (recovered from a sample of the input) an ID turns back into its
//! number, and numbers index arrays:
//!
//!   parse    one pass over every file. A line is cut from comma and quote masks of its
//!            128 bytes; each event becomes a 16-byte record sent to the partition of its
//!            number (16,384 numbers each), each click likewise; link lines become edges
//!            inside a person or, rarely, between people.
//!   people   per person, which aliases the links join (a union-find in one word); edges
//!            between people (shared devices, odd IDs) go through an exact general table.
//!   clicks   per partition: placed by number (dedup), then laid out person by person.
//!   events   per partition: placed by number, smallest ingest_ts winning (dedup); then
//!            read in number order, which is each person's events, together and in time
//!            order, so sessions are made as the slots are read.
//!   slow     every row the numbering does not cover takes an exact general path keyed by
//!            the person's name.
//!
//! Nothing here is assumed without being checked: an ID that is not a canonical base-36
//! number takes the general path, and if the numbering's layout is not what is expected
//! (people out of order, values out of range) the run gives up before writing anything and
//! the ordinary pipeline (main.rs) does the whole job instead.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use hashbrown::HashMap;

use crate::key::{self, Id};
use crate::scatter::{Scatter, Scattered};
use crate::table::{Campaigns, Str, StrMap};
use crate::ts::{self, DAY_MS};
use crate::{GAP_MS, Res, WEEK_MS, csv, io, output, par, parse_price};

// ---- The generator's numbering --------------------------------------------------------------

const M40: u64 = (1 << 40) - 1;
const MUL_A: u64 = 0x9E37_79B9_7F4B;
const MUL_B: u64 = 0xD6E8_FEB8_6659;

const fn inverse(a: u64) -> u64 {
    // Newton's iteration for the inverse of an odd number modulo 2^64.
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}
const INV_A: u64 = inverse(MUL_A) & M40;
const INV_B: u64 = inverse(MUL_B) & M40;

/// The generator's `scramble`.
#[inline(always)]
pub fn scramble(value: u64, key: u64) -> u64 {
    let mut m = (value & M40).wrapping_mul(MUL_A) & M40 ^ (key & M40);
    m ^= m >> 17;
    m.wrapping_mul(MUL_B) & M40
}

/// `scramble`'s first step undone: what `value * MUL_A ^ key` was.
#[inline(always)]
fn unmix(y: u64) -> u64 {
    let m = y.wrapping_mul(INV_B) & M40;
    m ^ (m >> 17) ^ (m >> 34)
}

#[inline(always)]
pub fn unscramble(y: u64, key: u64) -> u64 {
    (unmix(y) ^ (key & M40)).wrapping_mul(INV_A) & M40
}

/// The key that makes `y` the scramble of `value`.
#[inline(always)]
fn key_for(y: u64, value: u64) -> u64 {
    unmix(y) ^ (value.wrapping_mul(MUL_A) & M40)
}

const LOW: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// The base-36 value of an ID written `prefix` + 1 to 8 lowercase base-36 characters with no
/// leading zero, as the generator writes them; `None` for any other ID.
///
/// # Safety
/// 8 bytes must be readable after `s`'s first byte.
#[inline(always)]
unsafe fn base36(s: &[u8], prefix: u8) -> Option<u64> {
    let len = s.len();
    if len < 2 || len > 9 || unsafe { *s.as_ptr() } != prefix {
        return None;
    }
    let w = u64::from_le(unsafe { std::ptr::read_unaligned(s.as_ptr().add(1) as *const u64) });
    base36_word(w, len - 1)
}

/// `base36` of the `n` (1 to 8) characters in the low bytes of `w`.
#[inline(always)]
fn base36_word(w: u64, n: usize) -> Option<u64> {
    if n == 0 || n > 8 {
        return None;
    }
    // Right-align the n characters (the last one in the top byte) and pad with '0' digits.
    let r = if n == 8 { w } else { (w << (8 * (8 - n))) | (0x3030_3030_3030_3030u64 >> (8 * n)) };
    if r & HIGH != 0 {
        return None;
    }
    let ge = |k: u8| (r + LOW * (0x80 - k as u64)) & HIGH;
    let digit = ge(b'0') & !ge(b'9' + 1);
    let lower = ge(b'a') & !ge(b'z' + 1);
    if digit | lower != HIGH {
        return None;
    }
    // No leading zero, unless the whole number is "0".
    if n > 1 && (r >> (8 * (8 - n))) as u8 == b'0' {
        return None;
    }
    let low_mask = (lower >> 7) * 0xFF;
    let d = r - (LOW * 0x30 + (low_mask & (LOW * 0x27)));
    let x = (d & 0x00FF_00FF_00FF_00FF) * 36 + ((d >> 8) & 0x00FF_00FF_00FF_00FF);
    let x = (x & 0x0000_FFFF_0000_FFFF) * 1296 + ((x >> 16) & 0x0000_FFFF_0000_FFFF);
    let v = (x & 0xFFFF_FFFF) * 1_679_616 + (x >> 32);
    if v > M40 { None } else { Some(v) }
}

/// Eight IDs at once: `words[i]` holds the bytes after the prefix and `chars[i]` how many
/// there are (0 = not an ID of the shape). Returns the unscrambled numbers and a mask of
/// the lanes that decoded, exactly as `base36_word` + `unscramble` would.
#[inline(always)]
fn decode8(words: &[u64; 8], chars: &[u64; 8], key: u64) -> ([u64; 8], u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512dq"))]
    unsafe {
        use std::arch::x86_64::*;
        let raw = _mm512_loadu_si512(words.as_ptr() as *const _);
        let n = _mm512_loadu_si512(chars.as_ptr() as *const _);
        let one = _mm512_set1_epi64(1);
        let eight = _mm512_set1_epi64(8);
        let len_ok = _mm512_cmpge_epu64_mask(n, one) & _mm512_cmple_epu64_mask(n, eight);
        let shift = _mm512_slli_epi64::<3>(_mm512_sub_epi64(eight, n));
        let pad = _mm512_srlv_epi64(_mm512_set1_epi64(0x3030_3030_3030_3030), _mm512_slli_epi64::<3>(n));
        let r = _mm512_or_si512(_mm512_sllv_epi64(raw, shift), pad);
        let digit = _mm512_cmpge_epu8_mask(r, _mm512_set1_epi8(b'0' as i8)) & _mm512_cmple_epu8_mask(r, _mm512_set1_epi8(b'9' as i8));
        let lower = _mm512_cmpge_epu8_mask(r, _mm512_set1_epi8(b'a' as i8)) & _mm512_cmple_epu8_mask(r, _mm512_set1_epi8(b'z' as i8));
        let bytes_ok = _mm512_cmpeq_epi64_mask(_mm512_movm_epi8(digit | lower), _mm512_set1_epi64(-1));
        let first = _mm512_and_si512(_mm512_srlv_epi64(r, shift), _mm512_set1_epi64(0xFF));
        let leading_zero = _mm512_cmpeq_epi64_mask(first, _mm512_set1_epi64(b'0' as i64)) & _mm512_cmpgt_epu64_mask(n, one);
        let d = _mm512_sub_epi8(r, _mm512_mask_blend_epi8(lower, _mm512_set1_epi8(0x30), _mm512_set1_epi8(0x57)));
        let x = _mm512_maddubs_epi16(d, _mm512_set1_epi16(0x0124));
        let x = _mm512_madd_epi16(x, _mm512_set1_epi32(0x0001_0510));
        let v = _mm512_add_epi64(_mm512_mul_epu32(x, _mm512_set1_epi64(1_679_616)), _mm512_srli_epi64::<32>(x));
        let m40 = _mm512_set1_epi64(M40 as i64);
        let in_range = _mm512_cmple_epu64_mask(v, m40);
        let ok = len_ok & bytes_ok & !leading_zero & in_range;
        let m = _mm512_and_si512(_mm512_mullo_epi64(v, _mm512_set1_epi64(INV_B as i64)), m40);
        let m = _mm512_xor_si512(m, _mm512_xor_si512(_mm512_srli_epi64::<17>(m), _mm512_srli_epi64::<34>(m)));
        let out = _mm512_and_si512(_mm512_mullo_epi64(_mm512_xor_si512(m, _mm512_set1_epi64((key & M40) as i64)), _mm512_set1_epi64(INV_A as i64)), m40);
        let mut values = [0u64; 8];
        _mm512_storeu_si512(values.as_mut_ptr() as *mut _, out);
        (values, ok)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512dq")))]
    {
        let mut values = [0u64; 8];
        let mut ok = 0u8;
        for i in 0..8 {
            if let Some(y) = base36_word(words[i], chars[i] as usize) {
                values[i] = unscramble(y, key);
                ok |= 1 << i;
            }
        }
        (values, ok)
    }
}

/// Writes `prefix` and base 36 of `value` into `out`; returns the length.
fn write_base36(out: &mut [u8; 32], prefix: u8, mut value: u64) -> usize {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut tmp = [0u8; 13];
    let mut at = tmp.len();
    loop {
        at -= 1;
        tmp[at] = DIGITS[(value % 36) as usize];
        value /= 36;
        if value == 0 {
            break;
        }
    }
    out[0] = prefix;
    let n = tmp.len() - at;
    out[1..1 + n].copy_from_slice(&tmp[at..]);
    1 + n
}

/// The packed key of the ID the generator writes for `number` under `key`.
#[inline]
fn id_of(prefix: u8, number: u64, k: u64) -> Id {
    let mut buf = [0u8; 32];
    let n = write_base36(&mut buf, prefix, scramble(number, k));
    // SAFETY: `buf` has 16 readable bytes after any ID of at most 9 bytes.
    unsafe { key::id(&buf[..n]) }
}

// ---- Records ----------------------------------------------------------------------------------

const SLOT_BITS: u32 = 14;
const SLOTS: usize = 1 << SLOT_BITS;
const SLOT_MASK: u64 = SLOTS as u64 - 1;
/// A user that is not a dense `person * 512 + alias` below `PERSON_LIMIT`.
const SLOW: u32 = u32::MAX;
const PERSON_LIMIT: u32 = (1 << 23) - 1;
const ING_BITS: u32 = 21;
const ING_LIMIT: i64 = (1 << ING_BITS) - 1;
const EMPTY: u64 = u64::MAX;

/// One event line. `a` = ingest (21 bits, seconds from `ing_base`) << 43 | slot << 29 |
/// category << 24 | price (0 unless a valid purchase). `b` = valid << 63 | ts << 32 | user,
/// ts in ms from `base`. Only `a`'s ingest and slot mean anything for an invalid copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvRec {
    a: u64,
    b: u64,
}

/// One valid click. `a` = slot << 48 | campaign << 32 | ts (ms from `base`); `b` = user.
#[derive(Clone, Copy)]
#[repr(C)]
struct CkRec {
    a: u64,
    b: u64,
}

/// An event whose ID is not a number below the cap.
#[derive(Clone, Copy)]
struct OddEvent {
    id: Id,
    ingest: i64,
    valid: bool,
    ev: u64,
    user: u32,
    user_id: Id,
}

/// A valid click whose ID is not a number below the cap.
#[derive(Clone, Copy)]
struct OddClick {
    id: Id,
    ts: u32,
    campaign: u32,
    user: u32,
    user_id: Id,
}

/// A click laid out by person, in number order.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Click {
    person: u32,
    ts: u32,
    number: u32,
    campaign: u32,
}

// ---- Giving up -------------------------------------------------------------------------------

static GIVE_UP: Mutex<Option<&'static str>> = Mutex::new(None);

#[cold]
fn give_up(why: &'static str) {
    let mut g = GIVE_UP.lock().unwrap();
    if g.is_none() {
        *g = Some(why);
    }
}

fn given_up() -> Option<&'static str> {
    *GIVE_UP.lock().unwrap()
}

// ---- Line cutting -------------------------------------------------------------------------------

/// Comma and quote masks of the 128 bytes at `p`.
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u128, u128) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm512_loadu_si512(p as *const _);
        let hi = _mm512_loadu_si512(p.add(64) as *const _);
        let comma = _mm512_set1_epi8(b',' as i8);
        let quote = _mm512_set1_epi8(b'"' as i8);
        let c = (_mm512_cmpeq_epi8_mask(hi, comma) as u128) << 64 | _mm512_cmpeq_epi8_mask(lo, comma) as u128;
        let q = (_mm512_cmpeq_epi8_mask(hi, quote) as u128) << 64 | _mm512_cmpeq_epi8_mask(lo, quote) as u128;
        (c, q)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw")))]
    {
        let (mut c, mut q) = (0u128, 0u128);
        for i in 0..128 {
            let b = unsafe { *p.add(i) };
            c |= ((b == b',') as u128) << i;
            q |= ((b == b'"') as u128) << i;
        }
        (c, q)
    }
}

#[inline(always)]
fn pdep(x: u64, m: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pdep_u64(x, m)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let (mut out, mut m, mut x) = (0u64, m, x);
        while m != 0 {
            let low = m & m.wrapping_neg();
            if x & 1 != 0 {
                out |= low;
            }
            x >>= 1;
            m &= m - 1;
        }
        out
    }
}

#[inline(always)]
fn pext(x: u64, m: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(x, m)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let (mut out, mut m, mut k) = (0u64, m, 0);
        while m != 0 {
            let low = m & m.wrapping_neg();
            if x & low != 0 {
                out |= 1 << k;
            }
            k += 1;
            m &= m - 1;
        }
        out
    }
}

/// The bits of `x` under the mask `m`, packed: `pext` on 128 bits.
#[inline(always)]
fn pext128(x: u128, m: u128) -> u64 {
    let (ml, mh) = (m as u64, (m >> 64) as u64);
    pext(x as u64, ml) | pext((x >> 64) as u64, mh) << ml.count_ones()
}

/// Cuts the line `p[..len]` (no newline) into `N` unquoted fields, as `(start, end)` offsets.
/// `false` if the line is malformed. 128 bytes must be readable at `p`.
///
/// The commas are picked out independently (`pdep` selects the i-th one), and the quoting
/// is checked for the whole line at once: in positions shifted up by one, every quote must
/// sit on a field's first byte or its last, a field must have both or neither, and no field
/// may be a lone quote.
#[inline(always)]
unsafe fn cut<const N: usize>(p: *const u8, len: usize, out: &mut [(u32, u32); N]) -> bool {
    if len > 126 {
        return cut_long(unsafe { std::slice::from_raw_parts(p, len) }, out);
    }
    let live = (1u128 << len) - 1;
    let (c, q) = unsafe { masks(p) };
    let (c, q) = (c & live, q & live);
    if c.count_ones() as usize != N - 1 {
        return false;
    }
    let (lo, hi) = (c as u64, (c >> 64) as u64);
    let k_lo = lo.count_ones() as usize;
    let mut pos = [0u32; N];
    for (i, p) in pos.iter_mut().enumerate().take(N - 1) {
        *p = if i < k_lo { pdep(1 << i, lo).trailing_zeros() } else { 64 + pdep(1 << (i - k_lo), hi).trailing_zeros() };
    }
    pos[N - 1] = len as u32;
    let mut wrapped = 0u64;
    if q != 0 {
        let q1 = q << 1;
        let starts = c << 2 | 2;
        let ends = c | 1u128 << len;
        let (qs, qe) = (q1 & starts, q1 & ends);
        if q1 != qs | qe || qs & qe != 0 {
            return false;
        }
        wrapped = pext128(qs, starts);
        if wrapped != pext128(qe, ends) {
            return false;
        }
    }
    let mut from = 0u32;
    for i in 0..N {
        let w = (wrapped >> i) as u32 & 1;
        out[i] = (from + w, pos[i] - w);
        from = pos[i] + 1;
    }
    true
}

#[cold]
fn cut_long<const N: usize>(line: &[u8], out: &mut [(u32, u32); N]) -> bool {
    let Some(fields) = csv::split::<N>(line) else { return false };
    let base = line.as_ptr() as usize;
    for (o, f) in out.iter_mut().zip(fields) {
        let s = f.as_ptr() as usize - base;
        *o = (s as u32, (s + f.len()) as u32);
    }
    true
}

/// Positions of every `\n` in `buf[from..to]`, appended to `out`.
#[inline(always)]
fn newlines(buf: &[u8], from: usize, to: usize, out: &mut Vec<u32>) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm512_set1_epi8(b'\n' as i8);
        out.reserve((to - from) / 16 + 64);
        let mut base = from;
        while base < to {
            let v = _mm512_loadu_si512(buf.as_ptr().add(base) as *const _);
            let live = if to - base >= 64 { u64::MAX } else { (1u64 << (to - base)) - 1 };
            let mut bits = _mm512_cmpeq_epi8_mask(v, nl) & live;
            while bits != 0 {
                let at = out.len();
                *out.as_mut_ptr().add(at) = (base as u32) + bits.trailing_zeros();
                out.set_len(at + 1);
                bits &= bits - 1;
            }
            base += 64;
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw")))]
    out.extend(memchr::memchr_iter(b'\n', &buf[from..to]).map(|i| (from + i) as u32));
}

/// Bytes of a chunk scanned for newlines at a time, then cut while still in cache.
const PIECE: usize = 16 << 10;

/// Calls `f(start, len)` for each line starting in `buf[first..last)` (data `buf[..len]`),
/// with `csv::for_each_line`'s rule for the header and for a chunk starting mid-line.
#[inline(always)]
fn for_each_line(buf: &[u8], len: usize, first: usize, last: usize, nl: &mut Vec<u32>, mut f: impl FnMut(usize, usize)) {
    // Where the first line of this chunk starts.
    let mut pos = if first != 0 && buf[first - 1] == b'\n' {
        first
    } else {
        match memchr::memchr(b'\n', &buf[first..len]) {
            Some(i) => first + i + 1,
            None => return,
        }
    };
    let mut scanned = pos;
    while pos < last && pos < len {
        nl.clear();
        let to = (scanned + PIECE).min(len);
        newlines(buf, scanned, to, nl);
        scanned = to;
        if to == len && (len == 0 || buf[len - 1] != b'\n') {
            nl.push(len as u32);
        }
        for &end in nl.iter() {
            if pos >= last {
                return;
            }
            let end = end as usize;
            f(pos, end - pos);
            pos = end + 1;
        }
    }
}

// ---- Lookups -----------------------------------------------------------------------------------

/// Product ID -> category. `p<digits>` with no leading zero is looked up by number.
struct Products {
    by_number: Vec<u8>,
    others: StrMap,
    unknown: u32,
}

const NO_PRODUCT: u8 = u8::MAX;

#[inline(always)]
fn product_number(s: &[u8]) -> Option<usize> {
    if !(2..=8).contains(&s.len()) || s[0] != b'p' || (s[1] == b'0' && s.len() > 2) {
        return None;
    }
    let mut n = 0usize;
    for &c in &s[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as usize;
    }
    if n < 1 << 22 { Some(n) } else { None }
}

impl Products {
    #[inline(always)]
    fn category(&self, s: &[u8]) -> u32 {
        match product_number(s) {
            Some(n) => match self.by_number.get(n) {
                Some(&c) if c != NO_PRODUCT => c as u32,
                _ => self.unknown,
            },
            None => self.others.get(s).unwrap_or(self.unknown),
        }
    }
}

// ---- Inputs and context -------------------------------------------------------------------------

pub struct Inputs<'a> {
    pub data_dir: &'a Path,
    pub out_dir: &'a Path,
    pub threads: usize,
    pub window_start: i64,
    pub window_end: i64,
    pub products: &'a [(Str, u32)],
    pub unknown_category: u32,
    pub category_names: &'a [Str],
    pub campaigns: &'a Campaigns,
    pub campaign_names: &'a [Str],
    pub channels: &'a [Str],
    pub organic: usize,
}

struct Keys {
    user: u64,
    event: u64,
    click: u64,
}

struct Cx<'a> {
    keys: Keys,
    window_start: i64,
    window_end: i64,
    base: i64,
    ing_base: i64,
    ecap: u64,
    ccap: u64,
    products: Products,
    campaigns: &'a Campaigns,
    /// Benchmarking only (SS_DX): stop each event line early. 1 after the cut, 2 after
    /// ingest_ts, 3 before the record is pushed; 9 = skip event lines entirely.
    dx: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Events,
    Clicks,
    Links,
}

/// One parse thread's output.
struct Parsed {
    events: Option<Scatter<EvRec>>,
    clicks: Option<Scatter<CkRec>>,
    odd_events: Vec<OddEvent>,
    /// (number, ingest) -> user ID, for event records whose user is `SLOW`.
    slow_event_users: Vec<(u64, Id)>,
    odd_clicks: Vec<OddClick>,
    slow_click_users: Vec<(u32, Id)>,
    /// person << 18 | alias << 9 | alias.
    inner_links: Vec<u64>,
    outer_links: Vec<(Id, Id)>,
    outer_people: Vec<u32>,
    max_person: u32,
    dates: ts::Dates,
    nl: Vec<u32>,
    buf: Vec<u8>,
    ebatch: Batch,
    cbatch: Batch,
}

// SAFETY: after the parse, a `Parsed` is only read; its scatters are taken out by their owner,
// and its batches are always empty between chunks (their pointers are never followed then).
unsafe impl Sync for Parsed {}
unsafe impl Send for Parsed {}

/// The dense user number, `SLOW` if it is not one.
#[inline(always)]
unsafe fn user_number(s: &[u8], key: u64) -> u32 {
    match unsafe { base36(s, b'u') } {
        Some(y) => {
            let v = unscramble(y, key);
            if v < (PERSON_LIMIT as u64) << 9 { v as u32 } else { SLOW }
        }
        None => SLOW,
    }
}

/// Lines waiting to have their IDs decoded eight at a time.
#[derive(Clone, Copy)]
struct Batch {
    n: usize,
    p: [*const u8; 8],
    f: [[(u32, u32); 7]; 8],
}

impl Default for Batch {
    fn default() -> Self {
        Batch { n: 0, p: [std::ptr::null(); 8], f: [[(0, 0); 7]; 8] }
    }
}

/// The raw word and character count of field `i` of each line, if it starts with `prefix`.
#[inline(always)]
unsafe fn id_words(b: &Batch, i: usize, prefix: u8) -> ([u64; 8], [u64; 8]) {
    let (mut words, mut chars) = ([0u64; 8], [0u64; 8]);
    for lane in 0..b.n {
        let (s, e) = b.f[lane][i];
        let at = unsafe { b.p[lane].add(s as usize) };
        let len = (e - s) as u64;
        if len >= 2 && unsafe { *at } == prefix {
            words[lane] = u64::from_le(unsafe { std::ptr::read_unaligned(at.add(1) as *const u64) });
            chars[lane] = len - 1;
        }
    }
    (words, chars)
}

/// The dense user number from a decoded value, `SLOW` if it is not one.
#[inline(always)]
fn dense_user(value: u64, ok: bool) -> u32 {
    if ok && value < (PERSON_LIMIT as u64) << 9 { value as u32 } else { SLOW }
}

#[inline(always)]
unsafe fn event_line(out: &mut Parsed, cx: &Cx, p: *const u8, len: usize) {
    if cx.dx == 9 {
        return;
    }
    let n = out.ebatch.n;
    if !unsafe { cut::<7>(p, len, &mut out.ebatch.f[n]) } {
        return;
    }
    if cx.dx == 1 {
        std::hint::black_box(&out.ebatch.f[n]);
        return;
    }
    out.ebatch.p[n] = p;
    out.ebatch.n = n + 1;
    if n + 1 == 8 {
        unsafe { event_batch(out, cx) };
    }
}

unsafe fn event_batch(out: &mut Parsed, cx: &Cx) {
    let b = out.ebatch;
    out.ebatch.n = 0;
    let (ew, ec) = unsafe { id_words(&b, 0, b'e') };
    let (uw, uc) = unsafe { id_words(&b, 1, b'u') };
    let (ev_no, ev_ok) = decode8(&ew, &ec, cx.keys.event);
    let (us_no, us_ok) = decode8(&uw, &uc, cx.keys.user);
    for lane in 0..b.n {
        let number = if ev_ok >> lane & 1 != 0 && ev_no[lane] < cx.ecap { Some(ev_no[lane]) } else { None };
        let user_no = dense_user(us_no[lane], us_ok >> lane & 1 != 0);
        unsafe { event_rest(out, cx, b.p[lane], &b.f[lane], number, user_no) };
    }
}

#[inline(always)]
unsafe fn event_rest(out: &mut Parsed, cx: &Cx, p: *const u8, f: &[(u32, u32); 7], number: Option<u64>, user_no: u32) {
    let field = |i: usize| unsafe { std::slice::from_raw_parts(p.add(f[i].0 as usize), (f[i].1 - f[i].0) as usize) };
    let id = field(0);
    if id.is_empty() {
        return;
    }
    let Some(ingest) = out.dates.parse_f1_secs(field(3)) else { return };
    if cx.dx == 2 {
        std::hint::black_box(ingest);
        return;
    }
    let (user, product, kind) = (field(1), field(5), field(4));
    let mut valid = false;
    let mut ev = 0u64;
    'valid: {
        if user.is_empty() || product.is_empty() {
            break 'valid;
        }
        let Some(t) = out.dates.parse(field(2)) else { break 'valid };
        if t < cx.window_start || t >= cx.window_end || ingest * 1000 - t > DAY_MS {
            break 'valid;
        }
        let mut purchase = 0u64;
        if kind.len() == 4 {
            let w = u32::from_le_bytes(unsafe { *(kind.as_ptr() as *const [u8; 4]) });
            if w != u32::from_le_bytes(*b"view") && w != u32::from_le_bytes(*b"cart") {
                break 'valid;
            }
        } else if kind.len() == 8 && u64::from_le_bytes(unsafe { *(kind.as_ptr() as *const [u8; 8]) }) == u64::from_le_bytes(*b"purchase") {
            let Some(cents) = parse_price(field(6)) else { break 'valid };
            purchase = (cx.products.category(product) as u64) << 24 | cents as u64;
        } else {
            break 'valid;
        }
        valid = true;
        ev = ((t - cx.base) as u64) << 32 | purchase;
        if user_no != SLOW {
            out.max_person = out.max_person.max(user_no >> 9);
        }
    }
    let user_no = if valid { user_no } else { 0 };
    let Some(n) = number else {
        out.odd_events.push(OddEvent {
            id: unsafe { key::id(id) },
            ingest,
            valid,
            ev,
            user: user_no,
            user_id: if valid && user_no == SLOW { unsafe { key::id(user) } } else { 0 },
        });
        return;
    };
    let ing = ingest - cx.ing_base;
    if !(0..ING_LIMIT).contains(&ing) {
        give_up("ingest_ts out of the stored range");
        return;
    }
    let a = (ing as u64) << 43 | (n & SLOT_MASK) << 29 | (ev & 0x1FFF_FFFF);
    let b = if valid { 1u64 << 63 | (ev >> 32) << 32 | user_no as u64 } else { 0 };
    if cx.dx == 3 {
        std::hint::black_box((a, b, n));
        return;
    }
    out.events.as_mut().unwrap().push((n >> SLOT_BITS) as usize, EvRec { a, b });
    if valid && user_no == SLOW {
        out.slow_event_users.push((n << ING_BITS | ing as u64, unsafe { key::id(user) }));
    }
}

#[inline(always)]
unsafe fn click_line(out: &mut Parsed, cx: &Cx, p: *const u8, len: usize) {
    let n = out.cbatch.n;
    let mut f = [(0u32, 0u32); 4];
    if !unsafe { cut::<4>(p, len, &mut f) } {
        return;
    }
    out.cbatch.f[n][..4].copy_from_slice(&f);
    out.cbatch.p[n] = p;
    out.cbatch.n = n + 1;
    if n + 1 == 8 {
        unsafe { click_batch(out, cx) };
    }
}

unsafe fn click_batch(out: &mut Parsed, cx: &Cx) {
    let b = out.cbatch;
    out.cbatch.n = 0;
    let (kw, kc) = unsafe { id_words(&b, 0, b'k') };
    let (uw, uc) = unsafe { id_words(&b, 1, b'u') };
    let (ck_no, ck_ok) = decode8(&kw, &kc, cx.keys.click);
    let (us_no, us_ok) = decode8(&uw, &uc, cx.keys.user);
    for lane in 0..b.n {
        let number = if ck_ok >> lane & 1 != 0 && ck_no[lane] < cx.ccap { Some(ck_no[lane]) } else { None };
        let user_no = dense_user(us_no[lane], us_ok >> lane & 1 != 0);
        unsafe { click_rest(out, cx, b.p[lane], &b.f[lane], number, user_no) };
    }
}

#[inline(always)]
unsafe fn click_rest(out: &mut Parsed, cx: &Cx, p: *const u8, f: &[(u32, u32); 7], number: Option<u64>, user_no: u32) {
    let field = |i: usize| unsafe { std::slice::from_raw_parts(p.add(f[i].0 as usize), (f[i].1 - f[i].0) as usize) };
    let (id, user) = (field(0), field(1));
    if id.is_empty() || user.is_empty() {
        return;
    }
    let Some(campaign) = cx.campaigns.get(field(2)) else { return };
    let Some(t) = out.dates.parse(field(3)) else { return };
    if t < cx.base || t >= cx.window_end {
        return;
    }
    let ts = (t - cx.base) as u32;
    if user_no != SLOW {
        out.max_person = out.max_person.max(user_no >> 9);
    }
    let Some(m) = number else {
        out.odd_clicks.push(OddClick {
            id: unsafe { key::id(id) },
            ts,
            campaign,
            user: user_no,
            user_id: if user_no == SLOW { unsafe { key::id(user) } } else { 0 },
        });
        return;
    };
    let a = (m & SLOT_MASK) << 48 | (campaign as u64) << 32 | ts as u64;
    out.clicks.as_mut().unwrap().push((m >> SLOT_BITS) as usize, CkRec { a, b: user_no as u64 });
    if user_no == SLOW {
        out.slow_click_users.push((m as u32, unsafe { key::id(user) }));
    }
}

#[inline(always)]
unsafe fn link_line(out: &mut Parsed, cx: &Cx, p: *const u8, len: usize) {
    let mut f = [(0u32, 0u32); 2];
    if !unsafe { cut::<2>(p, len, &mut f) } {
        return;
    }
    let field = |i: usize| unsafe { std::slice::from_raw_parts(p.add(f[i].0 as usize), (f[i].1 - f[i].0) as usize) };
    let (a, b) = (field(0), field(1));
    if a.is_empty() || b.is_empty() {
        return;
    }
    let (ua, ub) = unsafe { (user_number(a, cx.keys.user), user_number(b, cx.keys.user)) };
    if ua != SLOW && ub != SLOW && ua >> 9 == ub >> 9 {
        let person = ua >> 9;
        out.max_person = out.max_person.max(person);
        if ua != ub {
            out.inner_links.push((person as u64) << 18 | ((ua & 511) as u64) << 9 | (ub & 511) as u64);
        }
        return;
    }
    for u in [ua, ub] {
        if u != SLOW {
            out.outer_people.push(u >> 9);
            out.max_person = out.max_person.max(u >> 9);
        }
    }
    out.outer_links.push(unsafe { (key::id(a), key::id(b)) });
}

// ---- Keys from a sample ---------------------------------------------------------------------------

/// Lines of a few event and click files, for finding the keys.
fn sample_lines(files: &[(PathBuf, Kind, u64)], kind: Kind, want: usize) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut picked: Vec<&(PathBuf, Kind, u64)> = files.iter().filter(|f| f.1 == kind && f.2 > 0).collect();
    picked.sort_by_key(|f| std::cmp::Reverse(f.2));
    for (path, _, len) in picked.into_iter().take(4) {
        let Ok(file) = File::open(path) else { continue };
        let take = (*len).min(512 << 10);
        let mut buf = Vec::new();
        let Ok((n, _)) = io::load_chunk(&file, *len, 0, take, &mut buf) else { continue };
        csv::for_each_line(&buf[..n], 0, n, |line| lines.push(line.to_vec()));
        if lines.len() >= want {
            break;
        }
    }
    lines
}

/// Finds the key under which the IDs `ys` all scramble numbers below `limit`, given
/// that `ys[0]`'s number is below `limit` too. Brute force over `ys[0]`'s number.
fn search_key(ys: &[u64], limit: u64, threads: usize) -> Option<u64> {
    if ys.len() < 8 {
        return None;
    }
    (0..4).find_map(|r| search_from(ys, r, limit, threads))
}

fn search_from(ys: &[u64], r: usize, limit: u64, threads: usize) -> Option<u64> {
    let (y0, y1, y2) = (ys[r], ys[(r + 1) % ys.len()], ys[(r + 2) % ys.len()]);
    let parts = (threads * 8) as u64;
    let found = par::run(threads, parts as usize, |_| Vec::<u64>::new(), |hits, part| {
        let from = limit * part as u64 / parts;
        let to = limit * (part as u64 + 1) / parts;
        for v in from..to {
            let k = key_for(y0, v);
            if unscramble(y1, k) < limit && unscramble(y2, k) < limit {
                hits.push(k);
            }
        }
    });
    // Most of the sample must agree: a few rows can belong elsewhere (revised users).
    found.into_iter().flatten().find(|&k| ys.iter().filter(|&&y| unscramble(y, k) < limit).count() * 10 >= ys.len() * 9)
}

fn find_keys(files: &[(PathBuf, Kind, u64)], est_events: u64, est_clicks: u64, threads: usize) -> Option<Keys> {
    let events = sample_lines(files, Kind::Events, 16_000);
    let clicks = sample_lines(files, Kind::Clicks, 8_000);
    let decode = |s: &[u8], prefix: u8| -> Option<u64> {
        let mut padded = [0u8; 32];
        if s.len() > 16 {
            return None;
        }
        padded[..s.len()].copy_from_slice(s);
        unsafe { base36(&padded[..s.len()], prefix) }
    };
    let ev_fields: Vec<[Vec<u8>; 2]> =
        events.iter().filter_map(|l| csv::split::<7>(l)).map(|f| [f[0].to_vec(), f[1].to_vec()]).collect();
    let ck_fields: Vec<[Vec<u8>; 2]> =
        clicks.iter().filter_map(|l| csv::split::<4>(l)).map(|f| [f[0].to_vec(), f[1].to_vec()]).collect();

    // The user key: the most used user ID is a whale's first alias (people 0..5, alias 0).
    let mut counts: HashMap<u64, u32> = HashMap::new();
    for f in &ev_fields {
        if let Some(y) = decode(&f[1], b'u') {
            *counts.entry(y).or_default() += 1;
        }
    }
    let (&top, _) = counts.iter().max_by_key(|e| *e.1)?;
    let people = est_events / 10 + 1000;
    let users: Vec<u64> = counts.keys().copied().collect();
    let user = (0..5u64).map(|p| key_for(top, p * 512)).find(|&k| {
        users.iter().filter(|&&y| unscramble(y, k) >> 9 < people).count() * 100 >= users.len() * 99
    })?;

    // Event and click keys: rows of whales (people 0..5) have numbers below the whales' share.
    let whale = |s: &[u8]| decode(s, b'u').map(|y| unscramble(y, user) >> 9 < 5).unwrap_or(false);
    let ev_ys: Vec<u64> = ev_fields.iter().filter(|f| whale(&f[1])).filter_map(|f| decode(&f[0], b'e')).take(64).collect();
    let ck_ys: Vec<u64> = ck_fields.iter().filter(|f| whale(&f[1])).filter_map(|f| decode(&f[0], b'k')).take(64).collect();
    let event = search_key(&ev_ys, est_events / 8 + 1000, threads)?;
    let click = search_key(&ck_ys, est_clicks / 8 + 1000, threads)?;
    Some(Keys { user, event, click })
}

// ---- People ---------------------------------------------------------------------------------------

const OUTER: u8 = 1;
const BIG: u8 = 2;
const EXTRA: u8 = 4;
/// Every alias its own parent, four bits each.
const IDENTITY: u64 = 0xFEDC_BA98_7654_3210;

#[inline(always)]
fn nib_find(parents: u64, mut a: u32) -> u32 {
    loop {
        let p = (parents >> (4 * a)) as u32 & 15;
        if p == a {
            return a;
        }
        a = p;
    }
}

/// Everything known about people after the links.
struct People {
    flags: Vec<u8>,
    parents: Vec<u64>,
    /// The aliases (below 16) in the group of alias 0, for people whose groups are all their own.
    fast: Vec<u16>,
    /// General union-find: ID -> name of its group, for IDs on outer links and every link of
    /// OUTER or BIG people.
    general: HashMap<Id, Id>,
    user_key: u64,
}

impl People {
    #[inline(always)]
    fn is_fast(&self, user: u32) -> bool {
        let (p, a) = ((user >> 9) as usize, user & 511);
        a < 16 && self.fast[p] >> a & 1 != 0
    }

    /// The name of the fast group of person `p`: the byte-wise smallest of its IDs.
    fn fast_name(&self, p: u32) -> Id {
        let mut mask = self.fast[p as usize];
        let mut best: Option<([u8; 32], usize)> = None;
        while mask != 0 {
            let a = mask.trailing_zeros();
            mask &= mask - 1;
            let mut buf = [0u8; 32];
            let n = write_base36(&mut buf, b'u', scramble((p as u64) << 9 | a as u64, self.user_key));
            if best.as_ref().is_none_or(|(b, m)| buf[..n] < b[..*m]) {
                best = Some((buf, n));
            }
        }
        let (buf, n) = best.expect("a fast person has alias 0");
        unsafe { key::id(&buf[..n]) }
    }

    /// The name of the group of a user that is not fast: `user` is a dense number, or `SLOW`
    /// with its ID in `user_id`.
    fn slow_name(&self, user: u32, user_id: Id) -> Id {
        if user == SLOW {
            return self.general.get(&user_id).copied().unwrap_or(user_id);
        }
        let (p, a) = (user >> 9, user & 511);
        let flags = self.flags[p as usize];
        if flags & (OUTER | BIG) != 0 || a >= 16 {
            let id = id_of(b'u', user as u64, self.user_key);
            return self.general.get(&id).copied().unwrap_or(id);
        }
        let parents = self.parents[p as usize];
        let root = nib_find(parents, a);
        let mut best: Option<([u8; 32], usize)> = None;
        for b in 0..16 {
            if nib_find(parents, b) == root {
                let mut buf = [0u8; 32];
                let n = write_base36(&mut buf, b'u', scramble((p as u64) << 9 | b as u64, self.user_key));
                if best.as_ref().is_none_or(|(x, m)| buf[..n] < x[..*m]) {
                    best = Some((buf, n));
                }
            }
        }
        let (buf, n) = best.unwrap();
        unsafe { key::id(&buf[..n]) }
    }
}

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> SendPtr<T> {
    #[inline(always)]
    unsafe fn at(&self, i: usize) -> &mut T {
        unsafe { &mut *self.0.add(i) }
    }
}

fn people(parsed: &[Parsed], count: usize, user_key: u64, threads: usize) -> People {
    let mut flags = vec![0u8; count];
    for t in parsed {
        for &p in &t.outer_people {
            flags[p as usize] |= OUTER;
        }
    }
    let mut parents = vec![IDENTITY; count];
    let mut fast = vec![0u16; count];
    let (fp, pp, sp) = (SendPtr(flags.as_mut_ptr()), SendPtr(parents.as_mut_ptr()), SendPtr(fast.as_mut_ptr()));
    // Each worker owns a range of people. The links are first bucketed by range, per parse thread.
    let ranges = threads * 4;
    let range_of = |p: usize| (p * ranges / count).min(ranges - 1);
    let buckets: Vec<Vec<Vec<u64>>> = par::run(threads, parsed.len(), |_| Vec::new(), |done: &mut Vec<(usize, Vec<Vec<u64>>)>, t| {
        let mut b: Vec<Vec<u64>> = (0..ranges).map(|_| Vec::new()).collect();
        for &e in &parsed[t].inner_links {
            b[range_of((e >> 18) as usize)].push(e);
        }
        done.push((t, b));
    })
    .into_iter()
    .flatten()
    .fold((0..parsed.len()).map(|_| Vec::new()).collect::<Vec<_>>(), |mut acc, (t, b)| {
        acc[t] = b;
        acc
    });
    let general_links: Vec<Vec<(Id, Id)>> = par::run(threads, ranges, |_| Vec::new(), |general, r| {
        let (lo, hi) = ((r * count).div_ceil(ranges), ((r + 1) * count).div_ceil(ranges));
        let mine = || buckets.iter().flat_map(|b| b[r].iter().copied());
        for e in mine() {
            if (e >> 9) & 511 >= 16 || e & 511 >= 16 {
                unsafe { *fp.at((e >> 18) as usize) |= BIG };
            }
        }
        for e in mine() {
            let p = (e >> 18) as usize;
            let (a, b) = (((e >> 9) & 511) as u32, (e & 511) as u32);
            if unsafe { *fp.at(p) } & (OUTER | BIG) != 0 {
                let v = (p as u64) << 9;
                general.push((id_of(b'u', v | a as u64, user_key), id_of(b'u', v | b as u64, user_key)));
                continue;
            }
            let par = unsafe { pp.at(p) };
            let (ra, rb) = (nib_find(*par, a), nib_find(*par, b));
            if ra != rb {
                let (lo_r, hi_r) = (ra.min(rb), ra.max(rb));
                *par = *par & !(15u64 << (4 * hi_r)) | (lo_r as u64) << (4 * hi_r);
            }
        }
        for p in lo..hi {
            if unsafe { *fp.at(p) } & (OUTER | BIG) != 0 {
                continue;
            }
            let par = unsafe { *pp.at(p) };
            if par == IDENTITY {
                unsafe { *sp.at(p) = 1 };
                continue;
            }
            let root = nib_find(par, 0);
            let mut mask = 0u16;
            for a in 0..16 {
                if nib_find(par, a) == root {
                    mask |= 1 << a;
                }
            }
            unsafe { *sp.at(p) = mask };
        }
    });

    // The general union-find, over IDs.
    let mut index: HashMap<Id, u32> = HashMap::new();
    let mut ids: Vec<Id> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let node = |id: Id, index: &mut HashMap<Id, u32>, ids: &mut Vec<Id>, parent: &mut Vec<u32>| -> u32 {
        *index.entry(id).or_insert_with(|| {
            ids.push(id);
            parent.push(ids.len() as u32 - 1);
            ids.len() as u32 - 1
        })
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up;
            x = up;
        }
        x
    }
    let edges = parsed.iter().flat_map(|t| t.outer_links.iter()).chain(general_links.iter().flatten());
    for &(a, b) in edges {
        let (x, y) = (node(a, &mut index, &mut ids, &mut parent), node(b, &mut index, &mut ids, &mut parent));
        let (rx, ry) = (find(&mut parent, x), find(&mut parent, y));
        if rx != ry {
            parent[rx.max(ry) as usize] = rx.min(ry);
        }
    }
    let mut names: Vec<Id> = ids.clone();
    for x in 0..ids.len() as u32 {
        let r = find(&mut parent, x) as usize;
        if key::cmp_bytes(ids[x as usize], names[r]).is_lt() {
            names[r] = ids[x as usize];
        }
    }
    let mut general = HashMap::with_capacity(ids.len());
    for x in 0..ids.len() as u32 {
        let r = find(&mut parent, x) as usize;
        general.insert(ids[x as usize], names[r]);
    }
    People { flags, parents, fast, general, user_key }
}

// ---- Sessions -----------------------------------------------------------------------------------

struct SessCx<'a> {
    base: i64,
    first_day: i64,
    days: usize,
    organic: usize,
    category_rank: &'a [u32],
}

/// Per-thread aggregation state, as in main.rs's sessions stage.
struct Sess {
    rows: output::UserRows,
    campaign_days: Vec<[u64; 3]>,
    assisted: Vec<u32>,
    in_window: Vec<u32>,
    slot: Vec<u32>,
    active: Vec<u32>,
    day_categories: Vec<(u32, u64)>,
    events: Vec<u64>,
    clicks: Vec<u128>,
}

impl Sess {
    fn new(campaigns: usize, days: usize) -> Self {
        Sess {
            rows: output::UserRows::default(),
            campaign_days: vec![[0; 3]; campaigns * days],
            assisted: vec![0; campaigns * days],
            in_window: vec![0; campaigns],
            slot: vec![0; campaigns],
            active: Vec::new(),
            day_categories: Vec::new(),
            events: Vec::new(),
            clicks: Vec::new(),
        }
    }

    /// One person: `events` in time order (`ts << 32 | category << 24 | price`), `clicks`
    /// ordered as main.rs orders them (time, then click_id descending).
    fn person(&mut self, cx: &SessCx, name: Id, events: &[u64], clicks: &[u128]) {
        let click_ts = |k: u128| (k >> 96) as u32;
        let mut row_day = i64::MIN;
        let (mut row_sessions, mut row_events, mut row_revenue) = (0i64, 0i64, 0i64);
        let rows = &mut self.rows;
        let mut flush = |day: i64, sessions: i64, events: i64, revenue: i64, categories: &mut Vec<(u32, u64)>| {
            if sessions == 0 {
                return;
            }
            let top = categories
                .iter()
                .max_by(|a, b| a.1.cmp(&b.1).then_with(|| cx.category_rank[b.0 as usize].cmp(&cx.category_rank[a.0 as usize])))
                .map_or(output::NO_CATEGORY, |&(c, _)| c);
            categories.clear();
            rows.user_id.push(name);
            rows.day.push(day as i32);
            rows.sessions.push(sessions);
            rows.events.push(events);
            rows.revenue_cents.push(revenue);
            rows.top_category.push(top);
        };
        let (mut lo, mut hi) = (0, 0);
        let mut i = 0;
        while i < events.len() {
            let start = (events[i] >> 32) as u32;
            let day = (cx.base + start as i64).div_euclid(DAY_MS);
            if day != row_day {
                flush(row_day, row_sessions, row_events, row_revenue, &mut self.day_categories);
                (row_day, row_sessions, row_events, row_revenue) = (day, 0, 0, 0);
            }
            let mut previous = start;
            let (mut count, mut revenue) = (0i64, 0u64);
            while i < events.len() {
                let ts = (events[i] >> 32) as u32;
                if ts - previous > GAP_MS {
                    break;
                }
                previous = ts;
                count += 1;
                let price = events[i] & 0xFF_FFFF;
                if price != 0 {
                    let category = (events[i] >> 24) as u32 & 31;
                    revenue += price;
                    match self.day_categories.iter_mut().find(|c| c.0 == category) {
                        Some(c) => c.1 += price,
                        None => self.day_categories.push((category, price)),
                    }
                }
                i += 1;
            }
            row_sessions += 1;
            row_events += count;
            row_revenue += revenue as i64;

            while hi < clicks.len() && click_ts(clicks[hi]) <= start {
                if hi == 0 || clicks[hi] >> 32 != clicks[hi - 1] >> 32 {
                    let c = clicks[hi] as u32 as usize;
                    if self.in_window[c] == 0 {
                        self.slot[c] = self.active.len() as u32;
                        self.active.push(c as u32);
                    }
                    self.in_window[c] += 1;
                }
                hi += 1;
            }
            while lo < hi && (start - click_ts(clicks[lo])) as i64 > WEEK_MS {
                if lo == 0 || clicks[lo] >> 32 != clicks[lo - 1] >> 32 {
                    let c = clicks[lo] as u32 as usize;
                    self.in_window[c] -= 1;
                    if self.in_window[c] == 0 {
                        let at = self.slot[c] as usize;
                        let moved = self.active.pop().unwrap();
                        if moved as usize != c {
                            self.active[at] = moved;
                            self.slot[moved as usize] = at as u32;
                        }
                    }
                }
                lo += 1;
            }
            let credited = if hi > lo { clicks[hi - 1] as u32 as usize } else { cx.organic };
            let day_index = (day - cx.first_day) as usize;
            let cell = &mut self.campaign_days[credited * cx.days + day_index];
            cell[0] += 1;
            cell[1] += (revenue != 0) as u64;
            cell[2] += revenue;
            for &c in &self.active {
                self.assisted[c as usize * cx.days + day_index] += 1;
            }
        }
        flush(row_day, row_sessions, row_events, row_revenue, &mut self.day_categories);
        for &c in &self.active {
            self.in_window[c as usize] = 0;
        }
        self.active.clear();
    }
}

/// Click keys in main.rs's order. Clicks at one millisecond are ordered by their IDs' bytes;
/// any others get their (unique) number in the ID's place, which only has to be distinct.
fn click_keys(clicks: &[Click], click_key: u64, extra: &[u128], out: &mut Vec<u128>) {
    out.clear();
    for c in clicks {
        out.push((c.ts as u128) << 96 | (!(c.number as u64) as u128) << 32 | c.campaign as u128);
    }
    out.extend_from_slice(extra);
    out.sort_unstable_by_key(|k| k >> 96);
    // Ties in time: put the real IDs in and order those runs exactly.
    let mut i = 0;
    while i < out.len() {
        let mut j = i + 1;
        while j < out.len() && out[j] >> 96 == out[i] >> 96 {
            j += 1;
        }
        if j - i > 1 {
            for k in &mut out[i..j] {
                let low = !((*k >> 32) as u64);
                // Numbered clicks carry their number; extras already carry `!id`.
                if low >> 32 == 0 && (*k >> 32) as u64 >> 32 == u32::MAX as u64 {
                    let id = id_of(b'k', low, click_key);
                    *k = (*k >> 96) << 96 | (!id as u128) << 32 | (*k as u32) as u128;
                }
            }
            let run = &mut out[i..j];
            if run.iter().any(|k| key::is_interned(!((*k >> 32) as u64))) {
                run.sort_unstable_by(|&a, &b| key::cmp_bytes(!((b >> 32) as u64), !((a >> 32) as u64)));
            } else {
                run.sort_unstable();
            }
        }
        i = j;
    }
}

// ---- The run --------------------------------------------------------------------------------------

/// Runs the numbered path. `Ok(true)`: the tables are written. `Ok(false)`: it gave up
/// before writing anything (the reason is logged), and the ordinary pipeline must run.
pub fn run(inp: &Inputs) -> Res<bool> {
    let started = std::time::Instant::now();
    let mut lap = std::time::Instant::now();
    let mut log = |stage: &str| {
        eprintln!("dense {stage:<10} {:>8.1} ms", lap.elapsed().as_secs_f64() * 1e3);
        lap = std::time::Instant::now();
    };
    let threads = inp.threads;
    let base = inp.window_start - WEEK_MS;
    if inp.window_end - base >= 1 << 31 {
        eprintln!("dense: window too long");
        return Ok(false);
    }
    if inp.category_names.len() > 32 || inp.campaign_names.len() > 1 << 16 {
        eprintln!("dense: too many categories or campaigns");
        return Ok(false);
    }

    // ---- Files and keys
    let mut files: Vec<(PathBuf, Kind, u64)> = Vec::new();
    for (kind, folder) in [(Kind::Events, "events"), (Kind::Clicks, "clicks"), (Kind::Links, "links")] {
        for path in crate::csv_files(&inp.data_dir.join(folder))? {
            let len = std::fs::metadata(&path)?.len();
            files.push((path, kind, len));
        }
    }
    let bytes = |k: Kind| files.iter().filter(|f| f.1 == k).map(|f| f.2).sum::<u64>();
    // Lines are at least ~45 (events) and ~35 (clicks) bytes; the caps are generous, and
    // a number at or above a cap only means that row takes the general path.
    let est_events = bytes(Kind::Events) / 60;
    let est_clicks = bytes(Kind::Clicks) / 48;
    let Some(keys) = find_keys(&files, est_events, est_clicks, threads) else {
        eprintln!("dense: no keys");
        return Ok(false);
    };
    let ecap = ((bytes(Kind::Events) / 45) as u64).next_multiple_of(SLOTS as u64).max(SLOTS as u64);
    let ccap = ((bytes(Kind::Clicks) / 35) as u64).next_multiple_of(SLOTS as u64).max(SLOTS as u64);
    let (eparts, cparts) = ((ecap >> SLOT_BITS) as usize, (ccap >> SLOT_BITS) as usize);
    log("keys");

    let mut by_number = vec![NO_PRODUCT; 0];
    let mut others = StrMap::default();
    for &(product, category) in inp.products {
        match product_number(product) {
            Some(n) => {
                if by_number.len() <= n {
                    by_number.resize(n + 1, NO_PRODUCT);
                }
                if by_number[n] == NO_PRODUCT {
                    by_number[n] = category as u8;
                }
            }
            None => {
                others.get_or_insert(product, category);
            }
        }
    }
    let cx = Cx {
        keys,
        window_start: inp.window_start,
        window_end: inp.window_end,
        base,
        ing_base: inp.window_start / 1000 - 86_400,
        ecap,
        ccap,
        products: Products { by_number, others, unknown: inp.unknown_category },
        campaigns: inp.campaigns,
        dx: std::env::var("SS_DX").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
    };

    // ---- Parse
    const CHUNK: u64 = 512 << 10;
    let opened: Vec<(File, u64, Kind)> = files.iter().map(|(p, k, l)| Ok((File::open(p)?, *l, *k))).collect::<Res<_>>()?;
    let maps: Vec<Option<&'static [u8]>> = opened
        .iter()
        .map(|(f, len, _)| {
            if *len == 0 {
                return None;
            }
            use std::os::fd::AsRawFd;
            let p = unsafe { libc::mmap(std::ptr::null_mut(), *len as usize, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) };
            (p != libc::MAP_FAILED).then(|| unsafe { std::slice::from_raw_parts(p as *const u8, *len as usize) })
        })
        .collect();
    let mut chunks: Vec<(usize, u64, u64)> = Vec::new();
    for (i, (_, len, _)) in opened.iter().enumerate() {
        let mut start = 0;
        while start < *len {
            let end = (start + CHUNK).min(*len);
            chunks.push((i, start, end));
            start = end;
        }
    }
    let parsed: Vec<Parsed> = par::run(
        threads,
        chunks.len(),
        |_| Parsed {
            events: Some(Scatter::new(eparts)),
            clicks: Some(Scatter::new(cparts)),
            odd_events: Vec::new(),
            slow_event_users: Vec::new(),
            odd_clicks: Vec::new(),
            slow_click_users: Vec::new(),
            inner_links: Vec::new(),
            outer_links: Vec::new(),
            outer_people: Vec::new(),
            max_person: 0,
            dates: ts::Dates::new(),
            nl: Vec::new(),
            buf: Vec::new(),
            ebatch: Batch::default(),
            cbatch: Batch::default(),
        },
        |out, item| {
            let (file, start, end) = chunks[item];
            let (_, flen, kind) = opened[file];
            let mut buf = std::mem::take(&mut out.buf);
            let mapped = maps[file].and_then(|m| {
                let from = start.saturating_sub(1) as usize;
                let mut stop = end as usize;
                if stop > 0 && m[stop - 1] != b'\n' {
                    stop = match memchr::memchr(b'\n', &m[stop..]) {
                        Some(i) => stop + i + 1,
                        None => m.len(),
                    };
                }
                (stop + io::SLACK <= m.len()).then(|| (&m[from..stop + io::SLACK], stop - from, (start as usize) - from))
            });
            let (data, len, first): (&[u8], usize, usize) = match mapped {
                Some(x) => x,
                None => {
                    let (len, first) = io::load_chunk(&opened[file].0, flen, start, end, &mut buf).expect("read failed");
                    (&buf[..], len, first)
                }
            };
            let last = first + (end - start) as usize;
            let mut nl = std::mem::take(&mut out.nl);
            let p = data.as_ptr();
            match kind {
                Kind::Events => for_each_line(data, len, first, last, &mut nl, |s, n| unsafe { event_line(out, &cx, p.add(s), n) }),
                Kind::Clicks => for_each_line(data, len, first, last, &mut nl, |s, n| unsafe { click_line(out, &cx, p.add(s), n) }),
                Kind::Links => for_each_line(data, len, first, last, &mut nl, |s, n| unsafe { link_line(out, &cx, p.add(s), n) }),
            }
            // The batches point into this chunk's data: finish them before it goes.
            if out.ebatch.n > 0 {
                unsafe { event_batch(out, &cx) };
            }
            if out.cbatch.n > 0 {
                unsafe { click_batch(out, &cx) };
            }
            out.nl = nl;
            out.buf = buf;
        },
    );
    if let Some(why) = given_up() {
        eprintln!("dense: {why}");
        return Ok(false);
    }
    let person_count = parsed.iter().map(|t| t.max_person).max().unwrap_or(0) as usize + 1;
    log("parse");

    // ---- People
    let people = people(&parsed, person_count, cx.keys.user, threads);
    log("people");

    let mut parsed = parsed;
    let event_parts: Vec<Scattered<EvRec>> = parsed.iter_mut().map(|t| t.events.take().unwrap().finish()).collect();
    let click_parts: Vec<Scattered<CkRec>> = parsed.iter_mut().map(|t| t.clicks.take().unwrap().finish()).collect();

    // Odd clicks: dedup by ID; a valid one with a fast user joins its person as an extra.
    let mut flags_extra: Vec<u32> = Vec::new();
    let mut extra_clicks: HashMap<u32, Vec<u128>> = HashMap::new();
    let mut slow_clicks: Vec<(Id, u128)> = Vec::new();
    {
        let mut seen: HashMap<Id, ()> = HashMap::new();
        for c in parsed.iter().flat_map(|t| t.odd_clicks.iter()) {
            if seen.insert(c.id, ()).is_some() {
                continue;
            }
            let k = (c.ts as u128) << 96 | (!c.id as u128) << 32 | c.campaign as u128;
            if c.user != SLOW && people.is_fast(c.user) {
                extra_clicks.entry(c.user >> 9).or_default().push(k);
                flags_extra.push(c.user >> 9);
            } else {
                slow_clicks.push((people.slow_name(c.user, c.user_id), k));
            }
        }
    }
    let slow_click_users: HashMap<u32, Id> = parsed.iter().flat_map(|t| t.slow_click_users.iter().copied()).collect();

    // ---- Clicks: dedup by number, then by person in number order.
    struct ClickPart {
        clicks: Vec<Click>,
        slow: Vec<(Id, u128)>,
    }
    let click_out: Vec<Vec<(usize, ClickPart)>> = par::run(threads, cparts, |_| (Vec::new(), vec![EMPTY; 2 * SLOTS]), |(done, slots), part| {
        let mut out = ClickPart { clicks: Vec::new(), slow: Vec::new() };
        let mut any = false;
        for t in &click_parts {
            for slice in t.slices(part) {
                for r in slice {
                    let s = (r.a >> 48) as usize;
                    slots[2 * s] = r.a;
                    slots[2 * s + 1] = r.b;
                    any = true;
                }
            }
        }
        if any {
            let mut last_person = 0u32;
            for s in 0..SLOTS {
                let a = slots[2 * s];
                if a == EMPTY {
                    continue;
                }
                slots[2 * s] = EMPTY;
                let user = slots[2 * s + 1] as u32;
                let number = ((part as u64) << SLOT_BITS | s as u64) as u32;
                let (ts, campaign) = (a as u32, (a >> 32) as u32 & 0xFFFF);
                if user != SLOW && people.is_fast(user) {
                    let person = user >> 9;
                    if person < last_person {
                        give_up("clicks out of person order");
                    }
                    last_person = person;
                    out.clicks.push(Click { person, ts, number, campaign });
                } else {
                    let user_id = if user == SLOW { slow_click_users[&number] } else { 0 };
                    let id = id_of(b'k', number as u64, cx.keys.click);
                    out.slow.push((people.slow_name(user, user_id), (ts as u128) << 96 | (!id as u128) << 32 | campaign as u128));
                }
            }
        }
        done.push((part, out));
    })
    .into_iter()
    .map(|(done, _)| done)
    .collect();
    let mut click_parts_out: Vec<Option<ClickPart>> = (0..cparts).map(|_| None).collect();
    for (part, out) in click_out.into_iter().flatten() {
        click_parts_out[part] = Some(out);
    }
    let parts: Vec<ClickPart> = click_parts_out.into_iter().flatten().filter(|c| !c.clicks.is_empty() || !c.slow.is_empty()).collect();
    // Laid out end to end in number order (so by person), in parallel; each person's run is
    // `click_start[p]..click_end[p]`, written by the partitions where it starts and ends.
    let mut offsets = Vec::with_capacity(parts.len() + 1);
    offsets.push(0usize);
    let (mut prev_last, mut next_first) = (vec![u32::MAX; parts.len()], vec![u32::MAX; parts.len()]);
    let mut last_seen: Option<u32> = None;
    for (i, part) in parts.iter().enumerate() {
        offsets.push(offsets[i] + part.clicks.len());
        if let (Some(first), Some(last)) = (part.clicks.first(), part.clicks.last()) {
            if last_seen.is_some_and(|l| first.person < l) {
                give_up("clicks out of person order");
            }
            prev_last[i] = last_seen.unwrap_or(u32::MAX);
            last_seen = Some(last.person);
        } else {
            prev_last[i] = last_seen.unwrap_or(u32::MAX);
        }
    }
    let mut first_seen: Option<u32> = None;
    for (i, part) in parts.iter().enumerate().rev() {
        next_first[i] = first_seen.unwrap_or(u32::MAX);
        if let Some(first) = part.clicks.first() {
            first_seen = Some(first.person);
        }
    }
    let total = offsets[parts.len()];
    let mut all_clicks: Vec<Click> = Vec::with_capacity(total);
    let mut click_start = vec![0u32; person_count];
    let mut click_end = vec![0u32; person_count];
    {
        let (ap, sp, ep) = (SendPtr(all_clicks.as_mut_ptr()), SendPtr(click_start.as_mut_ptr()), SendPtr(click_end.as_mut_ptr()));
        par::run(threads, parts.len(), |_| (), |_, i| {
            let clicks = &parts[i].clicks;
            let off = offsets[i];
            for (j, c) in clicks.iter().enumerate() {
                unsafe { *ap.at(off + j) = *c };
                let before = if j == 0 { prev_last[i] } else { clicks[j - 1].person };
                let after = if j + 1 == clicks.len() { next_first[i] } else { clicks[j + 1].person };
                if before != c.person {
                    unsafe { *sp.at(c.person as usize) = (off + j) as u32 };
                }
                if after != c.person {
                    unsafe { *ep.at(c.person as usize) = (off + j + 1) as u32 };
                }
            }
        });
        unsafe { all_clicks.set_len(total) };
    }
    let mut parts = parts;
    for part in &mut parts {
        slow_clicks.append(&mut part.slow);
    }
    drop(parts);
    if let Some(why) = given_up() {
        eprintln!("dense: {why}");
        return Ok(false);
    }
    log("clicks");

    // Odd events: dedup by ID (smallest ingest), then as odd clicks.
    let mut extra_events: HashMap<u32, Vec<u64>> = HashMap::new();
    let mut slow_events: Vec<(Id, u64)> = Vec::new();
    {
        let mut kept: HashMap<Id, OddEvent> = HashMap::new();
        for e in parsed.iter().flat_map(|t| t.odd_events.iter()) {
            match kept.get_mut(&e.id) {
                Some(k) if e.ingest < k.ingest => *k = *e,
                Some(_) => {}
                None => {
                    kept.insert(e.id, *e);
                }
            }
        }
        for e in kept.values().filter(|e| e.valid) {
            if e.user != SLOW && people.is_fast(e.user) {
                extra_events.entry(e.user >> 9).or_default().push(e.ev);
                flags_extra.push(e.user >> 9);
            } else {
                slow_events.push((people.slow_name(e.user, e.user_id), e.ev));
            }
        }
    }
    let mut people = people;
    for p in flags_extra {
        people.flags[p as usize] |= EXTRA;
    }
    let people = people;
    let slow_event_users: HashMap<u64, Id> = parsed.iter().flat_map(|t| t.slow_event_users.iter().copied()).collect();

    // ---- Events
    let first_day = inp.window_start.div_euclid(DAY_MS);
    let days = ((inp.window_end - 1).div_euclid(DAY_MS) - first_day + 1).max(1) as usize;
    let campaign_count = inp.campaign_names.len();
    let mut category_rank = vec![0u32; inp.category_names.len()];
    let mut by_name: Vec<usize> = (0..inp.category_names.len()).collect();
    by_name.sort_by_key(|&c| inp.category_names[c]);
    for (rank, &c) in by_name.iter().enumerate() {
        category_rank[c] = rank as u32;
    }
    let scx = SessCx { base, first_day, days, organic: inp.organic, category_rank: &category_rank };

    // People with odd-ID events: each must be finished once, with or without numbered events.
    let extra_done: HashMap<u32, std::sync::atomic::AtomicBool> = extra_events.keys().map(|&p| (p, std::sync::atomic::AtomicBool::new(false))).collect();
    let finish_person = |s: &mut Sess, p: u32, events: &mut Vec<u64>, sorted: bool| {
        let mut sorted = sorted;
        if people.flags[p as usize] & EXTRA != 0 {
            if let Some(extra) = extra_events.get(&p) {
                extra_done[&p].store(true, Ordering::Relaxed);
                events.extend_from_slice(extra);
                sorted = false;
            }
        }
        if !sorted {
            events.sort_unstable();
        }
        let range = click_start[p as usize] as usize..click_end[p as usize] as usize;
        let extra: &[u128] = if people.flags[p as usize] & EXTRA != 0 { extra_clicks.get(&p).map_or(&[], |v| v.as_slice()) } else { &[] };
        let mut keys = std::mem::take(&mut s.clicks);
        click_keys(&all_clicks[range], cx.keys.click, extra, &mut keys);
        s.person(&scx, people.fast_name(p), events, &keys);
        s.clicks = keys;
    };

    /// What a partition leaves for the stitch: its first and last person's events.
    #[derive(Default)]
    struct Edge {
        first: Option<(u32, Vec<u64>, bool)>,
        last: Option<(u32, Vec<u64>, bool)>,
    }
    struct EvState {
        sess: Sess,
        slots: Vec<u64>,
        edges: Vec<(usize, Edge)>,
        slow: Vec<(Id, u64)>,
        cur: Vec<u64>,
    }
    let states: Vec<EvState> = par::run(
        threads,
        eparts,
        |_| EvState { sess: Sess::new(campaign_count, days), slots: vec![EMPTY; 2 * SLOTS], edges: Vec::new(), slow: Vec::new(), cur: Vec::new() },
        |st, part| {
            let slots = &mut st.slots;
            let mut any = false;
            for t in &event_parts {
                for slice in t.slices(part) {
                    for r in slice {
                        let s = ((r.a >> 29) & SLOT_MASK) as usize;
                        let old = slots[2 * s];
                        if old == EMPTY || r.a >> 43 < old >> 43 {
                            slots[2 * s] = r.a;
                            slots[2 * s + 1] = r.b;
                        }
                        any = true;
                    }
                }
            }
            if !any {
                return;
            }
            let mut edge = Edge::default();
            let mut cur_p = u32::MAX;
            let mut sorted = true;
            let mut cur = std::mem::take(&mut st.cur);
            cur.clear();
            let flush = |st_sess: &mut Sess, edge: &mut Edge, p: u32, cur: &mut Vec<u64>, sorted: bool, last: bool| {
                if edge.first.is_none() {
                    edge.first = Some((p, std::mem::take(cur), sorted));
                } else if last {
                    edge.last = Some((p, std::mem::take(cur), sorted));
                } else {
                    finish_person(st_sess, p, cur, sorted);
                    cur.clear();
                }
            };
            for s in 0..SLOTS {
                let a = slots[2 * s];
                if a == EMPTY {
                    continue;
                }
                let b = slots[2 * s + 1];
                slots[2 * s] = EMPTY;
                if b >> 63 == 0 {
                    continue;
                }
                let user = b as u32;
                let ev = (b >> 32 & 0x7FFF_FFFF) << 32 | (a & 0x1FFF_FFFF);
                if user == SLOW || !people.is_fast(user) {
                    let user_id = if user == SLOW {
                        let n = (part as u64) << SLOT_BITS | s as u64;
                        slow_event_users[&(n << ING_BITS | a >> 43)]
                    } else {
                        0
                    };
                    st.slow.push((people.slow_name(user, user_id), ev));
                    continue;
                }
                let p = user >> 9;
                if p != cur_p {
                    if cur_p != u32::MAX {
                        if p < cur_p {
                            give_up("events out of person order");
                        }
                        flush(&mut st.sess, &mut edge, cur_p, &mut cur, sorted, false);
                    }
                    cur_p = p;
                    sorted = true;
                } else if let Some(&prev) = cur.last() {
                    sorted &= prev >> 32 <= ev >> 32;
                }
                cur.push(ev);
            }
            if cur_p != u32::MAX {
                flush(&mut st.sess, &mut edge, cur_p, &mut cur, sorted, true);
            }
            st.cur = cur;
            st.edges.push((part, edge));
        },
    );
    if let Some(why) = given_up() {
        eprintln!("dense: {why}");
        return Ok(false);
    }
    log("events");

    // ---- Stitch the people at partition edges, and the slow path
    let mut edges: Vec<Option<Edge>> = (0..eparts).map(|_| None).collect();
    let mut sessions: Vec<Sess> = Vec::new();
    let mut slow_events = slow_events;
    for st in states {
        for (part, e) in st.edges {
            edges[part] = Some(e);
        }
        slow_events.extend(st.slow);
        sessions.push(st.sess);
    }
    let mut stitched: Vec<(u32, Vec<u64>, bool)> = Vec::new();
    let mut last_person: Option<u32> = None;
    for e in edges.into_iter().flatten() {
        for seg in [e.first, e.last].into_iter().flatten() {
            let (p, events, sorted) = seg;
            match stitched.last_mut() {
                Some(top) if top.0 == p => {
                    if let (Some(&a), Some(&b)) = (top.1.last(), events.first()) {
                        top.2 &= a >> 32 <= b >> 32;
                    }
                    top.2 &= sorted;
                    top.1.extend_from_slice(&events);
                }
                _ => {
                    if last_person.is_some_and(|l| p < l) {
                        give_up("events out of person order at an edge");
                    }
                    stitched.push((p, events, sorted));
                }
            }
            last_person = Some(p);
        }
    }
    if let Some(why) = given_up() {
        eprintln!("dense: {why}");
        return Ok(false);
    }
    for (&p, done) in &extra_done {
        if !done.load(Ordering::Relaxed) && !stitched.iter().any(|s| s.0 == p) {
            stitched.push((p, Vec::new(), true));
        }
    }
    let stitched = Mutex::new(stitched.into_iter().map(Some).collect::<Vec<_>>());
    let count = stitched.lock().unwrap().len();
    // Slow groups, by name.
    let mut groups: HashMap<Id, (Vec<u64>, Vec<u128>)> = HashMap::new();
    for (name, ev) in slow_events {
        groups.entry(name).or_default().0.push(ev);
    }
    for (name, k) in slow_clicks {
        if let Some(g) = groups.get_mut(&name) {
            g.1.push(k);
        }
    }
    let groups: Vec<(Id, (Vec<u64>, Vec<u128>))> = groups.into_iter().collect();
    let groups = Mutex::new(groups.into_iter().map(Some).collect::<Vec<_>>());
    let gcount = groups.lock().unwrap().len();
    let more = par::run(threads, count + gcount, |_| Sess::new(campaign_count, days), |s, i| {
        if i < count {
            let (p, mut events, sorted) = stitched.lock().unwrap()[i].take().unwrap();
            finish_person(s, p, &mut events, sorted);
        } else {
            let (name, (mut events, mut clicks)) = groups.lock().unwrap()[i - count].take().unwrap();
            events.sort_unstable();
            if clicks.iter().any(|k| key::is_interned(!((*k >> 32) as u64))) {
                clicks.sort_unstable_by(|&a, &b| ((a >> 96) as u32).cmp(&((b >> 96) as u32)).then_with(|| key::cmp_bytes(!((b >> 32) as u64), !((a >> 32) as u64))));
            } else {
                clicks.sort_unstable();
            }
            s.person(&scx, name, &events, &clicks);
        }
    });
    sessions.extend(more);
    log("stitch");

    // ---- Write
    write(inp, &sessions, days, first_day, campaign_count)?;
    log("write");
    eprintln!("dense total      {:>8.1} ms", started.elapsed().as_secs_f64() * 1e3);
    Ok(true)
}

fn write(inp: &Inputs, sessions: &[Sess], days: usize, first_day: i64, campaign_count: usize) -> Res<()> {
    let user_dir = inp.out_dir.join("user_daily");
    let campaign_dir = inp.out_dir.join("campaign_daily");
    for folder in [&user_dir, &campaign_dir] {
        std::fs::create_dir_all(folder)?;
        for entry in std::fs::read_dir(folder)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                std::fs::remove_file(path)?;
            }
        }
    }
    let mut campaign_rows = output::CampaignRows::default();
    for campaign in 0..campaign_count {
        for day in 0..days {
            let mut total = [0u64; 4];
            for s in sessions {
                let cell = s.campaign_days[campaign * days + day];
                for k in 0..3 {
                    total[k] += cell[k];
                }
                total[3] += s.assisted[campaign * days + day] as u64;
            }
            if total[0] == 0 && total[3] == 0 {
                continue;
            }
            campaign_rows.campaign.push(campaign as u32);
            campaign_rows.day.push((first_day + day as i64) as i32);
            campaign_rows.sessions.push(total[0] as i64);
            campaign_rows.purchasing_sessions.push(total[1] as i64);
            campaign_rows.revenue_cents.push(total[2] as i64);
            campaign_rows.assisted_sessions.push(total[3] as i64);
        }
    }
    let written = AtomicU32::new(0);
    let results = par::run(
        inp.threads,
        sessions.len() + 1,
        |_| Vec::<Res<()>>::new(),
        |results, i| {
            if i == sessions.len() {
                results.push(output::write_campaign_daily(&campaign_dir.join("part-000.parquet"), &campaign_rows, inp.campaign_names, inp.channels));
            } else if !sessions[i].rows.user_id.is_empty() || i == 0 {
                written.fetch_add(1, Ordering::Relaxed);
                results.push(output::write_user_daily(&user_dir.join(format!("part-{i:03}.parquet")), &sessions[i].rows, inp.category_names));
            }
        },
    );
    for r in results.into_iter().flatten() {
        r?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_round_trips() {
        for &k in &[0u64, 1, 0xDEAD_BEEF_1234, u64::MAX] {
            for v in [0u64, 1, 2, 511, 512, 1 << 20, (1 << 40) - 1, 123_456_789] {
                let y = scramble(v, k);
                assert_eq!(unscramble(y, k), v);
                assert_eq!(key_for(y, v) & M40, k & M40);
            }
        }
    }

    #[test]
    fn base36_is_exact() {
        let check = |s: &[u8]| {
            let mut buf = [b'x'; 32];
            buf[..s.len()].copy_from_slice(s);
            unsafe { base36(&buf[..s.len()], b'e') }
        };
        for v in [0u64, 1, 35, 36, 1295, 1296, 999_999, (1 << 40) - 1, 36u64.pow(7), 36u64.pow(8) - 1] {
            let mut out = [0u8; 32];
            let n = write_base36(&mut out, b'e', v);
            let expect = if v <= M40 { Some(v) } else { None };
            assert_eq!(check(&out[..n]), expect, "{}", String::from_utf8_lossy(&out[..n]));
        }
        for bad in [&b"e"[..], b"E1", b"e01", b"eA", b"e1 ", b"e-1", b"e123456789", b"e\x80", b"u1", b"e1z_"] {
            assert_eq!(check(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert_eq!(check(b"e0"), Some(0));
        assert_eq!(check(b"ez"), Some(35));
        assert_eq!(check(b"e10"), Some(36));
    }

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut x = seed;
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }
    }

    #[test]
    fn cut_matches_split_random() {
        let mut next = rng(0x1234_5678_9ABC_DEF1);
        let alphabet = b"ab,,\"\"x";
        for round in 0..200_000 {
            let len = (next() % 140) as usize;
            let line: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let mut buf = line.clone();
            buf.resize(len + 256, if round % 2 == 0 { b'"' } else { b',' });
            let mut out7 = [(0u32, 0u32); 7];
            let ok = unsafe { cut::<7>(buf.as_ptr(), len, &mut out7) };
            let want = csv::split::<7>(&line);
            assert_eq!(ok, want.is_some(), "{}", String::from_utf8_lossy(&line));
            if let Some(w) = want {
                for (o, f) in out7.iter().zip(w) {
                    assert_eq!(&line[o.0 as usize..o.1 as usize], f, "{}", String::from_utf8_lossy(&line));
                }
            }
            let mut out2 = [(0u32, 0u32); 2];
            let ok = unsafe { cut::<2>(buf.as_ptr(), len, &mut out2) };
            assert_eq!(ok, csv::split::<2>(&line).is_some(), "{}", String::from_utf8_lossy(&line));
        }
    }

    #[test]
    fn decode8_matches_scalar() {
        let mut next = rng(0xDEAD_BEEF_F00D_1234);
        let chars = b"0123456789abcdefghijklmnopqrstuvwxyzA_ \x80";
        for round in 0..100_000 {
            let mut words = [0u64; 8];
            let mut n = [0u64; 8];
            for lane in 0..8 {
                let k = (next() % 10) as usize;
                let mut b = [0u8; 8];
                for (i, x) in b.iter_mut().enumerate() {
                    *x = if i < k.min(8) {
                        if next() % 8 == 0 { chars[(next() % chars.len() as u64) as usize] } else { chars[(next() % 36) as usize] }
                    } else {
                        (next() & 0xFF) as u8
                    };
                }
                words[lane] = u64::from_le_bytes(b);
                n[lane] = k as u64;
            }
            let key = if round % 3 == 0 { 0 } else { next() };
            let (values, ok) = decode8(&words, &n, key);
            for lane in 0..8 {
                let want = base36_word(words[lane], n[lane] as usize).map(|y| unscramble(y, key));
                assert_eq!(ok >> lane & 1 != 0, want.is_some(), "lane {lane} n {} {:x}", n[lane], words[lane]);
                if let Some(v) = want {
                    assert_eq!(values[lane], v);
                }
            }
        }
    }

    #[test]
    fn cut_matches_split() {
        let lines: &[&[u8]] = &[
            b"a,b,c,d", b"\"a\",b,\"c\",d", b"a,,c,", b"\"\",b,c,d", b"a\"x,b,c,d", b"a,b,c", b"a,b,c,d,e", b"\"a,b,c,d",
            b"a,\"b\"x,c,d", b"\",b,c,d", b"a,b,c,\"\"", b"a,b,c,\"",
        ];
        for l in lines {
            let mut buf = l.to_vec();
            buf.resize(l.len() + 256, b'"');
            let mut out = [(0u32, 0u32); 4];
            let ok = unsafe { cut::<4>(buf.as_ptr(), l.len(), &mut out) };
            let want = csv::split::<4>(l);
            assert_eq!(ok, want.is_some(), "{}", String::from_utf8_lossy(l));
            if let Some(w) = want {
                for (o, f) in out.iter().zip(w) {
                    assert_eq!(&l[o.0 as usize..o.1 as usize], f);
                }
            }
        }
    }
}
