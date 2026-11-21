//! IDs as u64 keys. An ID of at most 9 bytes, each 0x01..=0x7f, packs 7 bits per
//! byte, first byte most significant, zero-padded: the key compares exactly like the
//! bytes. Anything else gets an interned key with the top bit set; those compare by
//! looking the bytes back up. The generator's IDs (`u` + <= 8 base-36 chars) all pack.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

pub const FALLBACK: u64 = 1 << 63;

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
pub fn pack(b: &[u8]) -> Option<u64> {
    if b.len() > 9 {
        return None;
    }
    let mut k: u64 = 0;
    for &c in b {
        if c == 0 || c >= 0x80 {
            return None;
        }
        k = (k << 7) | c as u64;
    }
    Some(k << (7 * (9 - b.len())))
}

/// `pack` for a field inside a buffer with at least 16 readable bytes from its start
/// (the scanner's read buffers are padded). Same result as `pack`, without the byte loop.
///
/// # Safety
/// `b.as_ptr()..b.as_ptr() + 16` must be readable.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
pub unsafe fn pack_padded(b: &[u8]) -> Option<u64> {
    let n = b.len();
    if n == 0 || n > 9 {
        return if n == 0 { Some(0) } else { None };
    }
    let p = b.as_ptr();
    let raw = unsafe { std::ptr::read_unaligned(p as *const u64) };
    let w = if n >= 8 { raw } else { raw & ((1u64 << (8 * n)) - 1) };
    let b8 = if n == 9 { unsafe { *p.add(8) } } else { 0 };
    // bytes 0..min(n,8) must be 0x01..=0x7f; byte 8 likewise when n == 9
    let live = if n >= 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
    const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    let zero = !(((w & LO7).wrapping_add(LO7)) | w | LO7) & 0x8080_8080_8080_8080;
    if (w & 0x8080_8080_8080_8080) != 0 || (zero & live) != 0 || (n == 9 && (b8 == 0 || b8 >= 0x80)) {
        return None;
    }
    let be = w.swap_bytes(); // byte 0 in the top 8 bits
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let hi56 = unsafe { std::arch::x86_64::_pext_u64(be, LO7) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let hi56 = {
        let mut k = 0u64;
        for i in (0..8).rev() {
            k = (k << 7) | ((be >> (8 * i)) & 0x7f);
        }
        k
    };
    Some((hi56 << 7) | b8 as u64)
}

pub fn unpack(k: u64, out: &mut Vec<u8>) {
    for i in 0..9 {
        let c = ((k >> (7 * (8 - i))) & 0x7f) as u8;
        if c == 0 {
            break;
        }
        out.push(c);
    }
}

#[derive(Default)]
pub struct Interner {
    map: Mutex<HashMap<Vec<u8>, u64>>,
    strs: RwLock<Vec<Vec<u8>>>,
}

impl Interner {
    #[inline]
    pub fn key(&self, b: &[u8]) -> u64 {
        match pack(b) {
            Some(k) => k,
            None => self.slow(b),
        }
    }

    /// `key` for a field in a padded buffer (see `pack_padded`).
    ///
    /// # Safety
    /// As `pack_padded`.
    #[inline(always)]
    pub unsafe fn key_padded(&self, b: &[u8]) -> u64 {
        match unsafe { pack_padded(b) } {
            Some(k) if !b.is_empty() => k,
            _ => self.key(b),
        }
    }

    #[cold]
    fn slow(&self, b: &[u8]) -> u64 {
        let mut map = self.map.lock().unwrap();
        if let Some(&k) = map.get(b) {
            return k;
        }
        let mut strs = self.strs.write().unwrap();
        let k = FALLBACK | strs.len() as u64;
        strs.push(b.to_vec());
        map.insert(b.to_vec(), k);
        k
    }

    pub fn bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & FALLBACK == 0 {
            unpack(k, out);
        } else {
            out.extend_from_slice(&self.strs.read().unwrap()[(k & !FALLBACK) as usize]);
        }
    }

    #[inline]
    pub fn cmp(&self, a: u64, b: u64) -> Ordering {
        if (a | b) & FALLBACK == 0 {
            return a.cmp(&b);
        }
        let (mut x, mut y) = (Vec::new(), Vec::new());
        self.bytes(a, &mut x);
        self.bytes(b, &mut y);
        x.cmp(&y)
    }
}

/// Exact lookup table for small clean files (products, campaigns): packed keys go
/// in one map, anything unpackable in another, so lookups never allocate.
pub struct Lookup<V: Copy> {
    packed: FxMap<u64, V>,
    other: FxMap<Vec<u8>, V>,
    /// When every key is `prefix` + a canonical decimal (like `p123`, `c_45`), an array
    /// indexed by the number. Exact: a query of any other shape can match no key.
    dense: Option<(Vec<u8>, Vec<Option<V>>)>,
    keys: Vec<Vec<u8>>,
}

fn canonical_number(rest: &[u8]) -> Option<usize> {
    if rest.is_empty() || rest.len() > 7 || (rest[0] == b'0' && rest.len() > 1) {
        return None;
    }
    let mut v = 0usize;
    for &c in rest {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as usize;
    }
    Some(v)
}

/// `canonical_number` for `rest` inside a padded buffer: one load, SWAR digit check
/// and conversion (up to 7 digits).
///
/// # Safety
/// 8 bytes from `rest.as_ptr()` must be readable.
#[inline(always)]
unsafe fn canonical_number_padded(rest: &[u8]) -> Option<usize> {
    let n = rest.len();
    if n == 0 || n > 7 {
        return None;
    }
    let raw = unsafe { std::ptr::read_unaligned(rest.as_ptr() as *const u64) };
    let x = (raw ^ 0x3030_3030_3030_3030) & ((1u64 << (8 * n)) - 1);
    // all n bytes are digits (bytes past n are 0 after the mask)
    if ((x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    if n > 1 && (x & 0xff) == 0 {
        return None; // leading zero
    }
    // right-align the digits (first digit at byte 8-n), then the classic 8-digit SWAR fold
    let mut v = x << (8 * (8 - n));
    v = (v.wrapping_mul(10) + (v >> 8)) & 0x00ff_00ff_00ff_00ff;
    v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_ffff_0000_ffff;
    v = (v.wrapping_mul(10000) + (v >> 32)) & 0x0000_0000_ffff_ffff;
    Some(v as usize)
}

impl<V: Copy> Lookup<V> {
    pub fn new() -> Self {
        Self { packed: FxMap::default(), other: FxMap::default(), dense: None, keys: Vec::new() }
    }

    /// Call after the last insert: switches to the dense array when the keys allow it.
    pub fn finish(&mut self) {
        let Some(first) = self.keys.first() else { return };
        let plen = first.iter().position(|c| c.is_ascii_digit()).unwrap_or(first.len());
        let prefix = first[..plen].to_vec();
        let mut nums = Vec::with_capacity(self.keys.len());
        for k in &self.keys {
            match k.strip_prefix(&prefix[..]).and_then(canonical_number) {
                Some(n) => nums.push(n),
                None => return,
            }
        }
        let mut table = vec![None; nums.iter().max().unwrap() + 1];
        for (k, n) in self.keys.iter().zip(nums) {
            table[n] = self.get(k);
        }
        self.dense = Some((prefix, table));
    }
    pub fn insert(&mut self, b: &[u8], v: V) {
        self.keys.push(b.to_vec());
        match pack(b) {
            Some(k) => self.packed.insert(k, v),
            None => self.other.insert(b.to_vec(), v),
        };
    }
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn get(&self, b: &[u8]) -> Option<V> {
        if let Some((prefix, table)) = &self.dense {
            return match b.strip_prefix(&prefix[..]).and_then(canonical_number) {
                Some(n) => table.get(n).copied().flatten(),
                None => None,
            };
        }
        match pack(b) {
            Some(k) => self.packed.get(&k).copied(),
            None => self.other.get(b).copied(),
        }
    }

    /// `get` for a field inside a padded buffer.
    ///
    /// # Safety
    /// 8 bytes past the end of `b` must be readable.
    #[inline(always)]
    pub unsafe fn get_padded(&self, b: &[u8]) -> Option<V> {
        if let Some((prefix, table)) = &self.dense
            && b.len() > prefix.len()
            && b.starts_with(prefix)
        {
            let rest = &b[prefix.len()..];
            return match unsafe { canonical_number_padded(rest) } {
                Some(n) => table.get(n).copied().flatten(),
                None => None,
            };
        }
        self.get(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order_matches_bytes() {
        let ids: &[&[u8]] = &[b"u", b"u1", b"u10", b"u2", b"U9", b" u1", b"u1 ", b"uzzzzzzzz", b"\tA"];
        for a in ids {
            for b in ids {
                assert_eq!(pack(a).unwrap().cmp(&pack(b).unwrap()), a.cmp(b), "{a:?} {b:?}");
            }
            let mut v = Vec::new();
            unpack(pack(a).unwrap(), &mut v);
            assert_eq!(&v[..], *a);
        }
        assert!(pack(b"u123456789").is_none());
    }

    #[test]
    fn padded_paths_match() {
        let mut r: u64 = 7;
        let alpha = b"0123456789abcdefuU \x00\xff\"p_c";
        for _ in 0..300_000 {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let n = (r >> 59) as usize % 12;
            let mut buf = vec![0u8; 32];
            for (i, b) in buf.iter_mut().enumerate() {
                r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                *b = if i < n || (r >> 60) & 1 == 1 { alpha[(r >> 33) as usize % alpha.len()] } else { b'z' };
            }
            let f = &buf[..n];
            if n > 0 {
                assert_eq!(unsafe { pack_padded(f) }, pack(f), "{f:?}");
            }
            assert_eq!(unsafe { canonical_number_padded(f) }, canonical_number(f), "{f:?}");
        }
        for v in [0usize, 1, 9, 10, 99, 100, 12345, 49999, 50000, 999_999, 9_999_999] {
            let mut buf = v.to_string().into_bytes();
            buf.extend_from_slice(&[b'9'; 16]);
            let n = v.to_string().len();
            assert_eq!(unsafe { canonical_number_padded(&buf[..n]) }, Some(v));
        }
    }

    #[test]
    fn dense_lookup_is_exact() {
        let mut l: Lookup<u32> = Lookup::new();
        for (i, k) in ["c_1", "c_2", "c_10", "c_0"].iter().enumerate() {
            l.insert(k.as_bytes(), i as u32);
        }
        l.finish();
        assert!(l.dense.is_some());
        for (q, want) in [("c_1", Some(0)), ("c_10", Some(2)), ("c_0", Some(3)), ("c_01", None), ("C_1", None),
                          ("c_3", None), ("c_99999999", None), (" c_1", None), ("c_1 ", None), ("c_", None), ("", None)] {
            assert_eq!(l.get(q.as_bytes()), want, "{q}");
        }
        let mut l: Lookup<u32> = Lookup::new();
        l.insert(b"p1", 1);
        l.insert(b"x7", 2);
        l.finish();
        assert!(l.dense.is_none());
        assert_eq!(l.get(b"x7"), Some(2));
    }
}

/// Fast non-cryptographic hasher (FxHash-style). Inputs are our own packed keys,
/// so HashDoS is not a concern.
#[derive(Default, Clone, Copy)]
pub struct Fx(u64);

impl std::hash::Hasher for Fx {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(w));
        }
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        self.0 ^= self.0 >> 29;
    }
    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
}

pub type FxBuild = std::hash::BuildHasherDefault<Fx>;
pub type FxMap<K, V> = HashMap<K, V, FxBuild>;
