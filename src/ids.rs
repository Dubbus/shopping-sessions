//! IDs as u64 keys. An ID of at most 9 bytes, each 0x01..=0x7f, packs 7 bits per
//! byte, first byte most significant, zero-padded: the key compares exactly like the
//! bytes. Anything else gets an interned key with the top bit set; those compare by
//! looking the bytes back up. The generator's IDs (`u` + <= 8 base-36 chars) all pack.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

pub const FALLBACK: u64 = 1 << 63;

#[inline]
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
    packed: HashMap<u64, V>,
    other: HashMap<Vec<u8>, V>,
}

impl<V: Copy> Lookup<V> {
    pub fn new() -> Self {
        Self { packed: HashMap::new(), other: HashMap::new() }
    }
    pub fn insert(&mut self, b: &[u8], v: V) {
        match pack(b) {
            Some(k) => self.packed.insert(k, v),
            None => self.other.insert(b.to_vec(), v),
        };
    }
    #[inline]
    pub fn get(&self, b: &[u8]) -> Option<V> {
        match pack(b) {
            Some(k) => self.packed.get(&k).copied(),
            None => self.other.get(b).copied(),
        }
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
}
