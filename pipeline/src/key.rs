//! IDs as 16-byte keys. An ID of up to 15 bytes is stored inline (its bytes,
//! zero-padded, then its length), so two keys are equal exactly when the IDs
//! are. Longer IDs are rare and take a slow path:
//!
//! * user and click IDs are interned once in a global table and the key holds
//!   the pointer and length (tag `0xFF`), so equal keys still mean equal IDs;
//! * event IDs, which are only ever compared for equality, hold 120 bits of a
//!   128-bit hash (tag `0xFE`).

use std::cmp::Ordering;
use std::sync::Mutex;

use hashbrown::HashTable;

const LONG: u64 = 0xFF;
const HASHED: u64 = 0xFE;

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
#[repr(C)]
pub struct Key {
    /// Bytes 0 to 7, little-endian.
    pub lo: u64,
    /// Bytes 8 to 14, then the tag: the length (1 to 15), `LONG` or `HASHED`.
    pub hi: u64,
}

static LONG_STRINGS: Mutex<Option<HashTable<&'static [u8]>>> = Mutex::new(None);

fn long_hash(s: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(s)
}

/// The one shared copy of a long string.
fn intern_long(s: &[u8]) -> &'static [u8] {
    let mut guard = LONG_STRINGS.lock().unwrap();
    let table = guard.get_or_insert_with(HashTable::new);
    let h = long_hash(s);
    if let Some(found) = table.find(h, |e| *e == s) {
        return found;
    }
    let copy: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
    table.insert_unique(h, copy, |e| long_hash(e));
    copy
}

impl Key {
    /// An ID of at most 15 bytes.
    ///
    /// # Safety
    /// 16 bytes must be readable from `s.as_ptr()` (the chunk buffers keep that much slack).
    #[inline]
    unsafe fn inline(s: &[u8]) -> Key {
        debug_assert!(s.len() <= 15);
        let raw = u128::from_le(unsafe { std::ptr::read_unaligned(s.as_ptr() as *const u128) });
        let v = (raw & ((1u128 << (8 * s.len())) - 1)) | (s.len() as u128) << 120;
        Key { lo: v as u64, hi: (v >> 64) as u64 }
    }

    /// A user or click ID: compared for equality and for byte order, and written out.
    ///
    /// # Safety
    /// As for [`Key::inline`].
    #[inline]
    pub unsafe fn text(s: &[u8]) -> Key {
        if s.len() <= 15 {
            unsafe { Key::inline(s) }
        } else {
            let shared = intern_long(s);
            Key { lo: shared.as_ptr() as u64, hi: shared.len() as u64 | LONG << 56 }
        }
    }

    /// An event ID: only ever compared for equality.
    ///
    /// # Safety
    /// As for [`Key::inline`].
    #[inline]
    pub unsafe fn identity(s: &[u8]) -> Key {
        if s.len() <= 15 {
            unsafe { Key::inline(s) }
        } else {
            let h = xxhash_rust::xxh3::xxh3_128(s);
            Key { lo: h as u64, hi: ((h >> 64) as u64 & 0x00FF_FFFF_FFFF_FFFF) | HASHED << 56 }
        }
    }

    /// Like [`Key::text`], for a slice with no slack after it.
    #[cfg(test)]
    pub fn text_copy(s: &[u8]) -> Key {
        let mut padded = [0u8; 32];
        if s.len() <= 15 {
            padded[..s.len()].copy_from_slice(s);
            unsafe { Key::inline(&padded[..s.len()]) }
        } else {
            let shared = intern_long(s);
            Key { lo: shared.as_ptr() as u64, hi: shared.len() as u64 | LONG << 56 }
        }
    }

    #[inline]
    fn tag(&self) -> u64 {
        self.hi >> 56
    }

    /// The ID's bytes. Not meaningful for a hashed event ID.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        let tag = self.tag();
        if tag <= 15 {
            // SAFETY: the struct is 16 plain bytes and the first `tag` of them are the ID.
            unsafe { std::slice::from_raw_parts(self as *const Key as *const u8, tag as usize) }
        } else if tag == LONG {
            // SAFETY: made by `intern_long`, which leaks its copies.
            unsafe { std::slice::from_raw_parts(self.lo as *const u8, (self.hi & 0x00FF_FFFF_FFFF_FFFF) as usize) }
        } else {
            &[]
        }
    }

    /// Byte-wise order of the two IDs.
    #[inline]
    pub fn cmp_bytes(&self, other: &Key) -> Ordering {
        if self.tag() <= 15 && other.tag() <= 15 {
            // Big-endian views compare like the bytes; the length, last, settles a prefix.
            (self.lo.swap_bytes(), self.hi.swap_bytes()).cmp(&(other.lo.swap_bytes(), other.hi.swap_bytes()))
        } else {
            self.bytes().cmp(other.bytes())
        }
    }

    /// A well-mixed 64-bit hash. Partitions take its top bits; tables use [`remix`] of it.
    #[inline]
    pub fn hash(&self) -> u64 {
        let m = (self.lo ^ 0x9E37_79B9_7F4A_7C15) as u128 * (self.hi ^ 0xD1B5_4A32_D192_ED03) as u128;
        (m as u64) ^ (m >> 64) as u64
    }
}

/// A second hash from the first, independent of the bits a partition was chosen by.
#[inline]
pub fn remix(h: u64) -> u64 {
    (h ^ (h >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93)
}

#[inline]
pub fn partition(h: u64, bits: u32) -> usize {
    if bits == 0 { 0 } else { (h >> (64 - bits)) as usize }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &[u8]) -> Key {
        Key::text_copy(s)
    }

    #[test]
    fn round_trips() {
        for s in [&b"u"[..], b"u1", b"u7ztjlcfs", b"exactly15bytes!", b"sixteen bytes!!!", &[b'x'; 300][..], b"a\0", b"a\0\0"] {
            assert_eq!(k(s).bytes(), s);
            assert_eq!(k(s), k(s));
        }
        assert_ne!(k(b"a"), k(b"a\0"));
        assert_ne!(k(b"a\0"), k(b"a\0\0"));
        assert_ne!(k(b"u1"), k(b"U1"));
        assert_ne!(k(b"u1"), k(b"u1 "));
    }

    #[test]
    fn order_is_byte_wise() {
        let mut ids: Vec<&[u8]> = vec![
            b"a", b"a\0", b"a\0b", b"ab", b"abc", b"b", b"B", b"Z", b"_", b"0", b"9", b"u10", b"u9", b"u1",
            b"exactly15bytes!", b"exactly15bytes", b"sixteen bytes!!!", b"sixteen bytes!!", b"sixteen bytes!!!!",
            "é".as_bytes(), "ü1".as_bytes(), b"\xff", b"zzzzzzzzzzzzzzzzzzzzzzzz", b"A", b"aa",
        ];
        for a in &ids {
            for b in &ids {
                assert_eq!(k(a).cmp_bytes(&k(b)), a.cmp(b), "{a:?} vs {b:?}");
            }
        }
        ids.sort();
        ids.dedup();
        let mut keys: Vec<Key> = ids.iter().rev().map(|s| k(s)).collect();
        keys.sort_by(|a, b| a.cmp_bytes(b));
        assert_eq!(keys.iter().map(|x| x.bytes()).collect::<Vec<_>>(), ids);
    }

    #[test]
    fn identity_of_long_ids() {
        let a = vec![b'e'; 70_000];
        let mut b = a.clone();
        *b.last_mut().unwrap() = b'f';
        let (ka, kb) = unsafe { (Key::identity(&a), Key::identity(&b)) };
        assert_ne!(ka, kb);
        assert_eq!(ka, unsafe { Key::identity(&a.clone()) });
        let mut short = b"e1".to_vec();
        short.resize(32, 0);
        assert_eq!(unsafe { Key::identity(&short[..2]) }, k(b"e1"));
    }
}
