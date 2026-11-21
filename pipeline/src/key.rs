//! IDs as 8-byte keys. An ID of at most 9 bytes, each `0x01..=0x7f`, packs 7 bits
//! per byte, first byte most significant, zero-padded: two packed keys are equal
//! exactly when the IDs are, and compare like the bytes do. The generator's IDs
//! (a letter and 8 base-36 characters) all pack.
//!
//! Any other ID (longer, or with a NUL or non-ASCII byte) is rare and is interned
//! in a global table: its key is the top bit plus its index there, so equal keys
//! still mean equal IDs, and comparisons look the bytes up.
//!
//! (The 7-bit packing is from Adarsh's implementation on `master`.)

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub type Id = u64;

const INTERNED: u64 = 1 << 63;
const LOW7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const HIGH: u64 = 0x8080_8080_8080_8080;

#[derive(Default)]
struct Interner {
    index: HashMap<&'static [u8], u64>,
    strings: Vec<&'static [u8]>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);

#[cold]
fn intern(s: &[u8]) -> Id {
    let mut guard = INTERNER.lock().unwrap();
    let interner = guard.get_or_insert_with(Interner::default);
    if let Some(&index) = interner.index.get(s) {
        return INTERNED | index;
    }
    let copy: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
    let index = interner.strings.len() as u64;
    interner.strings.push(copy);
    interner.index.insert(copy, index);
    INTERNED | index
}

fn interned(id: Id) -> &'static [u8] {
    INTERNER.lock().unwrap().as_ref().expect("interned key without a table").strings[(id & !INTERNED) as usize]
}

/// Bits 7 apart, most significant byte first, squeezed together: 8 bytes to 56 bits.
#[inline]
fn squeeze(big_endian: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    // SAFETY: the `bmi2` feature is enabled for this build.
    unsafe {
        std::arch::x86_64::_pext_u64(big_endian, LOW7)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut packed = 0;
        for byte in big_endian.to_be_bytes() {
            packed = packed << 7 | (byte & 0x7f) as u64;
        }
        packed
    }
}

/// The key of a non-empty ID.
///
/// # Safety
/// 16 bytes must be readable from `s.as_ptr()` (the chunk buffers keep that much slack).
#[inline(always)]
pub unsafe fn id(s: &[u8]) -> Id {
    let n = s.len();
    if n <= 9 {
        // SAFETY: the caller guarantees 16 readable bytes.
        let raw = u64::from_le(unsafe { std::ptr::read_unaligned(s.as_ptr() as *const u64) });
        let head = if n >= 8 { raw } else { raw & ((1u64 << (8 * n)) - 1) };
        let ninth = if n == 9 { unsafe { *s.as_ptr().add(8) } } else { 0 };
        // Every byte of the ID must be 0x01..=0x7f. `zeros` has the high bit of each zero byte.
        let zeros = !((head & LOW7).wrapping_add(LOW7) | head | LOW7);
        let live = if n >= 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
        if (head & HIGH) | (zeros & live) == 0 && (n < 9 || (ninth != 0 && ninth < 0x80)) {
            return squeeze(head.swap_bytes()) << 7 | ninth as u64;
        }
    }
    intern(s)
}

/// Like [`id`], for a slice with no slack after it.
#[cfg(test)]
pub fn id_copy(s: &[u8]) -> Id {
    if s.len() > 9 {
        return intern(s);
    }
    let mut padded = [0u8; 16];
    padded[..s.len()].copy_from_slice(s);
    unsafe { id(&padded[..s.len()]) }
}

/// Whether the ID is one of the rare interned ones, whose key does not order like its bytes.
#[inline]
pub fn is_interned(id: Id) -> bool {
    id & INTERNED != 0
}

/// Appends the ID's bytes to `out`.
#[inline]
pub fn write_bytes(id: Id, out: &mut Vec<u8>) {
    if id & INTERNED != 0 {
        out.extend_from_slice(interned(id));
        return;
    }
    for i in 0..9 {
        let byte = (id >> (56 - 7 * i)) as u8 & 0x7f;
        if byte == 0 {
            break;
        }
        out.push(byte);
    }
}

/// Byte-wise order of the two IDs.
#[inline]
pub fn cmp_bytes(a: Id, b: Id) -> Ordering {
    if (a | b) & INTERNED == 0 {
        return a.cmp(&b);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    write_bytes(a, &mut x);
    write_bytes(b, &mut y);
    x.cmp(&y)
}

/// A well-mixed 64-bit hash. Partitions take its top bits; tables use [`remix`] of it.
#[inline]
pub fn hash(id: Id) -> u64 {
    let x = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 32)
}

/// A second hash from the first, independent of the bits a partition was chosen by.
#[inline]
pub fn remix(h: u64) -> u64 {
    (h ^ (h >> 29)).wrapping_mul(0xD6E8_FEB8_6659_FD93)
}

#[inline]
pub fn partition(h: u64, bits: u32) -> usize {
    if bits == 0 { 0 } else { (h >> (64 - bits)) as usize }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(id: Id) -> Vec<u8> {
        let mut out = Vec::new();
        write_bytes(id, &mut out);
        out
    }

    const SAMPLES: &[&[u8]] = &[
        b"u", b"u1", b"U1", b"u1 ", b" u1", b"u7ztjlcfs", b"U14NPKDLL", b"12345678", b"123456789", b"1234567890", b"a\0", b"a\0b",
        b"\x01", b"\x7f", b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f", b"\x80", b"abcdefgh\x80", b"abcdefg\0", b"abcdefgh\0", b"a", b"aa",
        b"ab", b"b", b"B", b"Z", b"_", b"0", b"9", b"u10", b"u9", "é".as_bytes(), "ü1".as_bytes(), b"\xff", b"zzzzzzzzzzzzzzzzzzzzzzzz",
        b"exactly15bytes!", b"sixteen bytes!!!", b"e3\r", b"e3", b"abcdefghi", b"abcdefghj", b"abcdefgh",
    ];

    #[test]
    fn round_trips_and_is_exact() {
        for a in SAMPLES {
            assert_eq!(bytes(id_copy(a)), *a);
            for b in SAMPLES {
                assert_eq!(id_copy(a) == id_copy(b), a == b, "{a:?} {b:?}");
            }
        }
        let long = vec![b'x'; 70_000];
        let mut other = long.clone();
        *other.last_mut().unwrap() = b'y';
        assert_ne!(id_copy(&long), id_copy(&other));
        assert_eq!(id_copy(&long), id_copy(&long.clone()));
        assert_eq!(bytes(id_copy(&long)), long);
    }

    #[test]
    fn order_is_byte_wise() {
        for a in SAMPLES {
            for b in SAMPLES {
                assert_eq!(cmp_bytes(id_copy(a), id_copy(b)), a.cmp(b), "{a:?} vs {b:?}");
            }
        }
    }

    /// The in-buffer constructor reads past the ID; what it reads must not matter.
    #[test]
    fn ignores_what_follows() {
        for s in SAMPLES {
            if s.len() > 9 {
                continue;
            }
            for fill in [0u8, b',', 0xff, b'a'] {
                let mut buf = s.to_vec();
                buf.resize(s.len() + 16, fill);
                assert_eq!(unsafe { id(&buf[..s.len()]) }, id_copy(s), "{s:?} fill {fill}");
            }
        }
    }
}
