//! A byte-string map for the small lookup files (products, campaigns).

use hashbrown::HashTable;

pub type Str = &'static [u8];

#[inline]
pub fn hash(s: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(s)
}

#[derive(Default)]
pub struct StrMap {
    table: HashTable<(Str, u32)>,
}

impl StrMap {
    #[inline]
    pub fn get(&self, s: &[u8]) -> Option<u32> {
        self.table.find(hash(s), |e| e.0 == s).map(|e| e.1)
    }

    /// Inserts if absent; returns the value now stored for `s`.
    pub fn get_or_insert(&mut self, s: Str, value: u32) -> u32 {
        let h = hash(s);
        if let Some(e) = self.table.find(h, |e| e.0 == s) {
            return e.1;
        }
        self.table.insert_unique(h, (s, value), |e| hash(e.0));
        value
    }
}

/// Campaign ID -> campaign number. IDs of the usual shape, `c_` and a number, are looked
/// up by that number in an array; every other ID goes through the general map.
#[derive(Default)]
pub struct Campaigns {
    by_number: Vec<u32>,
    others: StrMap,
}

const NONE: u32 = u32::MAX;
/// Numbers up to here get an array slot; a larger one is an ordinary ID.
const DIRECT: usize = 1 << 20;

/// The number in an ID written `c_<digits>` with no leading zero, if it is below `DIRECT`.
#[inline]
fn campaign_number(s: &[u8]) -> Option<usize> {
    if !(3..=9).contains(&s.len()) || s[0] != b'c' || s[1] != b'_' || (s[2] == b'0' && s.len() > 3) {
        return None;
    }
    let mut n = 0usize;
    for &c in &s[2..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as usize;
    }
    if n < DIRECT { Some(n) } else { None }
}

impl Campaigns {
    #[inline]
    pub fn get(&self, s: &[u8]) -> Option<u32> {
        match campaign_number(s) {
            // An ID of this shape that is in the file is in the array, so a miss is final.
            Some(n) => self.by_number.get(n).copied().filter(|&c| c != NONE),
            None => self.others.get(s),
        }
    }

    /// Inserts if absent; returns the value now stored for `s`.
    pub fn get_or_insert(&mut self, s: Str, value: u32) -> u32 {
        match campaign_number(s) {
            Some(n) => {
                if self.by_number.len() <= n {
                    self.by_number.resize(n + 1, NONE);
                }
                if self.by_number[n] == NONE {
                    self.by_number[n] = value;
                }
                self.by_number[n]
            }
            None => self.others.get_or_insert(s, value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaigns_by_number_and_by_name() {
        let mut campaigns = Campaigns::default();
        let ids: [&'static [u8]; 12] =
            [b"c_1", b"c_0", b"c_10", b"c_01", b"C_1", b"c_", b"c_1 ", b"c_9999999", b"c_1048576", b"organic", b"c_1x", b"c_00"];
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(campaigns.get_or_insert(id, i as u32), i as u32);
        }
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(campaigns.get(id), Some(i as u32), "{:?}", String::from_utf8_lossy(id));
            assert_eq!(campaigns.get_or_insert(id, 99), i as u32);
        }
        for missing in [&b"c_2"[..], b"c_100", b"c_1048575", b"c_1048577", b"c_001", b"", b"c", b"c_1\r", b" c_1", b"c_12345678901234567890"] {
            assert_eq!(campaigns.get(missing), None, "{:?}", String::from_utf8_lossy(missing));
        }
    }
}
