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
