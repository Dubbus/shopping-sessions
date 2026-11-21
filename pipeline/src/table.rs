//! String tables: a plain byte-string map, and a concurrent interner that hands
//! out dense ids for user IDs.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use hashbrown::HashTable;

pub type Str = &'static [u8];

#[inline]
pub fn hash(s: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(s)
}

/// Byte string to a small integer, single-threaded.
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

const SHARD_BITS: u32 = 10;

/// Gives every distinct byte string a dense id, from many threads at once.
pub struct Interner {
    shards: Vec<Mutex<HashTable<(Str, u32)>>>,
    next: AtomicU32,
}

impl Interner {
    pub fn new() -> Self {
        Interner {
            shards: (0..1 << SHARD_BITS).map(|_| Mutex::new(HashTable::new())).collect(),
            next: AtomicU32::new(0),
        }
    }

    #[inline]
    pub fn intern(&self, s: Str) -> u32 {
        let h = hash(s);
        // Middle bits pick the shard: the table itself uses the low and the top bits.
        let shard = ((h >> 32) & ((1 << SHARD_BITS) - 1)) as usize;
        let mut table = self.shards[shard].lock().unwrap();
        if let Some(e) = table.find(h, |e| e.0 == s) {
            return e.1;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        table.insert_unique(h, (s, id), |e| hash(e.0));
        id
    }

    /// The string of every id, indexed by id.
    pub fn into_names(self) -> Vec<Str> {
        let n = self.next.load(Ordering::Relaxed) as usize;
        let mut names: Vec<Str> = vec![b""; n];
        for shard in self.shards {
            for (s, id) in shard.into_inner().unwrap() {
                names[id as usize] = s;
            }
        }
        names
    }
}
