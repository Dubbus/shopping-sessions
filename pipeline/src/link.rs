//! Linked IDs: from the pairs in links/ to a table that maps an ID to the name
//! of its person (the byte-wise smallest ID of its group).

use crate::key::{self, Id};

/// ID -> person name, for the IDs that are not themselves their person's name.
/// An ID that is not in the table is a person on its own, or is the name already.
///
/// Most lookups are for such IDs, and the table is far bigger than the cache, so each slot
/// has a one-byte tag in a separate, much smaller array: a lookup reads tags only, and goes
/// to the table itself just when a tag matches.
pub struct Renames {
    /// Per slot: 0 if empty, else a nonzero byte of the ID's hash.
    tags: Vec<u8>,
    /// Open addressing on the top bits of `key::hash`.
    slots: Vec<[Id; 2]>,
    shift: u32,
}

/// The slot `hash` starts probing at, in a table of `1 << (64 - shift)` slots.
#[inline]
fn home(hash: u64, shift: u32) -> usize {
    (hash >> shift) as usize
}

/// A nonzero byte from hash bits the slot index does not use.
#[inline]
fn tag(hash: u64) -> u8 {
    hash as u8 | 1
}

#[inline]
fn prefetch<T>(slot: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetching has no effect on memory, whatever the address.
    unsafe {
        std::arch::x86_64::_mm_prefetch(slot as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = slot;
}

impl Renames {
    fn new(pairs: &[(Id, Id)]) -> Renames {
        if pairs.is_empty() {
            return Renames { tags: Vec::new(), slots: Vec::new(), shift: 0 };
        }
        let capacity = (pairs.len() * 2).next_power_of_two().max(16);
        let mut table = Renames { tags: vec![0; capacity], slots: vec![[0; 2]; capacity], shift: 64 - capacity.trailing_zeros() };
        for &(id, name) in pairs {
            let hash = key::hash(id);
            let mut at = home(hash, table.shift);
            while table.tags[at] != 0 {
                at = (at + 1) & (capacity - 1);
            }
            table.tags[at] = tag(hash);
            table.slots[at] = [id, name];
        }
        table
    }

    /// First of two steps before a [`Renames::person`] call: start loading the ID's tag.
    #[inline]
    pub fn prefetch_tag(&self, id: Id) {
        if !self.slots.is_empty() {
            prefetch(self.tags.as_ptr().wrapping_add(home(key::hash(id), self.shift)));
        }
    }

    /// Second step, once the tag has had time to arrive: if it matches, start loading the slot.
    #[inline]
    pub fn prefetch_slot(&self, id: Id) {
        if !self.slots.is_empty() {
            let hash = key::hash(id);
            let at = home(hash, self.shift);
            if self.tags[at] == tag(hash) {
                prefetch(self.slots.as_ptr().wrapping_add(at));
            }
        }
    }

    /// The name of the person `id` belongs to.
    #[inline]
    pub fn person(&self, id: Id) -> Id {
        if self.slots.is_empty() {
            return id;
        }
        let hash = key::hash(id);
        let wanted = tag(hash);
        let mut at = home(hash, self.shift);
        loop {
            let found = self.tags[at];
            if found == 0 {
                return id;
            }
            if found == wanted && self.slots[at][0] == id {
                return self.slots[at][1];
            }
            at = (at + 1) & (self.slots.len() - 1);
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let up = parent[parent[x as usize] as usize];
        parent[x as usize] = up;
        x = up;
    }
    x
}

/// `links` are the valid link lines. Runs on one thread, while the others parse events.
pub fn resolve(links: &[(Id, Id)]) -> Renames {
    if links.is_empty() {
        return Renames::new(&[]);
    }
    // Number every ID that appears in a link. One big table would miss the cache on every
    // lookup, so the endpoints are first dealt into buckets by the top bits of their hash
    // and each bucket is numbered with a table that fits in cache. Numbers therefore rise
    // with the hash, which is also the order of the rename table built at the end.
    let bits = (links.len() / 2048).max(1).next_power_of_two().trailing_zeros().min(12);
    let mut buckets: Vec<Vec<(Id, u32)>> = (0..1usize << bits).map(|_| Vec::with_capacity(links.len() * 2 / (1 << bits) * 5 / 4 + 16)).collect();
    for (i, &(a, b)) in links.iter().enumerate() {
        buckets[key::partition(key::hash(a), bits)].push((a, 2 * i as u32));
        buckets[key::partition(key::hash(b), bits)].push((b, 2 * i as u32 + 1));
    }
    let mut ids: Vec<Id> = Vec::with_capacity(links.len() * 3 / 2);
    // The node number of each endpoint: entries 2i and 2i + 1 are the two sides of link i.
    let mut endpoint = vec![0u32; links.len() * 2];
    let mut numbered: Vec<(Id, u32)> = Vec::new();
    for bucket in &buckets {
        let capacity = (bucket.len() * 2).next_power_of_two().max(16);
        let shift = 64 - capacity.trailing_zeros();
        numbered.clear();
        numbered.resize(capacity, (0, 0));
        for &(id, slot) in bucket {
            let mut at = home(key::remix(key::hash(id)), shift);
            endpoint[slot as usize] = loop {
                let entry = &mut numbered[at];
                if entry.0 == id {
                    break entry.1;
                }
                if entry.0 == 0 {
                    *entry = (id, ids.len() as u32);
                    ids.push(id);
                    break entry.1;
                }
                at = (at + 1) & (capacity - 1);
            };
        }
    }
    drop((buckets, numbered));

    // Links step 2: union-find over the link pairs.
    let mut parent: Vec<u32> = (0..ids.len() as u32).collect();
    for pair in endpoint.chunks_exact(2) {
        let (a, b) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
        if a != b {
            parent[a.max(b) as usize] = a.min(b);
        }
    }
    drop(endpoint);

    // Links step 3: each person is named by the byte-wise smallest of their IDs.
    let mut name = ids.clone();
    for node in 0..ids.len() {
        let root = find(&mut parent, node as u32) as usize;
        if key::cmp_bytes(ids[node], name[root]).is_lt() {
            name[root] = ids[node];
        }
    }
    // After the pass above every node points at its root or one step short of it.
    let renamed: Vec<(Id, Id)> =
        (0..ids.len()).map(|node| (ids[node], name[find(&mut parent, node as u32) as usize])).filter(|(id, name)| id != name).collect();
    Renames::new(&renamed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Id {
        key::id_copy(s.as_bytes())
    }

    #[test]
    fn names_are_the_smallest_id() {
        let long = "a-very-long-user-id-that-does-not-pack";
        let links = vec![
            (k("b"), k("c")),
            (k("x"), k("x")),
            (k("m"), k("n")),
            (k("c"), k("a")),
            (k("c"), k("b")),
            (k("U1"), k("u1")),
            (k("n"), k("o")),
            (k("p"), k("o")),
            (k(long), k("zz9")),
            (k("zz8"), k(long)),
        ];
        let renames = resolve(&links);
        for (id, name) in [
            ("a", "a"), ("b", "a"), ("c", "a"), ("x", "x"), ("u1", "U1"), ("U1", "U1"), ("m", "m"), ("n", "m"), ("o", "m"), ("p", "m"),
            ("zz", "zz"), ("zz9", long), ("zz8", long), (long, long),
        ] {
            assert_eq!(renames.person(k(id)), k(name), "{id}");
        }
        assert_eq!(resolve(&[]).person(k("a")), k("a"));
    }
}
