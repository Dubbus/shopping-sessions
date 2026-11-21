//! Linked IDs: from the pairs in links/ to a table that maps an ID to the name
//! of its person (the byte-wise smallest ID of its group).

use hashbrown::HashTable;

use crate::key::{Key, partition, remix};
use crate::par;

/// ID -> person name, for the IDs that are not themselves their person's name.
/// An ID that is not in the table is a person on its own, or is the name already.
pub struct Renames {
    /// Open addressing on the top bits of `Key::hash`. An empty slot has a zero key.
    slots: Vec<[Key; 2]>,
    shift: u32,
}

impl Renames {
    fn new(pairs: impl Iterator<Item = (Key, Key)>, count: usize) -> Renames {
        if count == 0 {
            return Renames { slots: Vec::new(), shift: 0 };
        }
        let capacity = (count * 2).next_power_of_two().max(16);
        let mut table = Renames { slots: vec![[Key::default(); 2]; capacity], shift: 64 - capacity.trailing_zeros() };
        for (id, name) in pairs {
            let mut at = (id.hash() >> table.shift) as usize;
            while table.slots[at][0].hi != 0 {
                at = (at + 1) & (capacity - 1);
            }
            table.slots[at] = [id, name];
        }
        table
    }

    /// Starts loading the slot a later [`Renames::person`] call with this hash will read.
    #[inline]
    pub fn prefetch(&self, hash: u64) {
        #[cfg(target_arch = "x86_64")]
        if !self.slots.is_empty() {
            // SAFETY: the index is below the capacity, and prefetching has no effect on memory.
            unsafe {
                std::arch::x86_64::_mm_prefetch(self.slots.as_ptr().add((hash >> self.shift) as usize) as *const i8, std::arch::x86_64::_MM_HINT_T0);
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = hash;
    }

    /// The name of the person `id` belongs to. `hash` is `id.hash()`.
    #[inline]
    pub fn person(&self, id: Key, hash: u64) -> Key {
        if self.slots.is_empty() {
            return id;
        }
        let mut at = (hash >> self.shift) as usize;
        loop {
            let slot = &self.slots[at];
            if slot[0].hi == 0 {
                return id;
            }
            if slot[0] == id {
                return slot[1];
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

/// `links[t]` are the valid link lines one thread parsed. `bits` sets how finely the
/// IDs are partitioned while they are numbered.
pub fn resolve(threads: usize, links: &[Vec<(Key, Key)>], bits: u32) -> Renames {
    let parts = 1usize << bits;
    let mut link_base = vec![0usize; links.len() + 1];
    for (t, thread) in links.iter().enumerate() {
        link_base[t + 1] = link_base[t] + thread.len();
    }
    let link_count = link_base[links.len()];
    if link_count == 0 {
        return Renames::new(std::iter::empty(), 0);
    }

    // Number every ID that appears in a link, partition by partition.
    // An endpoint: the ID, and which side of which link it is.
    let ends = par::run(
        threads,
        links.len(),
        |_| (0..parts).map(|_| Vec::new()).collect::<Vec<Vec<(Key, u32)>>>(),
        |out, t| {
            for (i, &(a, b)) in links[t].iter().enumerate() {
                let slot = ((link_base[t] + i) * 2) as u32;
                out[partition(a.hash(), bits)].push((a, slot));
                out[partition(b.hash(), bits)].push((b, slot + 1));
            }
        },
    );
    struct Nodes {
        ids: Vec<Key>,
        /// (endpoint slot, index into `ids`)
        ends: Vec<(u32, u32)>,
    }
    let nodes: Vec<Nodes> = par::map(threads, parts, |part| {
        let mut table: HashTable<(Key, u32)> = HashTable::new();
        let mut found = Nodes { ids: Vec::new(), ends: Vec::new() };
        for thread in &ends {
            for &(id, slot) in &thread[part] {
                let h = remix(id.hash());
                let index = match table.find(h, |e| e.0 == id) {
                    Some(e) => e.1,
                    None => {
                        let index = found.ids.len() as u32;
                        found.ids.push(id);
                        table.insert_unique(h, (id, index), |e| remix(e.0.hash()));
                        index
                    }
                };
                found.ends.push((slot, index));
            }
        }
        found
    });
    drop(ends);
    let mut node_base = vec![0u32; parts + 1];
    for part in 0..parts {
        node_base[part + 1] = node_base[part] + nodes[part].ids.len() as u32;
    }
    let node_count = node_base[parts] as usize;
    let mut endpoint = vec![0u32; link_count * 2];
    {
        let cells = par::Shared(endpoint.as_mut_ptr());
        par::run(threads, parts, |_| (), |_, part| {
            let cells = cells;
            for &(slot, index) in &nodes[part].ends {
                // SAFETY: every slot belongs to exactly one endpoint, so no two writes collide.
                unsafe { *cells.0.add(slot as usize) = node_base[part] + index };
            }
        });
    }

    // Links step 2: union-find over the link pairs.
    let mut parent: Vec<u32> = (0..node_count as u32).collect();
    for pair in endpoint.chunks_exact(2) {
        let (a, b) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
        if a != b {
            parent[a.max(b) as usize] = a.min(b);
        }
    }
    drop(endpoint);

    // Links step 3: each person is named by the byte-wise smallest of their IDs.
    let ids: Vec<Key> = nodes.iter().flat_map(|n| n.ids.iter().copied()).collect();
    let mut name = ids.clone();
    let mut root_of = vec![0u32; node_count];
    for node in 0..node_count {
        let root = find(&mut parent, node as u32);
        root_of[node] = root;
        if ids[node].cmp_bytes(&name[root as usize]).is_lt() {
            name[root as usize] = ids[node];
        }
    }
    let renamed = (0..node_count).filter(|&n| name[root_of[n] as usize] != ids[n]).count();
    Renames::new((0..node_count).map(|n| (ids[n], name[root_of[n] as usize])).filter(|(id, name)| id != name), renamed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Key {
        Key::text_copy(s.as_bytes())
    }

    #[test]
    fn names_are_the_smallest_id() {
        let links = vec![
            vec![(k("b"), k("c")), (k("x"), k("x")), (k("m"), k("n"))],
            vec![(k("c"), k("a")), (k("c"), k("b")), (k("U1"), k("u1")), (k("n"), k("o")), (k("p"), k("o"))],
        ];
        for bits in [0, 2, 5] {
            let renames = resolve(3, &links, bits);
            let person = |s: &str| String::from_utf8(renames.person(k(s), k(s).hash()).bytes().to_vec()).unwrap();
            for (id, name) in [("a", "a"), ("b", "a"), ("c", "a"), ("x", "x"), ("u1", "U1"), ("U1", "U1"), ("m", "m"), ("n", "m"), ("o", "m"), ("p", "m"), ("zz", "zz")] {
                assert_eq!(person(id), name, "bits {bits}");
            }
        }
        let none = resolve(2, &[Vec::new(), Vec::new()], 3);
        assert_eq!(none.person(k("a"), k("a").hash()), k("a"));
    }
}
