//! Scattering records into partitions without paying a cache miss per record.
//!
//! Each partition has a small staging line that stays in cache. When a line is
//! full it is copied to the partition's current block with non-temporal stores,
//! which go straight to memory: the blocks are not read again until the next
//! stage. Blocks come from large huge-page regions and are never moved or freed.

use std::marker::PhantomData;

const REGION: usize = 16 << 20;
const HUGE: usize = 2 << 20;
/// Target size of one block; the real size is a whole number of staging lines.
const BLOCK: usize = 32 << 10;

/// Hands out 64-byte-aligned memory from large regions. Nothing is ever returned.
struct Arena {
    next: *mut u8,
    end: *mut u8,
}

impl Arena {
    fn alloc(&mut self, bytes: usize) -> *mut u8 {
        if (self.end as usize) - (self.next as usize) < bytes {
            let size = REGION.max(bytes.next_multiple_of(HUGE));
            let layout = std::alloc::Layout::from_size_align(size, HUGE).unwrap();
            // SAFETY: the layout has a non-zero size.
            let region = unsafe { std::alloc::alloc(layout) };
            assert!(!region.is_null(), "out of memory");
            #[cfg(target_os = "linux")]
            unsafe {
                libc::madvise(region as *mut _, size, libc::MADV_HUGEPAGE);
            }
            self.next = region;
            self.end = unsafe { region.add(size) };
        }
        let at = self.next;
        self.next = unsafe { at.add(bytes) };
        at
    }
}

/// Copies `lines` 64-byte lines from `from` to the 64-byte-aligned `to`, bypassing the cache.
#[inline]
unsafe fn stream(from: *const u8, to: *mut u8, lines: usize) {
    unsafe {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        {
            use std::arch::x86_64::*;
            for i in 0..lines {
                _mm512_stream_si512(to.add(i * 64) as *mut _, _mm512_loadu_si512(from.add(i * 64) as *const _));
            }
        }
        #[cfg(all(target_arch = "x86_64", not(target_feature = "avx512f")))]
        {
            use std::arch::x86_64::*;
            for i in 0..lines * 4 {
                _mm_stream_si128(to.add(i * 16) as *mut _, _mm_loadu_si128(from.add(i * 16) as *const _));
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        std::ptr::copy_nonoverlapping(from, to, lines * 64);
    }
}

/// One thread's writer. `T` must be plain data whose size divides a few cache lines evenly
/// (32 or 48 bytes here).
pub struct Scatter<T> {
    /// Records per staging line, and the line's size in bytes (a multiple of 64).
    per_line: usize,
    line: usize,
    block: usize,
    stage: *mut u8,
    /// Records staged per partition.
    staged: Vec<u8>,
    /// Write position and end of each partition's current block.
    at: Vec<*mut u8>,
    end: Vec<*mut u8>,
    blocks: Vec<Vec<*mut u8>>,
    arena: Arena,
    kind: PhantomData<T>,
}

// SAFETY: a Scatter owns everything its pointers refer to.
unsafe impl<T: Send> Send for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize) -> Self {
        let size = std::mem::size_of::<T>();
        // The smallest run of records that fills whole cache lines.
        let per_line = (1..=8).find(|n| (n * size) % 64 == 0).expect("record size must tile cache lines");
        let line = per_line * size;
        let mut arena = Arena { next: std::ptr::null_mut(), end: std::ptr::null_mut() };
        let stage = arena.alloc((parts * line).next_multiple_of(64));
        Scatter {
            per_line,
            line,
            block: (BLOCK / line).max(1) * line,
            stage,
            staged: vec![0; parts],
            at: vec![std::ptr::null_mut(); parts],
            end: vec![std::ptr::null_mut(); parts],
            blocks: (0..parts).map(|_| Vec::new()).collect(),
            arena,
            kind: PhantomData,
        }
    }

    #[inline]
    pub fn push(&mut self, part: usize, record: T) {
        // SAFETY: `part` is checked by the indexing below; the staging line has room for
        // `per_line` records and `staged[part] < per_line`.
        unsafe {
            let n = self.staged[part] as usize;
            let line = self.stage.add(part * self.line);
            (line.add(n * std::mem::size_of::<T>()) as *mut T).write_unaligned(record);
            if n + 1 == self.per_line {
                self.staged[part] = 0;
                let to = self.room(part);
                stream(line, to, self.line / 64);
            } else {
                self.staged[part] = n as u8 + 1;
            }
        }
    }

    /// Where the next full line of `part` goes, starting a new block if the current one is full.
    #[inline]
    fn room(&mut self, part: usize) -> *mut u8 {
        if self.at[part] == self.end[part] {
            let block = self.arena.alloc(self.block);
            self.blocks[part].push(block);
            self.at[part] = block;
            self.end[part] = unsafe { block.add(self.block) };
        }
        let to = self.at[part];
        self.at[part] = unsafe { to.add(self.line) };
        to
    }

    /// Writes out the partly filled lines and returns what was scattered.
    pub fn finish(mut self) -> Scattered<T> {
        let size = std::mem::size_of::<T>();
        let mut counts = Vec::with_capacity(self.staged.len());
        for part in 0..self.staged.len() {
            let left = self.staged[part] as usize;
            let full = match self.blocks[part].len() {
                0 => 0,
                n => (n - 1) * self.block + (self.at[part] as usize - self.blocks[part][n - 1] as usize),
            };
            if left > 0 {
                let to = self.room(part);
                unsafe { std::ptr::copy_nonoverlapping(self.stage.add(part * self.line), to, left * size) };
            }
            counts.push(full / size + left);
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        Scattered { per_block: self.block / size, blocks: std::mem::take(&mut self.blocks), counts, kind: PhantomData }
    }
}

/// The partitions one thread wrote.
pub struct Scattered<T> {
    per_block: usize,
    blocks: Vec<Vec<*mut u8>>,
    counts: Vec<usize>,
    kind: PhantomData<T>,
}

// SAFETY: read-only after `finish`, and the memory is never freed.
unsafe impl<T: Send> Send for Scattered<T> {}
unsafe impl<T: Sync> Sync for Scattered<T> {}

impl<T: Copy> Scattered<T> {
    #[cfg(test)]
    pub fn len(&self, part: usize) -> usize {
        self.counts[part]
    }

    /// The records of one partition, block by block.
    #[inline]
    pub fn slices(&self, part: usize) -> impl Iterator<Item = &[T]> {
        let mut left = self.counts[part];
        self.blocks[part].iter().map(move |&block| {
            let n = left.min(self.per_block);
            left -= n;
            // SAFETY: the block holds `n` initialised records and is 64-byte aligned.
            unsafe { std::slice::from_raw_parts(block as *const T, n) }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C)]
    struct Wide([u64; 6]);
    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C)]
    struct Narrow([u64; 4]);

    #[test]
    fn everything_comes_back_in_order() {
        for parts in [1usize, 3, 64] {
            for total in [0usize, 1, 3, 4, 5, 679, 680, 681, 5000, 200_000] {
                let mut wide = Scatter::<Wide>::new(parts);
                let mut narrow = Scatter::<Narrow>::new(parts);
                for i in 0..total as u64 {
                    // Uneven: partition 0 gets most.
                    let part = if i % 3 == 0 { (i as usize / 3) % parts } else { 0 };
                    wide.push(part, Wide([i, part as u64, 2, 3, 4, i ^ 5]));
                    narrow.push(part, Narrow([i, part as u64, 7, !i]));
                }
                let (wide, narrow) = (wide.finish(), narrow.finish());
                let mut seen = 0;
                for part in 0..parts {
                    let w: Vec<Wide> = wide.slices(part).flatten().copied().collect();
                    let n: Vec<Narrow> = narrow.slices(part).flatten().copied().collect();
                    assert_eq!(w.len(), wide.len(part));
                    assert_eq!(n.len(), w.len());
                    let mut last = None;
                    for (a, b) in w.iter().zip(&n) {
                        assert_eq!((a.0[1], b.0[1]), (part as u64, part as u64));
                        assert_eq!((a.0[5], b.0[3], b.0[0]), (a.0[0] ^ 5, !a.0[0], a.0[0]));
                        assert!(last < Some(a.0[0]), "order kept within a partition");
                        last = Some(a.0[0]);
                    }
                    seen += w.len();
                }
                assert_eq!(seen, total, "parts {parts} total {total}");
            }
        }
    }
}
