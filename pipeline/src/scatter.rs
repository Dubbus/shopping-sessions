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

/// On unless SS_NO_REUSE: blocks a stage has finished reading are pooled, and new blocks are taken from
/// the pool before fresh memory (whose first touch makes the kernel zero it).
pub static REUSE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Pooled blocks, all exactly `BLOCK` bytes and 64-byte aligned.
static POOL: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());

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

/// Benchmarking only: plain stores instead of non-temporal ones.
pub static PLAIN_STORES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Copies `lines` 64-byte lines from `from` to the 64-byte-aligned `to`, bypassing the cache.
#[inline]
unsafe fn stream(from: *const u8, to: *mut u8, lines: usize) {
    unsafe {
        if PLAIN_STORES.load(std::sync::atomic::Ordering::Relaxed) {
            std::ptr::copy_nonoverlapping(from, to, lines * 64);
            return;
        }
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

/// One thread's writer. `T` must be plain data whose size tiles a few cache lines evenly
/// (24 or 32 bytes here). Everything about the layout is a compile-time constant of `T`.
pub struct Scatter<T> {
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
    const SIZE: usize = std::mem::size_of::<T>();
    /// Records per staging line: the smallest run of records that fills whole cache lines.
    const PER_LINE: usize = {
        let mut n = 1;
        while (n * Self::SIZE) % 64 != 0 {
            n += 1;
            assert!(n <= 8, "record size must tile cache lines");
        }
        n
    };
    /// A staging line's size in bytes, a multiple of 64.
    const LINE: usize = Self::PER_LINE * Self::SIZE;
    const BLOCK: usize = BLOCK / Self::LINE * Self::LINE;

    pub fn new(parts: usize) -> Self {
        let mut arena = Arena { next: std::ptr::null_mut(), end: std::ptr::null_mut() };
        let stage = arena.alloc((parts * Self::LINE).next_multiple_of(64));
        Scatter {
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
        assert!(part < self.staged.len());
        // SAFETY: `part` is in range for every per-partition array; the staging line has
        // room for `PER_LINE` records and holds fewer than that.
        unsafe {
            let staged = self.staged.get_unchecked_mut(part);
            let n = *staged as usize;
            let line = self.stage.add(part * Self::LINE);
            (line.add(n * Self::SIZE) as *mut T).write_unaligned(record);
            if n + 1 == Self::PER_LINE {
                *staged = 0;
                let to = self.room(part);
                stream(line, to, Self::LINE / 64);
            } else {
                *staged = n as u8 + 1;
            }
        }
    }

    /// Where the next full line of `part` goes, starting a new block if the current one is full.
    ///
    /// # Safety
    /// `part` must be in range.
    #[inline]
    unsafe fn room(&mut self, part: usize) -> *mut u8 {
        unsafe {
            let at = self.at.get_unchecked_mut(part);
            if *at == *self.end.get_unchecked(part) {
                return self.new_block(part);
            }
            let to = *at;
            *at = to.add(Self::LINE);
            to
        }
    }

    #[cold]
    fn new_block(&mut self, part: usize) -> *mut u8 {
        let pooled = if REUSE.load(std::sync::atomic::Ordering::Relaxed) { POOL.lock().unwrap().pop() } else { None };
        let block = match pooled {
            Some(b) => b as *mut u8,
            None => self.arena.alloc(Self::BLOCK),
        };
        self.blocks[part].push(block);
        self.at[part] = unsafe { block.add(Self::LINE) };
        self.end[part] = unsafe { block.add(Self::BLOCK) };
        block
    }

    /// Writes out the partly filled lines and returns what was scattered.
    pub fn finish(mut self) -> Scattered<T> {
        let mut counts = Vec::with_capacity(self.staged.len());
        for part in 0..self.staged.len() {
            let left = self.staged[part] as usize;
            let full = match self.blocks[part].len() {
                0 => 0,
                n => (n - 1) * Self::BLOCK + (self.at[part] as usize - self.blocks[part][n - 1] as usize),
            };
            if left > 0 {
                // SAFETY: `part` is in range, and the line holds `left` records.
                unsafe {
                    let to = self.room(part);
                    std::ptr::copy_nonoverlapping(self.stage.add(part * Self::LINE), to, left * Self::SIZE);
                }
            }
            counts.push(full / Self::SIZE + left);
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        Scattered { per_block: Self::BLOCK / Self::SIZE, blocks: std::mem::take(&mut self.blocks), counts, kind: PhantomData }
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
    /// Hands this partition's blocks to the pool: the caller will not read them
    /// again. Only whole `BLOCK`-byte blocks are pooled.
    pub fn recycle(&self, part: usize) {
        if REUSE.load(std::sync::atomic::Ordering::Relaxed) && self.per_block * std::mem::size_of::<T>() == BLOCK {
            POOL.lock().unwrap().extend(self.blocks[part].iter().map(|&b| b as usize));
        }
    }

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
