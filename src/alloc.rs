//! Global allocator: big blocks (>= 1 MiB) come straight from mmap, 2 MiB aligned, and on
//! Linux are marked MADV_HUGEPAGE so the kernel backs them with transparent huge pages.
//! That turns ~500k 4 KiB page faults (each zeroed, charged to the memory cgroup, and
//! taken under the process's mmap lock by 8 threads at once) into ~1k, and makes the
//! exit-time teardown cheap. Small blocks go to the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};

const BIG: usize = 1 << 20;
const HUGE: usize = 2 << 20;

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    #[cfg(target_os = "linux")]
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const PROT_RW: i32 = 1 | 2;
#[cfg(target_os = "linux")]
const MAP_PRIVATE_ANON: i32 = 0x02 | 0x20;
#[cfg(not(target_os = "linux"))]
const MAP_PRIVATE_ANON: i32 = 0x0002 | 0x1000;
#[cfg(target_os = "linux")]
const MADV_HUGEPAGE: i32 = 14;
const MAP_FAILED: *mut u8 = !0usize as *mut u8;

pub struct Alloc;

#[inline]
fn round(n: usize) -> usize {
    (n + HUGE - 1) & !(HUGE - 1)
}

unsafe fn big_alloc(size: usize) -> *mut u8 {
    let len = round(size);
    unsafe {
        // Over-map by 2 MiB, then trim so the block starts on a 2 MiB boundary.
        let raw = mmap(std::ptr::null_mut(), len + HUGE, PROT_RW, MAP_PRIVATE_ANON, -1, 0);
        if raw == MAP_FAILED {
            return std::ptr::null_mut();
        }
        let start = ((raw as usize + HUGE - 1) & !(HUGE - 1)) as *mut u8;
        let head = start as usize - raw as usize;
        if head > 0 {
            munmap(raw, head);
        }
        let tail = HUGE - head;
        if tail > 0 {
            munmap(start.add(len), tail);
        }
        #[cfg(target_os = "linux")]
        madvise(start, len, MADV_HUGEPAGE);
        start
    }
}

unsafe impl GlobalAlloc for Alloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            unsafe { big_alloc(l.size()) }
        } else {
            unsafe { System.alloc(l) }
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            unsafe { big_alloc(l.size()) } // fresh anonymous pages are already zero
        } else {
            unsafe { System.alloc_zeroed(l) }
        }
    }

    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= BIG && l.align() <= HUGE {
            unsafe { munmap(p, round(l.size())) };
        } else {
            unsafe { System.dealloc(p, l) }
        }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let old_big = l.size() >= BIG && l.align() <= HUGE;
        let new_big = new_size >= BIG && l.align() <= HUGE;
        if !old_big && !new_big {
            return unsafe { System.realloc(p, l, new_size) };
        }
        if old_big && new_big && round(new_size) == round(l.size()) {
            return p; // still fits in the mapping
        }
        unsafe {
            let nl = Layout::from_size_align_unchecked(new_size, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
                self.dealloc(p, l);
            }
            q
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn big_and_small_round_trip() {
        let mut v: Vec<u64> = Vec::new();
        for i in 0..3_000_000u64 {
            v.push(i); // grows through small -> big -> bigger reallocations
        }
        assert!(v.iter().enumerate().all(|(i, &x)| x == i as u64));
        assert_eq!(v.as_ptr() as usize % (2 << 20), 0);
        let z = vec![0u32; 5_000_000];
        assert!(z.iter().all(|&x| x == 0));
        let mut s = vec![7u8; 100];
        s.resize(3 << 20, 1);
        assert_eq!(s[99], 7);
        assert_eq!(s[100], 1);
        s.truncate(10);
        s.shrink_to_fit();
        assert_eq!(&s[..], &[7u8; 10]);
    }
}
