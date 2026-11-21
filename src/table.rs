//! Flat open-addressing u64 -> u64 map, linear probing, key 0 = empty (no real key is
//! 0: IDs are non-empty, so their packed or fallback keys are never 0). One 16-byte
//! slot per probe, so a lookup is usually one cache line, and `prefetch` lets a loop
//! hide that miss.

pub struct Table {
    slots: Vec<(u64, u64)>,
    shift: u32,
    len: usize,
}

#[inline(always)]
fn hash(k: u64) -> u64 {
    let h = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}

impl Table {
    /// Sized for exactly `n` keys at <= ~80% load (a lookup-only table that never grows).
    pub fn dense(n: usize) -> Self {
        let cap = (n.max(8) * 5 / 4).next_power_of_two();
        Table { slots: vec![(0, 0); cap], shift: 64 - cap.trailing_zeros(), len: 0 }
    }

    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        Table { slots: vec![(0, 0); cap], shift: 64 - cap.trailing_zeros(), len: 0 }
    }

    /// Empty the table, sized for `n` keys, reusing its memory.
    pub fn reset(&mut self, n: usize) {
        // ~60% max load: small enough to stay in L2 for a dedup bucket, probes still short.
        let cap = (n.max(8) * 5 / 3).next_power_of_two();
        if self.slots.len() == cap {
            self.slots.fill((0, 0));
        } else {
            self.slots = vec![(0, 0); cap];
        }
        self.shift = 64 - cap.trailing_zeros();
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn cap(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (hash(k).wrapping_mul(0xD6E8_FEB8_6659_FD93) >> self.shift) as usize
    }

    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let p = self.slots.as_ptr().add(self.slot(k));
            std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = k;
    }

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mask = self.slots.len() - 1;
        let mut i = self.slot(k);
        loop {
            let (sk, sv) = unsafe { *self.slots.get_unchecked(i) };
            if sk == k {
                return Some(sv);
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & mask;
        }
    }

    /// Insert or return the existing value's slot index for in-place update.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn entry(&mut self, k: u64, v: u64) -> (&mut u64, bool) {
        if self.len * 5 >= self.slots.len() * 4 {
            self.grow();
        }
        let mask = self.slots.len() - 1;
        let mut i = self.slot(k);
        loop {
            let sk = self.slots[i].0;
            if sk == k {
                return (&mut self.slots[i].1, false);
            }
            if sk == 0 {
                self.slots[i] = (k, v);
                self.len += 1;
                return (&mut self.slots[i].1, true);
            }
            i = (i + 1) & mask;
        }
    }

    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        *self = Table::with_capacity(old.len());
        for (k, v) in old {
            if k != 0 {
                self.entry(k, v);
            }
        }
    }
}
