//! Minimal parallel loops over item indexes: workers pull from a shared counter.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Each worker has its own state; the states come back in thread order.
pub fn run<S: Send>(
    threads: usize,
    items: usize,
    make_state: impl Fn(usize) -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let threads = threads.max(1);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let (next, make_state, work) = (&next, &make_state, &work);
                scope.spawn(move || {
                    let mut state = make_state(t);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= items {
                            break;
                        }
                        work(&mut state, i);
                    }
                    state
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    })
}

/// One result per item, in item order.
pub fn map<R: Send>(threads: usize, items: usize, work: impl Fn(usize) -> R + Sync) -> Vec<R> {
    let mut tagged: Vec<(usize, R)> =
        run(threads, items, |_| Vec::new(), |out: &mut Vec<(usize, R)>, i| out.push((i, work(i)))).into_iter().flatten().collect();
    tagged.sort_unstable_by_key(|r| r.0);
    tagged.into_iter().map(|r| r.1).collect()
}

/// A raw pointer that may cross threads, for writes to provably disjoint slots.
#[derive(Clone, Copy)]
pub struct Shared<T>(pub *mut T);
unsafe impl<T> Send for Shared<T> {}
unsafe impl<T> Sync for Shared<T> {}
