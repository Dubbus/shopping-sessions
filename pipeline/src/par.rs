//! A minimal parallel-for: `threads` workers pull item indexes from a shared
//! counter, each with its own state, and the states come back in thread order.

use std::sync::atomic::{AtomicUsize, Ordering};

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
