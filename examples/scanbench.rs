//! Pure scanner throughput on one in-memory chunk (no I/O, no allocation).
//!   cargo run --release --example scanbench -- <csv file>
#[path = "../src/scan.rs"]
mod scan;
fn main() {
    let path = std::env::args().nth(1).expect("csv path");
    let data = std::fs::read(&path).unwrap();
    let chunk = &data[..data.len().min(512 << 10)];
    let reps = 2000;
    let t = std::time::Instant::now();
    let mut n = 0u64;
    for _ in 0..reps {
        scan::for_each::<7, _>(std::hint::black_box(chunk), |f| n += f[0].len() as u64);
    }
    let s = t.elapsed().as_secs_f64();
    println!("scan: {:.2} GB/s ({n})", (chunk.len() * reps) as f64 / s / 1e9);
    let t = std::time::Instant::now();
    let mut x = 0u64;
    for r in 0..reps {
        for w in chunk.chunks_exact(8) {
            x = x.wrapping_mul(31).wrapping_add(u64::from_le_bytes(w.try_into().unwrap()) ^ r as u64);
        }
    }
    let s = t.elapsed().as_secs_f64();
    println!("scalar loop: {:.2} GB/s ({x})", (chunk.len() * reps) as f64 / s / 1e9);
}
