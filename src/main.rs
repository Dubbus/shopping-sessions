//! Shopping Sessions pipeline. Stages mirror rules.md:
//!   1. read + clean CSVs (parallel, chunked on newline boundaries)
//!   2. intern string IDs -> u32
//!   3. link IDs into persons (union-find)
//!   4. sort events by (person, ts), dedup
//!   5. sessionize, attribute, count assists (one pass per person, parallel by person shard)
//!   6. aggregate daily rows, write Parquet
//!
//! Log to stderr only (stdout is capped at 50 MB).

use std::env;
use std::path::PathBuf;
use std::time::Instant;

struct Cfg {
    data: PathBuf,
    out: PathBuf,
    #[allow(dead_code)]
    scratch: PathBuf,
    ncpu: usize,
}

fn cfg() -> Cfg {
    let get = |k: &str, d: &str| env::var(k).unwrap_or_else(|_| d.to_string());
    Cfg {
        data: get("DATA_DIR", "/data").into(),
        out: get("OUTPUT_DIR", "/output").into(),
        scratch: get("SCRATCH_DIR", "/scratch").into(),
        ncpu: get("NCPU", "8").parse().unwrap_or(8),
    }
}

/// Split `buf` into ~n chunks that each end on a '\n'. Only valid if the CSV
/// dialect forbids newlines inside quoted fields; check rules.md before relying on it.
#[allow(dead_code)]
fn split_lines(buf: &[u8], n: usize) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(n);
    let step = buf.len() / n.max(1) + 1;
    let mut start = 0;
    while start < buf.len() {
        let mut end = (start + step).min(buf.len());
        while end < buf.len() && buf[end - 1] != b'\n' {
            end += 1;
        }
        out.push(&buf[start..end]);
        start = end;
    }
    out
}

fn main() {
    let t0 = Instant::now();
    let c = cfg();
    eprintln!("data={:?} out={:?} ncpu={}", c.data, c.out, c.ncpu);

    std::fs::create_dir_all(c.out.join("user_daily")).expect("mkdir user_daily");
    std::fs::create_dir_all(c.out.join("campaign_daily")).expect("mkdir campaign_daily");

    // TODO: stages 1-6 once rules.md is in spec/.

    eprintln!("done in {:.3}s", t0.elapsed().as_secs_f64());
}
