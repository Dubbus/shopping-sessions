//! Shopping Sessions pipeline (rules.md). Stages:
//!   1. small tables (manifest, products, campaigns), then links -> person names
//!   2. events + clicks parsed in parallel chunks; events bucketed by event_id,
//!      clicks by person
//!   3. per event_id bucket: dedup (smallest ingest_ts), validate, re-bucket by person
//!   4. per person bucket: sessions, credit, assists, daily aggregates
//!   5. Parquet out
//! Logs go to stderr only.

mod csv;
mod ids;
mod parquet;
mod scan;
mod table;
mod ts;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::time::Instant;

use ids::{Interner, Lookup};
use table::Table;
use parquet::{Column, Data};
use ts::DAY;

const GAP: i64 = 30 * ts::MIN;
const REACH: i64 = 7 * DAY;
const LATE: i64 = DAY;

const VIEW: u8 = 0;
const CART: u8 = 1;
const PURCHASE: u8 = 2;

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    ingest: i64,
    ts: i64,
    person: u64,
    price: u32,
    cat: u16,
    kind: u8,
    ok: bool,
}

#[derive(Clone, Copy)]
struct PEv {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}

struct UserRow {
    person: u64,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: Option<u16>,
}

#[inline]
fn bucket(k: u64, n: usize) -> usize {
    ((k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize * n) >> 32
}

/// `threads` workers, each with its own state from `init`, pull items 0..n off a shared
/// counter. Returns the workers' states.
fn workers<S: Send, I: Fn() -> S + Sync, F: Fn(&mut S, usize) + Sync>(
    n: usize,
    threads: usize,
    init: I,
    f: F,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.max(1))
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Run f(i) for i in 0..n on `threads` threads; results in index order.
fn par<R: Send, F: Fn(usize) -> R + Sync>(n: usize, threads: usize, f: F) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let mut out: Vec<(usize, R)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.min(n).max(1))
            .map(|_| {
                s.spawn(|| {
                    let mut mine = Vec::new();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= n {
                            break;
                        }
                        mine.push((i, f(i)));
                    }
                    mine
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    out.sort_unstable_by_key(|p| p.0);
    out.into_iter().map(|p| p.1).collect()
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Input files read with pread into per-thread buffers. mmap was much slower: first-touch
/// page faults cost more than parsing (measured 3.4 s per 1.6 GB single-threaded on macOS).
struct Files {
    files: Vec<std::fs::File>,
}

thread_local! {
    static BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl Files {
    fn open(paths: &[PathBuf]) -> Self {
        Files { files: paths.iter().map(|p| std::fs::File::open(p).expect("open")).collect() }
    }

    /// Offset just past the first '\n' at or after `from`, or the file length.
    fn after_newline(f: &std::fs::File, from: u64, len: u64) -> u64 {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; 1 << 16];
        let mut at = from;
        while at < len {
            let n = f.read_at(&mut buf, at).expect("read");
            if n == 0 {
                break;
            }
            if let Some(i) = memchr(b'\n', &buf[..n]) {
                return at + i as u64 + 1;
            }
            at += n as u64;
        }
        len
    }

    /// (file, start, end) chunks of whole lines, header removed.
    fn chunks(&self, target: u64) -> Vec<(usize, u64, u64)> {
        let mut out = Vec::new();
        for (i, f) in self.files.iter().enumerate() {
            let len = f.metadata().expect("stat").len();
            let mut start = Self::after_newline(f, 0, len);
            while start < len {
                let end = if start + target >= len { len } else { Self::after_newline(f, start + target, len) };
                out.push((i, start, end));
                start = end;
            }
        }
        out
    }

    /// Read a chunk into this thread's buffer and run `f` on it.
    fn with_chunk<R>(&self, c: (usize, u64, u64), f: impl FnOnce(&[u8]) -> R) -> R {
        use std::os::unix::fs::FileExt;
        BUF.with_borrow_mut(|buf| {
            let n = (c.2 - c.1) as usize;
            if buf.len() < n {
                buf.resize(n, 0);
            }
            self.files[c.0].read_exact_at(&mut buf[..n], c.1).expect("read");
            f(&buf[..n])
        })
    }
}

#[inline]
fn memchr(b: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&c| c == b)
}

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

fn body_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    csv::lines(buf).skip(1)
}

fn json_ts(s: &str, key: &str) -> i64 {
    let at = s.find(&format!("\"{key}\"")).expect("manifest key") + key.len() + 2;
    let rest = &s[at..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    ts::parse(rest[a..b].as_bytes()).expect("manifest ts")
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            // keep checking digits? any non-digit is also invalid, so either way: invalid
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

fn main() {
    let t0 = Instant::now();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads: usize = env("NCPU", "8").parse().unwrap_or(8).max(1);
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());

    // ---- small tables ----
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = json_ts(&manifest, "window_start");
    let we = json_ts(&manifest, "window_end");
    let first_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - first_day + 1) as usize;

    let mut cat_names: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut prod_raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let pbuf = read_small(&data.join("products.csv"));
    for line in body_lines(&pbuf) {
        let mut f = [&b""[..]; 2];
        if csv::fields(line, &mut f) {
            prod_raw.push((f[0].to_vec(), f[1].to_vec()));
            cat_names.push(f[1].to_vec());
        }
    }
    cat_names.sort();
    cat_names.dedup();
    let cat_index: HashMap<&[u8], u16> =
        cat_names.iter().enumerate().map(|(i, c)| (&c[..], i as u16)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let mut products: Lookup<u16> = Lookup::new();
    for (p, c) in &prod_raw {
        products.insert(p, cat_index[&c[..]]);
    }
    products.finish();
    let ncats = cat_names.len();

    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut campaigns: Lookup<u32> = Lookup::new();
    let cbuf = read_small(&data.join("campaigns.csv"));
    for line in body_lines(&cbuf) {
        let mut f = [&b""[..]; 2];
        if csv::fields(line, &mut f) {
            campaigns.insert(f[0], camp_ids.len() as u32);
            camp_ids.push(f[0].to_vec());
            camp_channels.push(f[1].to_vec());
        }
    }
    campaigns.finish();
    let ncamp = camp_ids.len();
    let organic = ncamp; // index of the organic pseudo-campaign

    let ids = Interner::default();

    // Links run as the first task of the parse stage, in parallel with event parsing:
    // events keep raw user IDs until dedup, clicks until after parsing.
    let lk_files = Files::open(&csv_files(&data.join("links")));
    let person_slot: std::sync::OnceLock<Table> = std::sync::OnceLock::new();

    // ---- parse events + clicks ----
    // Events go to many small event_id buckets so each dedup table stays cache-sized;
    // everything person-keyed uses fewer, bigger buckets.
    let nbe = threads * std::env::var("NBE").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let nb = threads * 8;
    let ev_files = Files::open(&csv_files(&data.join("events")));
    let ck_files = Files::open(&csv_files(&data.join("clicks")));
    let ev_chunks = ev_files.chunks(4 << 20);
    let ck_chunks = ck_files.chunks(4 << 20);
    let nev = ev_chunks.len();

    let ev_bytes: u64 = ev_chunks.iter().map(|c| c.2 - c.1).sum();
    let ck_bytes: u64 = ck_chunks.iter().map(|c| c.2 - c.1).sum();
    // Per-worker bucket arrays, presized from the input size so they rarely regrow.
    let ev_cap = (ev_bytes / 70 / (threads * nbe) as u64) as usize * 11 / 10 + 64;
    let ck_cap = (ck_bytes / 40 / (threads * nb) as u64) as usize * 11 / 10 + 64;
    struct ParseState {
        ev: Vec<Vec<Ev>>,
        ck: Vec<Ck>,
    }
    let parsed = workers(
        1 + nev + ck_chunks.len(),
        threads,
        || ParseState {
            ev: (0..nbe).map(|_| Vec::with_capacity(ev_cap)).collect(),
            ck: Vec::with_capacity(ck_cap * nb),
        },
        |st, i| {
            if i == 0 {
                let _ = person_slot.set(build_persons(&lk_files, &ids));
                eprintln!("{:>8.3}s links done", t0.elapsed().as_secs_f64());
                return;
            }
            let i = i - 1;
            if i < nev {
                let bk = &mut st.ev;
                ev_files.with_chunk(ev_chunks[i], |chunk| {
                    scan::for_each::<7, _>(chunk, |f| {
                        if f[0].is_empty() {
                            return;
                        }
                        let eid = ids.key(f[0]);
                        let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
                        let t = ts::parse(f[2]);
                        let kind = match f[4] {
                            b"view" => VIEW,
                            b"cart" => CART,
                            b"purchase" => PURCHASE,
                            _ => u8::MAX,
                        };
                        let mut e = Ev { eid, ingest, ts: 0, person: 0, price: 0, cat: 0, kind, ok: false };
                        if let Some(t) = t
                            && !f[1].is_empty()
                            && t >= ws
                            && t < we
                            && kind != u8::MAX
                            && !f[5].is_empty()
                            && ingest - t <= LATE
                        {
                            let p = if kind == PURCHASE { price(f[6]) } else { Some(0) };
                            if let Some(p) = p {
                                e.ok = true;
                                e.ts = t;
                                e.price = p;
                                e.cat = products.get(f[5]).unwrap_or(unknown_cat);
                                e.person = ids.key(f[1]); // raw user ID; resolved after dedup
                            }
                        }
                        bk[bucket(eid, nbe)].push(e);
                    });
                })
            } else {
                let bk = &mut st.ck;
                ck_files.with_chunk(ck_chunks[i - nev], |chunk| {
                    scan::for_each::<4, _>(chunk, |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let Some(t) = ts::parse(f[3]) else { return };
                        if t < ws - REACH || t >= we {
                            return;
                        }
                        let Some(camp) = campaigns.get(f[2]) else { return };
                        bk.push(Ck { person: ids.key(f[1]), ts: t, cid: ids.key(f[0]), camp }); // raw user ID
                    });
                })
            }
        },
    );
    lap("parse");
    let person_of = person_slot.into_inner().expect("links task ran");
    let person = |uid: u64| -> u64 { person_of.get(uid).unwrap_or(uid) };

    // ---- dedup by event_id (smallest ingest_ts wins), resolve person, re-bucket ----
    let pev_cap = (ev_bytes / 70 / (threads * nb) as u64) as usize * 11 / 10 + 64;
    struct DedupState {
        best: Table,
        dead: Vec<u64>,
        out: Vec<Vec<PEv>>,
        ck: Vec<Vec<Ck>>,
    }
    let deduped = workers(
        threads + nbe,
        threads,
        || DedupState {
            best: Table::with_capacity(8),
            dead: Vec::new(),
            out: (0..nb).map(|_| Vec::with_capacity(pev_cap)).collect(),
            ck: (0..nb).map(|_| Vec::new()).collect(),
        },
        |st, b| {
            if b < threads {
                // one worker's raw clicks: resolve person, bucket by person
                let raw = &parsed[b].ck;
                const AHEAD: usize = 16;
                for i in 0..raw.len() {
                    if i + AHEAD < raw.len() {
                        person_of.prefetch(raw[i + AHEAD].person);
                    }
                    let mut c = raw[i];
                    c.person = person(c.person);
                    st.ck[bucket(c.person, nb)].push(c);
                }
                return;
            }
            let b = b - threads;
            let parts: Vec<&[Ev]> = parsed.iter().map(|p| &p.ev[b][..]).collect();
            let n: usize = parts.iter().map(|p| p.len()).sum();
            let best = &mut st.best;
            best.reset(n);
            // One probe per event: when two copies meet, the loser is marked dead.
            let dead = &mut st.dead;
            dead.clear();
            dead.resize(n.div_ceil(64), 0);
            let mut base = Vec::with_capacity(parts.len());
            let mut acc = 0usize;
            for p in &parts {
                base.push(acc);
                acc += p.len();
            }
            let ev_at = |g: usize| -> &Ev {
                let w = base.partition_point(|&b| b <= g) - 1;
                &parts[w][g - base[w]]
            };
            for (w, part) in parts.iter().enumerate() {
                for (i, e) in part.iter().enumerate() {
                    let g = base[w] + i;
                    let (slot, new) = best.entry(e.eid, g as u64);
                    if !new {
                        let old = *slot as usize;
                        let loser = if ev_at(old).ingest > e.ingest {
                            *slot = g as u64;
                            old
                        } else {
                            g
                        };
                        dead[loser / 64] |= 1 << (loser % 64);
                    }
                }
            }
            const AHEAD: usize = 16;
            for (w, part) in parts.iter().enumerate() {
                for i in 0..part.len() {
                    if i + AHEAD < part.len() && part[i + AHEAD].ok {
                        person_of.prefetch(part[i + AHEAD].person);
                    }
                    let e = &part[i];
                    let g = base[w] + i;
                    if !e.ok || dead[g / 64] >> (g % 64) & 1 == 1 {
                        continue;
                    }
                    let p = person(e.person);
                    st.out[bucket(p, nb)].push(PEv { person: p, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind });
                }
            }
        },
    );
    lap("dedup");

    // ---- sessions, credit, assists per person bucket ----
    let parts: Vec<Part> = par(nb, threads, |b| {
        // Group by person with a counting sort on dense local person numbers, then sort
        // each person's few events by ts. Much cheaper than sorting the bucket by (person, ts).
        let n: usize = deduped.iter().map(|p| p.out[b].len()).sum();
        let mut local = Table::with_capacity(n / 16 + 16);
        let mut keys: Vec<u64> = Vec::with_capacity(n / 16 + 16);
        let mut ev_off: Vec<u32> = Vec::with_capacity(n / 16 + 16);
        let mut pid: Vec<u32> = Vec::with_capacity(n);
        for d in &deduped {
            for e in &d.out[b] {
                let next = keys.len() as u64;
                let (slot, new) = local.entry(e.person, next);
                let q = *slot as usize;
                if new {
                    keys.push(e.person);
                    ev_off.push(0);
                }
                ev_off[q] += 1;
                pid.push(q as u32);
            }
        }
        let m = keys.len();
        let mut acc = 0u32;
        for o in ev_off.iter_mut() {
            let c = *o;
            *o = acc;
            acc += c;
        }
        ev_off.push(acc);
        let mut ev: Vec<PEv> = vec![PEv { person: 0, ts: 0, price: 0, cat: 0, kind: 0 }; n];
        {
            let mut fill: Vec<u32> = ev_off[..m].to_vec();
            let mut k = 0;
            for d in &deduped {
                for e in &d.out[b] {
                    let q = pid[k] as usize;
                    k += 1;
                    ev[fill[q] as usize] = *e;
                    fill[q] += 1;
                }
            }
        }
        for q in 0..m {
            let r = &mut ev[ev_off[q] as usize..ev_off[q + 1] as usize];
            if r.len() > 1 {
                r.sort_unstable_by_key(|e| e.ts);
            }
        }
        // Clicks of persons with events, grouped the same way, each group by (ts, click_id).
        let mut ck_off: Vec<u32> = vec![0; m + 1];
        let mut ck_q: Vec<(u32, Ck)> = Vec::new();
        for p in &deduped {
            for c in &p.ck[b] {
                if let Some(q) = local.get(c.person) {
                    ck_off[q as usize] += 1;
                    ck_q.push((q as u32, *c));
                }
            }
        }
        let mut acc = 0u32;
        for o in ck_off.iter_mut() {
            let c = *o;
            *o = acc;
            acc += c;
        }
        let mut ck: Vec<Ck> = vec![Ck { person: 0, ts: 0, cid: 0, camp: 0 }; ck_q.len()];
        {
            let mut fill: Vec<u32> = ck_off[..m].to_vec();
            for (q, c) in &ck_q {
                ck[fill[*q as usize] as usize] = *c;
                fill[*q as usize] += 1;
            }
        }
        for q in 0..m {
            let r = &mut ck[ck_off[q] as usize..ck_off[q + 1] as usize];
            if r.len() > 1 {
                r.sort_unstable_by(|a, c| a.ts.cmp(&c.ts).then_with(|| ids.cmp(a.cid, c.cid)));
            }
        }

        let mut users = Vec::new();
        let mut camp = vec![[0u64; 4]; (ncamp + 1) * ndays];
        let mut catrev = vec![0u64; ncats];
        let mut starts: Vec<i64> = Vec::new();
        let mut sdays: Vec<usize> = Vec::new();
        let mut run_end: Vec<usize> = Vec::new();
        let mut by_camp: Vec<(u32, i64)> = Vec::new();

        for q in 0..m {
            let p = keys[q];
            let (i, e_end) = (ev_off[q] as usize, ev_off[q + 1] as usize);
            let cl = &ck[ck_off[q] as usize..ck_off[q + 1] as usize];

            starts.clear();
            sdays.clear();
            let mut hi = 0;
            let mut cur: Option<UserRow> = None;
            let mut j = i;
            while j < e_end {
                let start = ev[j].ts;
                let day = start.div_euclid(DAY);
                if cur.as_ref().is_some_and(|u| u.day as i64 != day) {
                    users.push(finish(cur.take().unwrap(), &mut catrev));
                }
                let mut last = start;
                let (mut n, mut rev, mut purch) = (0u64, 0u64, false);
                while j < e_end && ev[j].ts - last <= GAP {
                    let e = ev[j];
                    last = e.ts;
                    n += 1;
                    if e.kind == PURCHASE {
                        rev += e.price as u64;
                        purch = true;
                        catrev[e.cat as usize] += e.price as u64;
                    }
                    j += 1;
                }
                let di = (day - first_day) as usize;
                starts.push(start);
                sdays.push(di);

                let u = cur.get_or_insert(UserRow {
                    person: p,
                    day: day as i32,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    top: None,
                });
                u.sessions += 1;
                u.events += n;
                u.revenue += rev;

                // credit: latest click with ts <= start, within 7 days; tie -> smallest cid
                while hi < cl.len() && cl[hi].ts <= start {
                    hi += 1;
                }
                let mut credited = organic;
                if hi > 0 && start - cl[hi - 1].ts <= REACH {
                    let t = cl[hi - 1].ts;
                    let mut g = hi - 1;
                    while g > 0 && cl[g - 1].ts == t {
                        g -= 1;
                    }
                    credited = cl[g].camp as usize;
                }
                let a = &mut camp[credited * ndays + di];
                a[0] += 1;
                a[1] += purch as u64;
                a[2] += rev;
            }
            if let Some(u) = cur.take() {
                users.push(finish(u, &mut catrev));
            }

            // assists: per campaign, union of [click, click + 7d] intervals over session starts
            if !cl.is_empty() {
                run_end.clear();
                run_end.resize(starts.len(), 0);
                let mut k = starts.len();
                while k > 0 {
                    let mut s = k - 1;
                    while s > 0 && sdays[s - 1] == sdays[k - 1] {
                        s -= 1;
                    }
                    for r in s..k {
                        run_end[r] = k;
                    }
                    k = s;
                }
                by_camp.clear();
                by_camp.extend(cl.iter().map(|x| (x.camp, x.ts)));
                by_camp.sort_unstable();
                let mut g = 0;
                while g < by_camp.len() {
                    let cm = by_camp[g].0;
                    let mut lo = by_camp[g].1;
                    let mut hi_t = lo + REACH;
                    g += 1;
                    loop {
                        let more = g < by_camp.len() && by_camp[g].0 == cm;
                        if more && by_camp[g].1 <= hi_t {
                            hi_t = by_camp[g].1 + REACH;
                            g += 1;
                            continue;
                        }
                        // sessions with lo <= start <= hi_t
                        let a = starts.partition_point(|&s| s < lo);
                        let z = starts.partition_point(|&s| s <= hi_t);
                        let mut q = a;
                        while q < z {
                            let e = run_end[q].min(z);
                            camp[cm as usize * ndays + sdays[q]][3] += (e - q) as u64;
                            q = e;
                        }
                        if more {
                            lo = by_camp[g].1;
                            hi_t = lo + REACH;
                            g += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Part { users, camp }
    });
    lap("sessions");

    write_outputs(&out, &parts, &ids, &cat_names, &camp_ids, &camp_channels, first_day, ndays)
        .expect("write outputs");
    lap("done");
    // Skip freeing ~GBs of buffers and unmapping: the OS reclaims it all at exit.
    std::process::exit(0);
}

/// Union-find over the link files; the table maps an ID to its person's name (the
/// byte-wise smallest ID) for every ID not named by itself.
fn build_persons(lk_files: &Files, ids: &Interner) -> Table {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    for c in lk_files.chunks(1 << 24) {
        lk_files.with_chunk(c, |chunk| {
            scan::for_each::<2, _>(chunk, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    pairs.push((ids.key(f[0]), ids.key(f[1])));
                }
            });
        });
    }
    let npairs = pairs.len();
    let mut node_of = Table::with_capacity(npairs + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs + 16);
    let mut node_key: Vec<u64> = Vec::with_capacity(npairs + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for &(ka, kb) in &pairs {
        let mut node = |k: u64| -> u32 {
            let next = parent.len() as u64;
            let (v, new) = node_of.entry(k, next);
            let v = *v as u32;
            if new {
                parent.push(v);
                node_key.push(k);
            }
            v
        };
        let a = node(ka);
        let b = node(kb);
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let nnodes = parent.len();
    let mut root_name: Vec<u64> = vec![0; nnodes];
    for n in 0..nnodes {
        let r = find(&mut parent, n as u32) as usize;
        let k = node_key[n];
        if root_name[r] == 0 || ids.cmp(k, root_name[r]).is_lt() {
            root_name[r] = k;
        }
    }
    // Only IDs whose person is named by another ID need an entry; the rest map to themselves.
    let mut person_of = Table::with_capacity(nnodes / 2 + 16);
    for n in 0..nnodes {
        let name = root_name[find(&mut parent, n as u32) as usize];
        if name != node_key[n] {
            person_of.entry(node_key[n], name);
        }
    }
    person_of
}

/// Close a user-day: top category = highest summed price, tie -> smallest (sorted) name.
fn finish(mut u: UserRow, catrev: &mut [u64]) -> UserRow {
    let mut best: Option<(u64, u16)> = None;
    for (c, v) in catrev.iter_mut().enumerate() {
        if *v > 0 && best.is_none_or(|(bv, _)| *v > bv) {
            best = Some((*v, c as u16));
        }
        *v = 0;
    }
    u.top = best.map(|b| b.1);
    u
}

struct Part {
    users: Vec<UserRow>,
    camp: Vec<[u64; 4]>,
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(
    out: &Path,
    parts: &[Part],
    ids: &Interner,
    cats: &[Vec<u8>],
    camp_ids: &[Vec<u8>],
    channels: &[Vec<u8>],
    first_day: i64,
    ndays: usize,
) -> std::io::Result<()> {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;

    let mut uid = Data::str();
    let mut top = Data::opt_str();
    let (mut day, mut ses, mut evs, mut rev) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut buf = Vec::new();
    for u in parts.iter().flat_map(|p| p.users.iter()) {
        buf.clear();
        ids.bytes(u.person, &mut buf);
        uid.push_str(&buf);
        day.push(u.day);
        ses.push(u.sessions as i64);
        evs.push(u.events as i64);
        rev.push(u.revenue as i64);
        match u.top {
            Some(c) => top.push_str(&cats[c as usize]),
            None => top.push_null(),
        }
    }
    parquet::write(
        &ud.join("part-0.parquet"),
        &[
            Column { name: "user_id", data: uid },
            Column { name: "day", data: Data::Date(day) },
            Column { name: "sessions", data: Data::I64(ses) },
            Column { name: "events", data: Data::I64(evs) },
            Column { name: "revenue_cents", data: Data::I64(rev) },
            Column { name: "top_category", data: top },
        ],
    )?;

    let ncamp = camp_ids.len();
    let mut tot = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for p in parts {
        for (t, a) in tot.iter_mut().zip(&p.camp) {
            for k in 0..4 {
                t[k] += a[k];
            }
        }
    }
    let mut cid = Data::str();
    let mut chan = Data::str();
    let (mut day, mut s, mut ps, mut r, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = tot[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == ncamp {
                cid.push_str(b"organic");
                chan.push_str(b"organic");
            } else {
                cid.push_str(&camp_ids[c]);
                chan.push_str(&channels[c]);
            }
            day.push((first_day + d as i64) as i32);
            s.push(a[0] as i64);
            ps.push(a[1] as i64);
            r.push(a[2] as i64);
            asst.push(a[3] as i64);
        }
    }
    parquet::write(
        &cd.join("part-0.parquet"),
        &[
            Column { name: "campaign_id", data: cid },
            Column { name: "channel", data: chan },
            Column { name: "day", data: Data::Date(day) },
            Column { name: "sessions", data: Data::I64(s) },
            Column { name: "purchasing_sessions", data: Data::I64(ps) },
            Column { name: "revenue_cents", data: Data::I64(r) },
            Column { name: "assisted_sessions", data: Data::I64(asst) },
        ],
    )
}
