//! Shopping Sessions pipeline. See docs/rules.md for the rules and
//! docs/PLAN.md for the design.
//!
//! Every stage is parallel, and every lookup happens inside a partition small
//! enough to stay in the CPU cache: records are passed from stage to stage by
//! scattering them on the hash of whatever the next stage looks up.
//!
//!   1. parse     chunks of every CSV into records. One thread turns links/ into the
//!                table of person names (union-find) while the others parse events/,
//!                scattered by event_id; clicks/ come last, scattered by person
//!   2. dedup     per event_id partition; survivors get their person and are
//!                scattered by it
//!   3. sessions  per person partition: sort, sessions, click credit, aggregation
//!   4. write     Parquet, one user_daily file per thread

mod csv;
mod io;
mod key;
mod link;
mod output;
mod par;
mod scan;
mod scatter;
mod table;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use hashbrown::HashTable;
use key::{Id, partition, remix};
use scatter::{Scatter, Scattered};
use table::{Campaigns, Str, StrMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const GAP_MS: u32 = 30 * 60 * 1000;
const WEEK_MS: i64 = 7 * DAY_MS;
/// Seconds from 0000-01-01 to the epoch: makes any F1 time non-negative.
const YEAR_ZERO: i64 = 62_167_219_200;
const VALID: u32 = 1 << 31;

/// One parsed event line, before dedup. 32 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    id: Id,
    /// The user ID as written. Meaningful only when valid, like `ts`.
    user: Id,
    /// Ingest time in seconds since year 0 (high 40 bits), price in cents (low 24; 0 unless a purchase).
    ingest_price: u64,
    /// Milliseconds since `Ctx::base`.
    ts: u32,
    /// Category, plus `VALID` if the event passes validation and is not late.
    category: u32,
}

/// A surviving event on its way to its person's partition. 16 bytes, four to a cache line.
#[derive(Clone, Copy)]
#[repr(C)]
struct Survivor {
    who: Id,
    ts: u32,
    /// 0, or which purchase this is: see [`purchase_ref`].
    purchase: u32,
}

/// Purchases are about 3% of events, so their price and category travel apart from the
/// survivors, in one list per dedup thread. A reference names the thread in its top
/// `PURCHASE_THREAD_BITS` bits and one more than the list index in the rest; 0 is "none".
const PURCHASE_THREAD_BITS: u32 = 6;

#[inline]
fn purchase_ref(thread: usize, index: usize) -> u32 {
    assert!(index + 1 < 1 << (32 - PURCHASE_THREAD_BITS), "more purchases than a reference can number");
    (thread as u32) << (32 - PURCHASE_THREAD_BITS) | (index as u32 + 1)
}

#[inline]
fn purchase_of(purchases: &[Vec<(u32, u32)>], reference: u32) -> (u32, u32) {
    let index = (reference & ((1 << (32 - PURCHASE_THREAD_BITS)) - 1)) as usize - 1;
    purchases[(reference >> (32 - PURCHASE_THREAD_BITS)) as usize][index]
}

/// A valid click on its way to its person's partition. 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    who: Id,
    id: Id,
    ts: u32,
    campaign: u32,
}

// Inside a person partition, events and clicks are single integers, so that putting them
// in order is a plain integer sort.
//
// An event is `ts << 32 | purchase`, where `purchase` is 0 or a reference to its price and
// category (see `purchase_ref`).
//
// A click is `ts << 96 | !id << 32 | campaign`: ascending order is by time, then by
// click_id descending, which is the order the credit rule wants (see the sessions stage).

#[inline]
fn click_key(ts: u32, id: Id, campaign: u32) -> u128 {
    (ts as u128) << 96 | (!id as u128) << 32 | campaign as u128
}

#[inline]
fn click_ts(key: u128) -> u32 {
    (key >> 96) as u32
}

#[inline]
fn click_id(key: u128) -> Id {
    !((key >> 32) as u64)
}

/// Everything the parse stage reads but never changes.
struct Ctx {
    window_start: i64,
    window_end: i64,
    /// `window_start` minus 7 days: the zero of every stored timestamp.
    base: i64,
    products: StrMap,
    unknown_category: u32,
    campaigns: Campaigns,
    /// Set by the link task; the click parsers wait for it.
    renames: OnceLock<link::Renames>,
    event_bits: u32,
    person_bits: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Events,
    Clicks,
    Links,
}

/// How many clicks wait for their rename-table slot to arrive in cache.
const BATCH: usize = 32;

struct Parsed {
    /// By partition of the event ID.
    events: Scatter<Event>,
    /// By partition of the person.
    clicks: Scatter<Click>,
    /// Parsed clicks whose user ID is not yet replaced by the person's name.
    waiting_clicks: Vec<Click>,
    links: Vec<(Id, Id)>,
    buf: Vec<u8>,
    index: scan::Index,
    dates: ts::Dates,
}

impl Parsed {
    /// "Links come before everything else": the user ID becomes the person.
    #[inline]
    fn settle_clicks(&mut self, renames: &link::Renames, person_bits: u32) {
        for click in &self.waiting_clicks {
            renames.prefetch_slot(click.who);
        }
        for mut click in self.waiting_clicks.drain(..) {
            click.who = renames.person(click.who);
            self.clicks.push(partition(key::hash(click.who), person_bits), click);
        }
    }
}

/// A purchase price: ASCII digits only, 1 to 10,000,000, leading zeros allowed.
#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    if b.len() - i > 8 {
        return None;
    }
    let mut v = 0u32;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
    }
    if (1..=10_000_000).contains(&v) { Some(v) } else { None }
}

// The three line parsers take lines from a chunk buffer, which keeps `io::SLACK`
// readable bytes after its data; that is what makes the `Key` constructors sound.

#[inline]
fn parse_event(&[id, user, ts, ingest, kind, product, price]: &[&[u8]; 7], ctx: &Ctx, out: &mut Parsed) {
    // Events step 1: malformed lines never get here, and empty event IDs are gone before dedup.
    if id.is_empty() {
        return;
    }
    // The rules promise a valid F1 ingest_ts; a copy without one cannot be ordered, so it is dropped.
    let Some(ingest) = out.dates.parse_f1_secs(ingest) else { return };
    let mut event = Event {
        id: unsafe { key::id(id) },
        user: 0,
        ingest_price: ((ingest + YEAR_ZERO) as u64) << 24,
        ts: 0,
        category: 0,
    };
    // Steps 3 and 4, decided now but applied only to the copy that survives dedup.
    'valid: {
        if user.is_empty() || product.is_empty() {
            break 'valid;
        }
        let Some(t) = out.dates.parse(ts) else { break 'valid };
        if t < ctx.window_start || t >= ctx.window_end || ingest * 1000 - t > DAY_MS {
            break 'valid;
        }
        // The event type, compared as an integer: `view`, `cart` or `purchase`, exactly.
        let cents = if kind.len() == 4 {
            let word = u32::from_le_bytes(kind.try_into().unwrap());
            if word != u32::from_le_bytes(*b"view") && word != u32::from_le_bytes(*b"cart") {
                break 'valid;
            }
            0
        } else if kind.len() == 8 && u64::from_le_bytes(kind.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
            let Some(cents) = parse_price(price) else { break 'valid };
            event.category = ctx.products.get(product).unwrap_or(ctx.unknown_category);
            cents
        } else {
            break 'valid;
        };
        event.ts = (t - ctx.base) as u32;
        event.user = unsafe { key::id(user) };
        event.ingest_price |= cents as u64;
        event.category |= VALID;
    }
    // An invalid copy travels on too: it can be the one dedup keeps.
    out.events.push(partition(key::hash(event.id), ctx.event_bits), event);
}

#[inline]
fn parse_click(&[id, user, campaign, ts]: &[&[u8]; 4], ctx: &Ctx, renames: &link::Renames, out: &mut Parsed) {
    // Clicks steps 1 and 3. Copies of a click are identical, so validating before dedup is the same.
    if id.is_empty() || user.is_empty() {
        return;
    }
    let Some(campaign) = ctx.campaigns.get(campaign) else { return };
    let Some(t) = out.dates.parse(ts) else { return };
    if t < ctx.base || t >= ctx.window_end {
        return;
    }
    let who = unsafe { key::id(user) };
    renames.prefetch_tag(who);
    out.waiting_clicks.push(Click { who, id: unsafe { key::id(id) }, ts: (t - ctx.base) as u32, campaign });
    if out.waiting_clicks.len() == BATCH {
        out.settle_clicks(renames, ctx.person_bits);
    }
}

#[inline]
fn parse_link(&[a, b]: &[&[u8]; 2], out: &mut Vec<(Id, Id)>) {
    // Links step 1: malformed lines never get here; a line with an empty side is dropped.
    if a.is_empty() || b.is_empty() {
        return;
    }
    out.push(unsafe { (key::id(a), key::id(b)) });
}

/// A whole small file, kept for the run because names point into it.
fn read_static(path: &Path) -> Res<Str> {
    Ok(Box::leak(std::fs::read(path)?.into_boxed_slice()))
}

/// Every `*.csv` file directly in `dir`.
fn csv_files(dir: &Path) -> Res<Vec<PathBuf>> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else { return Ok(files) };
    for entry in entries {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".csv") && !name.starts_with('.') && path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Reads a two-column lookup file (products or campaigns) into (key, value) pairs.
fn pairs(data: Str) -> Vec<(Str, Str)> {
    let mut rows = Vec::new();
    csv::for_each_line(data, 0, data.len(), |line| {
        if let Some([key, value]) = csv::split::<2>(line) {
            if !key.is_empty() {
                rows.push((key, value));
            }
        }
    });
    rows
}

/// The string values of the top-level keys of a JSON object. Nested values and
/// keys that merely appear inside other strings are skipped, not matched.
fn json_top_level_strings(text: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    // Reads the string starting at the quote at `i`; returns its raw bytes and the index after it.
    fn string(text: &[u8], mut i: usize) -> Option<(Vec<u8>, usize)> {
        let mut out = Vec::new();
        i += 1;
        while i < text.len() {
            match text[i] {
                b'"' => return Some((out, i + 1)),
                b'\\' if i + 1 < text.len() => {
                    out.push(text[i + 1]);
                    i += 2;
                }
                c => {
                    out.push(c);
                    i += 1;
                }
            }
        }
        None
    }
    let mut found = Vec::new();
    let mut i = 0;
    let space = |i: &mut usize| {
        while *i < text.len() && text[*i].is_ascii_whitespace() {
            *i += 1;
        }
    };
    space(&mut i);
    if i >= text.len() || text[i] != b'{' {
        return found;
    }
    i += 1;
    loop {
        space(&mut i);
        if i >= text.len() || text[i] != b'"' {
            return found;
        }
        let Some((key, after)) = string(text, i) else { return found };
        i = after;
        space(&mut i);
        if i >= text.len() || text[i] != b':' {
            return found;
        }
        i += 1;
        space(&mut i);
        if i < text.len() && text[i] == b'"' {
            let Some((value, after)) = string(text, i) else { return found };
            found.push((key, value));
            i = after;
        } else {
            // Any other value: skip to the comma or brace that ends it at this depth.
            let mut depth = 0usize;
            while i < text.len() {
                match text[i] {
                    b'"' => match string(text, i) {
                        Some((_, after)) => {
                            i = after;
                            continue;
                        }
                        None => return found,
                    },
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' if depth > 0 => depth -= 1,
                    b',' | b'}' if depth == 0 => break,
                    _ => {}
                }
                i += 1;
            }
        }
        space(&mut i);
        if i >= text.len() || text[i] != b',' {
            return found;
        }
        i += 1;
    }
}

/// The timestamp stored under `key` in manifest.json.
fn manifest_time(text: &[u8], key: &str) -> Res<i64> {
    let values = json_top_level_strings(text);
    let value = values.iter().find(|(k, _)| k == key.as_bytes()).ok_or("manifest: key missing")?;
    ts::parse(&value.1).ok_or_else(|| "manifest: bad timestamp".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_reader() {
        let t = |text: &str, key: &str| manifest_time(text.as_bytes(), key).ok();
        let plain = r#"{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}"#;
        assert_eq!(t(plain, "window_start"), Some(1_790_812_800_000));
        assert_eq!(t(plain, "window_end"), Some(1_792_022_400_000));
        let tricky = r#" {"note": "window_start", "sizes": {"events": 4, "window_end": 3, "x": ["}", {"window_start": "1"}]},
            "n": -1.5e3, "b": true, "z": null, "esc": "a\"b", "window_end":"2026-10-15T00:00:00Z" ,
            "window_start"
            : "2026-10-01T00:00:00Z"} "#;
        assert_eq!(t(tricky, "window_start"), Some(1_790_812_800_000));
        assert_eq!(t(tricky, "window_end"), Some(1_792_022_400_000));
        assert_eq!(t(plain, "window"), None);
        assert_eq!(t("", "window_start"), None);
        assert_eq!(t(r#"{"window_start": 5}"#, "window_start"), None);
    }
}

fn bits_for(count: usize, per_partition: usize, max_bits: u32) -> u32 {
    let wanted = count.div_ceil(per_partition).max(1).next_power_of_two();
    wanted.trailing_zeros().min(max_bits)
}

/// Per-thread state of the sessions stage.
struct Sessions {
    rows: output::UserRows,
    /// Per (campaign, day): sessions, purchasing sessions, revenue.
    campaign_days: Vec<[u64; 3]>,
    /// Per (campaign, day): assisted sessions. Apart and small, because a session touches
    /// one cell here for every campaign in its window.
    assisted: Vec<u32>,
    /// Person name -> index into `names`, for this partition.
    people: HashTable<(Id, u32)>,
    names: Vec<Id>,
    /// The person of each record, in the order the records are read.
    owner: Vec<u32>,
    /// Where each person's events (and clicks) start in the grouped arrays; one extra entry at the end.
    event_start: Vec<u32>,
    click_start: Vec<u32>,
    events: Vec<u64>,
    clicks: Vec<u128>,
    /// Clicks per campaign inside the current 7-day window, and the campaigns present.
    in_window: Vec<u32>,
    slot: Vec<u32>,
    active: Vec<u32>,
    /// Purchase cents per category for the current person and day.
    day_categories: Vec<(u32, u64)>,
}

/// Turns per-person counts (in `start[1..]`) into start offsets, in place.
fn offsets(start: &mut [u32]) {
    let mut total = 0;
    for slot in start.iter_mut() {
        let count = *slot;
        *slot = total;
        total += count;
    }
}

fn main() -> Res<()> {
    let started = Instant::now();
    let mut lap = Instant::now();
    let mut log = |stage: &str| {
        eprintln!("{stage:<10} {:>8.1} ms", lap.elapsed().as_secs_f64() * 1e3);
        lap = Instant::now();
    };

    let args: Vec<String> = std::env::args().collect();
    let dir = |position: usize, var: &str, default: &str| -> PathBuf {
        if args.len() == 3 {
            return PathBuf::from(&args[position]);
        }
        PathBuf::from(std::env::var(var).unwrap_or_else(|_| default.to_string()))
    };
    let data_dir = dir(1, "DATA_DIR", "/data");
    let out_dir = dir(2, "OUTPUT_DIR", "/output");
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));

    // ---- Inputs --------------------------------------------------------------------------
    let manifest = std::fs::read(data_dir.join("manifest.json"))?;
    let window_start = manifest_time(&manifest, "window_start")?;
    let window_end = manifest_time(&manifest, "window_end")?;
    let base = window_start - WEEK_MS;
    if window_end - base > u32::MAX as i64 {
        return Err("window longer than this build stores (about 42 days)".into());
    }

    // Categories and campaigns become small integers. `unknown` is a category like any other.
    let mut category_ids = StrMap::default();
    let mut category_names: Vec<Str> = Vec::new();
    let mut category_id = |name: Str| -> u32 {
        let id = category_ids.get_or_insert(name, category_names.len() as u32);
        if id as usize == category_names.len() {
            category_names.push(name);
        }
        id
    };
    let mut products = StrMap::default();
    for (product, category) in pairs(read_static(&data_dir.join("products.csv"))?) {
        let category = category_id(category);
        products.get_or_insert(product, category);
    }
    let unknown_category = category_id(b"unknown");

    let mut campaigns = Campaigns::default();
    let mut campaign_names: Vec<Str> = Vec::new();
    let mut channels: Vec<Str> = Vec::new();
    for (campaign, channel) in pairs(read_static(&data_dir.join("campaigns.csv"))?) {
        let id = campaigns.get_or_insert(campaign, campaign_names.len() as u32);
        if id as usize == campaign_names.len() {
            campaign_names.push(campaign);
            channels.push(channel);
        }
    }
    let organic = campaign_names.len();
    campaign_names.push(b"organic");
    channels.push(b"organic");

    // Tuning knobs, for benchmarking only: unset, each takes the default chosen below.
    let knob = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok());
    let chunk = knob("SS_CHUNK_KB").unwrap_or(512) << 10;

    let mut files: Vec<(File, u64)> = Vec::new();
    let mut chunks: Vec<(Kind, usize, u64, u64)> = Vec::new();
    let mut event_bytes = 0u64;
    for (kind, folder) in [(Kind::Events, "events"), (Kind::Clicks, "clicks"), (Kind::Links, "links")] {
        for path in csv_files(&data_dir.join(folder))? {
            let file = File::open(&path)?;
            let len = file.metadata()?.len();
            if matches!(kind, Kind::Events) {
                event_bytes += len;
            }
            let mut start = 0;
            while start < len {
                let end = (start + chunk).min(len);
                chunks.push((kind, files.len(), start, end));
                start = end;
            }
            files.push((file, len));
        }
    }
    let rough_events = (event_bytes / 64) as usize;
    let event_bits = knob("SS_EVENT_BITS").map_or(bits_for(rough_events, 1 << 15, 12), |b| b as u32);
    let person_bits = knob("SS_PERSON_BITS").map_or(bits_for(rough_events, 1 << 16, 11), |b| b as u32);
    log("open");

    // ---- 1. Parse ------------------------------------------------------------------------
    // Work items, in order: the link chunks, the link task, the event chunks, the click chunks.
    // Events keep their user IDs until dedup, so they do not wait for the links; clicks do.
    chunks.sort_by_key(|c| match c.0 {
        Kind::Links => 0,
        Kind::Events => 1,
        Kind::Clicks => 2,
    });
    let link_chunks = chunks.iter().filter(|c| c.0 == Kind::Links).count();
    let links_read = AtomicUsize::new(0);
    let all_links: Mutex<Vec<(Id, Id)>> = Mutex::new(Vec::new());
    let ctx = Ctx { window_start, window_end, base, products, unknown_category, campaigns, renames: OnceLock::new(), event_bits, person_bits };
    let parsed = par::run(
        threads,
        chunks.len() + 1,
        |_| Parsed {
            events: Scatter::new(1 << event_bits),
            clicks: Scatter::new(1 << person_bits),
            waiting_clicks: Vec::with_capacity(BATCH),
            links: Vec::new(),
            buf: Vec::new(),
            index: scan::Index::default(),
            dates: ts::Dates::new(),
        },
        |out, item| {
            if item == link_chunks {
                // The link task: wait for the link chunks still being read, then build the table.
                while links_read.load(Ordering::Acquire) < link_chunks {
                    std::thread::yield_now();
                }
                let links = std::mem::take(&mut *all_links.lock().unwrap());
                let _ = ctx.renames.set(link::resolve(&links));
                return;
            }
            let (kind, file, start, end) = chunks[if item < link_chunks { item } else { item - 1 }];
            let mut buf = std::mem::take(&mut out.buf);
            let (len, first) = io::load_chunk(&files[file].0, files[file].1, start, end, &mut buf).expect("read failed");
            let last = first + (end - start) as usize;
            let mut index = std::mem::take(&mut out.index);
            index.build(&buf, len);
            match kind {
                Kind::Links => {
                    let links = &mut out.links;
                    index.for_each_record(&buf, first, last, |fields| parse_link(fields, links));
                    all_links.lock().unwrap().append(links);
                    links_read.fetch_add(1, Ordering::Release);
                }
                Kind::Events => index.for_each_record(&buf, first, last, |fields| parse_event(fields, &ctx, out)),
                Kind::Clicks => {
                    let renames = loop {
                        match ctx.renames.get() {
                            Some(renames) => break renames,
                            None => std::thread::yield_now(),
                        }
                    };
                    index.for_each_record(&buf, first, last, |fields| parse_click(fields, &ctx, renames, out));
                    out.settle_clicks(renames, person_bits);
                }
            }
            out.buf = buf;
            out.index = index;
        },
    );
    drop(files);
    let renames = ctx.renames.get().expect("the link task ran");
    let (events, clicks): (Vec<Scattered<Event>>, Vec<Scattered<Click>>) =
        parsed.into_iter().map(|thread| (thread.events.finish(), thread.clicks.finish())).unzip();
    log("parse");

    // ---- 2. Dedup events; scatter survivors by person ---------------------------------------
    /// How many survivors wait for their rename-table slot to arrive in cache.
    const AHEAD: usize = 16;
    struct Dedup {
        /// Two bitmaps over a hash of the event ID: seen once, seen again.
        seen: Vec<u64>,
        again: Vec<u64>,
        /// The copy to keep of each event ID seen again, with the ID first.
        kept: HashTable<Event>,
        /// Valid winners waiting for their rename-table slot, oldest first from `next`.
        waiting: [Survivor; AHEAD],
        next: usize,
        out: Scatter<Survivor>,
        /// This thread's number, and the (price, category) of the purchases it kept.
        thread: usize,
        purchases: Vec<(u32, u32)>,
    }
    impl Dedup {
        /// A valid event that survived dedup: once its rename-table slot has had time to
        /// arrive, it gets its person and goes to that person's partition.
        #[inline]
        fn keep(&mut self, event: &Event, renames: &link::Renames, person_bits: u32) {
            renames.prefetch_tag(event.user);
            let slot = self.next % AHEAD;
            if self.next >= AHEAD {
                self.settle(slot, renames, person_bits);
                // Halfway through its wait, a survivor's tag is here: fetch its slot if needed.
                renames.prefetch_slot(self.waiting[(slot + AHEAD / 2) % AHEAD].who);
            }
            let price = (event.ingest_price & 0xFF_FFFF) as u32;
            let purchase = if price != 0 {
                self.purchases.push((price, event.category & !VALID));
                purchase_ref(self.thread, self.purchases.len() - 1)
            } else {
                0
            };
            self.waiting[slot] = Survivor { who: event.user, ts: event.ts, purchase };
            self.next += 1;
        }

        #[inline]
        fn settle(&mut self, slot: usize, renames: &link::Renames, person_bits: u32) {
            let mut survivor = self.waiting[slot];
            survivor.who = renames.person(survivor.who);
            self.out.push(partition(key::hash(survivor.who), person_bits), survivor);
        }

        fn settle_all(&mut self, renames: &link::Renames, person_bits: u32) {
            for slot in 0..self.next.min(AHEAD) {
                self.settle(slot, renames, person_bits);
            }
            self.next = 0;
        }
    }
    assert!(threads <= 1 << PURCHASE_THREAD_BITS, "more threads than a purchase reference can name");
    let (survivors, purchases): (Vec<Scattered<Survivor>>, Vec<Vec<(u32, u32)>>) = par::run(
        threads,
        1 << event_bits,
        |thread| Dedup {
            seen: Vec::new(),
            again: Vec::new(),
            kept: HashTable::new(),
            waiting: [Survivor { who: 0, ts: 0, purchase: 0 }; AHEAD],
            next: 0,
            out: Scatter::new(1 << person_bits),
            thread,
            purchases: Vec::new(),
        },
        |state, part| {
            let count: usize = events.iter().map(|thread| thread.len(part)).sum();
            if count == 0 {
                return;
            }
            // Pass 1: an event whose hash bit is set only once is certainly not duplicated.
            let bits = (count * 8).next_power_of_two().max(64);
            let shift = 64 - bits.trailing_zeros();
            state.seen.clear();
            state.seen.resize(bits / 64, 0);
            state.again.clear();
            state.again.resize(bits / 64, 0);
            for thread in &events {
                for slice in thread.slices(part) {
                    for event in slice {
                        let h = (remix(key::hash(event.id)) >> shift) as usize;
                        let (word, bit) = (h / 64, 1u64 << (h % 64));
                        state.again[word] |= state.seen[word] & bit;
                        state.seen[word] |= bit;
                    }
                }
            }
            // Pass 2: the certainly-unique events go straight on. The few that may be
            // duplicated go through an exact table that keeps, per ID, the copy ingested
            // first (events step 2).
            state.kept.clear();
            for thread in &events {
                for slice in thread.slices(part) {
                    for event in slice {
                        let h = remix(key::hash(event.id));
                        let at = (h >> shift) as usize;
                        if state.again[at / 64] >> (at % 64) & 1 == 0 {
                            if event.category & VALID != 0 {
                                state.keep(event, renames, person_bits);
                            }
                            continue;
                        }
                        match state.kept.find_mut(h, |kept| kept.id == event.id) {
                            Some(kept) => {
                                if event.ingest_price >> 24 < kept.ingest_price >> 24 {
                                    *kept = *event;
                                }
                            }
                            None => {
                                state.kept.insert_unique(h, *event, |kept| remix(key::hash(kept.id)));
                            }
                        }
                    }
                }
            }
            // Steps 3 and 4 were decided at parse time, for whichever copy is kept.
            let winners = std::mem::take(&mut state.kept);
            for event in winners.iter().filter(|event| event.category & VALID != 0) {
                state.keep(event, renames, person_bits);
            }
            state.kept = winners;
            state.settle_all(renames, person_bits);
        },
    )
    .into_iter()
    .map(|state| (state.out.finish(), state.purchases))
    .unzip();
    drop(events);
    log("dedup");

    // ---- 3. Sessions, credit, assists, aggregation -----------------------------------------
    let first_day = window_start.div_euclid(DAY_MS);
    let days = ((window_end - 1).div_euclid(DAY_MS) - first_day + 1).max(1) as usize;
    let campaign_count = campaign_names.len();
    // Ties in top_category go to the byte-wise smallest name.
    let mut category_rank = vec![0u32; category_names.len()];
    let mut by_name: Vec<usize> = (0..category_names.len()).collect();
    by_name.sort_by_key(|&c| category_names[c]);
    for (rank, &c) in by_name.iter().enumerate() {
        category_rank[c] = rank as u32;
    }

    let sessions = par::run(
        threads,
        1 << person_bits,
        |_| Sessions {
            rows: output::UserRows::default(),
            campaign_days: vec![[0; 3]; campaign_count * days],
            assisted: vec![0; campaign_count * days],
            people: HashTable::new(),
            names: Vec::new(),
            owner: Vec::new(),
            event_start: Vec::new(),
            click_start: Vec::new(),
            events: Vec::new(),
            clicks: Vec::new(),
            in_window: vec![0; campaign_count],
            slot: vec![0; campaign_count],
            active: Vec::new(),
            day_categories: Vec::new(),
        },
        |s, part| {
            // Number the people of this partition and count each one's events.
            s.people.clear();
            s.names.clear();
            s.owner.clear();
            s.event_start.clear();
            s.event_start.push(0);
            for event in survivors.iter().flat_map(|thread| thread.slices(part)).flatten() {
                let h = remix(key::hash(event.who));
                let person = match s.people.find(h, |e| e.0 == event.who) {
                    Some(e) => e.1,
                    None => {
                        let person = s.names.len() as u32;
                        s.names.push(event.who);
                        s.event_start.push(0);
                        s.people.insert_unique(h, (event.who, person), |e| remix(key::hash(e.0)));
                        person
                    }
                };
                s.owner.push(person);
                s.event_start[person as usize + 1] += 1;
            }
            if s.owner.is_empty() {
                return;
            }
            let people = s.names.len();
            // Group the events by person, then put each person's in time order (events step 5).
            offsets(&mut s.event_start);
            s.events.clear();
            s.events.resize(s.owner.len(), 0);
            for (event, &person) in survivors.iter().flat_map(|thread| thread.slices(part)).flatten().zip(&s.owner) {
                let at = &mut s.event_start[person as usize + 1];
                s.events[*at as usize] = (event.ts as u64) << 32 | event.purchase as u64;
                *at += 1;
            }
            // Each start slid one person along while filling; entry p is again where p begins.
            for person in 0..people {
                s.events[s.event_start[person] as usize..s.event_start[person + 1] as usize].sort_unstable();
            }

            // The same for clicks. A person with no surviving events has no sessions to credit.
            s.owner.clear();
            s.click_start.clear();
            s.click_start.resize(people + 1, 0);
            for click in clicks.iter().flat_map(|thread| thread.slices(part)).flatten() {
                let person = s.people.find(remix(key::hash(click.who)), |e| e.0 == click.who).map_or(u32::MAX, |e| e.1);
                s.owner.push(person);
                if person != u32::MAX {
                    s.click_start[person as usize + 1] += 1;
                }
            }
            let kept: u32 = s.click_start.iter().sum();
            offsets(&mut s.click_start);
            s.clicks.clear();
            s.clicks.resize(kept as usize, 0);
            // An interned click ID does not order like its bytes, so a partition holding
            // one sorts its clicks with the exact comparison instead.
            let mut odd_ids = false;
            for (click, &person) in clicks.iter().flat_map(|thread| thread.slices(part)).flatten().zip(&s.owner) {
                if person != u32::MAX {
                    odd_ids |= key::is_interned(click.id);
                    let at = &mut s.click_start[person as usize + 1];
                    s.clicks[*at as usize] = click_key(click.ts, click.id, click.campaign);
                    *at += 1;
                }
            }

            for person in 0..people {
                let events = &s.events[s.event_start[person] as usize..s.event_start[person + 1] as usize];
                let clicks = &mut s.clicks[s.click_start[person] as usize..s.click_start[person + 1] as usize];
                // Within one millisecond the smallest click_id goes last, so "the last click at or
                // before the session start" is the one the tie rule picks.
                if odd_ids {
                    clicks.sort_unstable_by(|&a, &b| click_ts(a).cmp(&click_ts(b)).then_with(|| key::cmp_bytes(click_id(b), click_id(a))));
                } else {
                    clicks.sort_unstable();
                }
                let clicks = &*clicks;
                // The window of clicks within 7 days before the session start is clicks[lo..hi].
                let (mut lo, mut hi) = (0, 0);

                // The current user_daily row.
                let mut row_day = i64::MIN;
                let (mut row_sessions, mut row_events, mut row_revenue) = (0i64, 0i64, 0i64);
                let mut flush = |day: i64, sessions: i64, events: i64, revenue: i64, categories: &mut Vec<(u32, u64)>| {
                    if sessions == 0 {
                        return;
                    }
                    let top = categories
                        .iter()
                        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| category_rank[b.0 as usize].cmp(&category_rank[a.0 as usize])))
                        .map_or(output::NO_CATEGORY, |&(c, _)| c);
                    categories.clear();
                    s.rows.user_id.push(s.names[person]);
                    s.rows.day.push(day as i32);
                    s.rows.sessions.push(sessions);
                    s.rows.events.push(events);
                    s.rows.revenue_cents.push(revenue);
                    s.rows.top_category.push(top);
                };

                let mut i = 0;
                while i < events.len() {
                    // One session: events up to the first gap over 30 minutes.
                    let start = (events[i] >> 32) as u32;
                    // Step 6: the session's day is the UTC date of its start.
                    let day = (base + start as i64).div_euclid(DAY_MS);
                    if day != row_day {
                        flush(row_day, row_sessions, row_events, row_revenue, &mut s.day_categories);
                        (row_day, row_sessions, row_events, row_revenue) = (day, 0, 0, 0);
                    }
                    let mut previous = start;
                    let (mut count, mut revenue) = (0i64, 0u64);
                    while i < events.len() {
                        let ts = (events[i] >> 32) as u32;
                        if ts - previous > GAP_MS {
                            break;
                        }
                        previous = ts;
                        count += 1;
                        let purchase = events[i] as u32;
                        if purchase != 0 {
                            let (price, category) = purchase_of(&purchases, purchase);
                            revenue += price as u64;
                            match s.day_categories.iter_mut().find(|c| c.0 == category) {
                                Some(c) => c.1 += price as u64,
                                None => s.day_categories.push((category, price as u64)),
                            }
                        }
                        i += 1;
                    }
                    row_sessions += 1;
                    row_events += count;
                    row_revenue += revenue as i64;

                    // Clicks steps 4 to 7. Slide the window to this session's start.
                    while hi < clicks.len() && click_ts(clicks[hi]) <= start {
                        // Clicks step 2: copies of a click are identical, so they are neighbours
                        // here (same time and ID), and only the first of them counts.
                        if hi == 0 || clicks[hi] >> 32 != clicks[hi - 1] >> 32 {
                            let c = clicks[hi] as u32 as usize;
                            if s.in_window[c] == 0 {
                                s.slot[c] = s.active.len() as u32;
                                s.active.push(c as u32);
                            }
                            s.in_window[c] += 1;
                        }
                        hi += 1;
                    }
                    while lo < hi && (start - click_ts(clicks[lo])) as i64 > WEEK_MS {
                        if lo == 0 || clicks[lo] >> 32 != clicks[lo - 1] >> 32 {
                            let c = clicks[lo] as u32 as usize;
                            s.in_window[c] -= 1;
                            if s.in_window[c] == 0 {
                                let at = s.slot[c] as usize;
                                let moved = s.active.pop().unwrap();
                                if moved as usize != c {
                                    s.active[at] = moved;
                                    s.slot[moved as usize] = at as u32;
                                }
                            }
                        }
                        lo += 1;
                    }
                    let credited = if hi > lo { clicks[hi - 1] as u32 as usize } else { organic };
                    let day_index = (day - first_day) as usize;
                    let cell = &mut s.campaign_days[credited * days + day_index];
                    cell[0] += 1;
                    cell[1] += (revenue != 0) as u64;
                    cell[2] += revenue;
                    for &c in &s.active {
                        s.assisted[c as usize * days + day_index] += 1;
                    }
                }
                flush(row_day, row_sessions, row_events, row_revenue, &mut s.day_categories);
                for &c in &s.active {
                    s.in_window[c as usize] = 0;
                }
                s.active.clear();
            }
        },
    );
    log("sessions");

    // ---- 4. Write ------------------------------------------------------------------------
    let user_dir = out_dir.join("user_daily");
    let campaign_dir = out_dir.join("campaign_daily");
    for folder in [&user_dir, &campaign_dir] {
        std::fs::create_dir_all(folder)?;
        // A rerun into the same folder must not leave an older run's files behind.
        for entry in std::fs::read_dir(folder)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                std::fs::remove_file(path)?;
            }
        }
    }

    let mut campaign_rows = output::CampaignRows::default();
    for campaign in 0..campaign_count {
        for day in 0..days {
            let mut total = [0u64; 4];
            for thread in &sessions {
                let cell = thread.campaign_days[campaign * days + day];
                for k in 0..3 {
                    total[k] += cell[k];
                }
                total[3] += thread.assisted[campaign * days + day] as u64;
            }
            if total[0] == 0 && total[3] == 0 {
                continue;
            }
            campaign_rows.campaign.push(campaign as u32);
            campaign_rows.day.push((first_day + day as i64) as i32);
            campaign_rows.sessions.push(total[0] as i64);
            campaign_rows.purchasing_sessions.push(total[1] as i64);
            campaign_rows.revenue_cents.push(total[2] as i64);
            campaign_rows.assisted_sessions.push(total[3] as i64);
        }
    }

    let results = par::run(
        threads,
        sessions.len() + 1,
        |_| Vec::<Res<()>>::new(),
        |results, i| {
            if i == sessions.len() {
                results.push(output::write_campaign_daily(&campaign_dir.join("part-000.parquet"), &campaign_rows, &campaign_names, &channels));
            } else if i == 0 || !sessions[i].rows.user_id.is_empty() {
                // The first file is written even when empty, so the table always exists.
                results.push(output::write_user_daily(&user_dir.join(format!("part-{i:03}.parquet")), &sessions[i].rows, &category_names));
            }
        },
    );
    for result in results.into_iter().flatten() {
        result?;
    }
    log("write");
    eprintln!("total      {:>8.1} ms", started.elapsed().as_secs_f64() * 1e3);
    // Everything left is memory the OS reclaims faster than we could free it.
    std::process::exit(0);
}
