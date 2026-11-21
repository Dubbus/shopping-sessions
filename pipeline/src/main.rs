//! Shopping Sessions pipeline. See docs/rules.md for the rules and
//! docs/PLAN.md for the design.
//!
//! Every stage is parallel, and every lookup happens inside a partition small
//! enough to stay in the CPU cache: records are passed from stage to stage by
//! scattering them on the hash of whatever the next stage looks up.
//!
//!   1. link      links/ first: every linked ID -> its person's name (union-find)
//!   2. parse     chunks of events/ and clicks/ into records, user IDs replaced by
//!                person names; events scattered by event_id, clicks by person
//!   3. dedup     per event_id partition; survivors scattered by person
//!   4. sessions  per person partition: sort, sessions, click credit, aggregation
//!   5. write     Parquet, one user_daily file per thread

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
use std::time::Instant;

use hashbrown::HashTable;
use key::{Key, partition, remix};
use scatter::{Scatter, Scattered};
use table::{Str, StrMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const GAP_MS: u32 = 30 * 60 * 1000;
const WEEK_MS: i64 = 7 * DAY_MS;
/// Seconds from 0000-01-01 to the epoch: makes any F1 time non-negative.
const YEAR_ZERO: i64 = 62_167_219_200;
const VALID: u32 = 1 << 31;
const EMPTY: u32 = u32::MAX;

/// One parsed event line, before dedup. 48 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    id: Key,
    /// The person's name. Meaningful only when valid, like `ts`.
    who: Key,
    /// Ingest time in seconds since year 0 (high 40 bits), price in cents (low 24; 0 unless a purchase).
    ingest_price: u64,
    /// Milliseconds since `Ctx::base`.
    ts: u32,
    /// Category, plus `VALID` if the event passes validation and is not late.
    category: u32,
}

/// A surviving event on its way to its person's partition. 32 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Survivor {
    who: Key,
    ts: u32,
    price: u32,
    category: u32,
    pad: u32,
}

/// A valid click on its way to its person's partition. 48 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    who: Key,
    id: Key,
    ts: u32,
    campaign: u32,
    pad: u64,
}

/// An event inside its person partition: person index in the high half of the key, time in the low.
#[derive(Clone, Copy)]
struct PersonEvent {
    key: u64,
    price: u32,
    category: u32,
}

#[derive(Clone, Copy)]
struct PersonClick {
    key: u64,
    id: Key,
    campaign: u32,
}

/// Everything the parse stage reads but never changes.
struct Ctx {
    window_start: i64,
    window_end: i64,
    /// `window_start` minus 7 days: the zero of every stored timestamp.
    base: i64,
    products: StrMap,
    unknown_category: u32,
    campaigns: StrMap,
    renames: link::Renames,
    event_bits: u32,
    person_bits: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Events,
    Clicks,
    Links,
}

/// How many records wait for their rename-table slot to arrive in cache.
const BATCH: usize = 64;

struct Parsed {
    /// By partition of the event ID.
    events: Scatter<Event>,
    /// By partition of the person.
    clicks: Scatter<Click>,
    /// Parsed records whose user ID is not yet replaced by the person's name, with its hash.
    waiting_events: Vec<(Event, u64)>,
    waiting_clicks: Vec<(Click, u64)>,
    buf: Vec<u8>,
    index: scan::Index,
    dates: ts::Dates,
}

impl Parsed {
    /// Events step "links come before everything else": the user ID becomes the person.
    #[inline]
    fn settle_events(&mut self, ctx: &Ctx) {
        for (mut event, hash) in self.waiting_events.drain(..) {
            event.who = ctx.renames.person(event.who, hash);
            self.events.push(partition(event.id.hash(), ctx.event_bits), event);
        }
    }

    #[inline]
    fn settle_clicks(&mut self, ctx: &Ctx) {
        for (mut click, hash) in self.waiting_clicks.drain(..) {
            click.who = ctx.renames.person(click.who, hash);
            self.clicks.push(partition(click.who.hash(), ctx.person_bits), click);
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
        id: unsafe { Key::identity(id) },
        who: Key::default(),
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
        let cents = match kind {
            b"view" | b"cart" => 0,
            b"purchase" => {
                let Some(cents) = parse_price(price) else { break 'valid };
                event.category = ctx.products.get(product).unwrap_or(ctx.unknown_category);
                cents
            }
            _ => break 'valid,
        };
        event.ts = (t - ctx.base) as u32;
        event.who = unsafe { Key::text(user) };
        event.ingest_price |= cents as u64;
        event.category |= VALID;
        let hash = event.who.hash();
        ctx.renames.prefetch(hash);
        out.waiting_events.push((event, hash));
        if out.waiting_events.len() == BATCH {
            out.settle_events(ctx);
        }
        return;
    }
    // Not valid: it can still be the copy dedup keeps, so it travels on.
    out.events.push(partition(event.id.hash(), ctx.event_bits), event);
}

#[inline]
fn parse_click(&[id, user, campaign, ts]: &[&[u8]; 4], ctx: &Ctx, out: &mut Parsed) {
    // Clicks steps 1 and 3. Copies of a click are identical, so validating before dedup is the same.
    if id.is_empty() || user.is_empty() {
        return;
    }
    let Some(campaign) = ctx.campaigns.get(campaign) else { return };
    let Some(t) = out.dates.parse(ts) else { return };
    if t < ctx.base || t >= ctx.window_end {
        return;
    }
    let who = unsafe { Key::text(user) };
    let hash = who.hash();
    ctx.renames.prefetch(hash);
    out.waiting_clicks.push((Click { who, id: unsafe { Key::text(id) }, ts: (t - ctx.base) as u32, campaign, pad: 0 }, hash));
    if out.waiting_clicks.len() == BATCH {
        out.settle_clicks(ctx);
    }
}

#[inline]
fn parse_link(&[a, b]: &[&[u8]; 2], out: &mut Vec<(Key, Key)>) {
    // Links step 1: malformed lines never get here; a line with an empty side is dropped.
    if a.is_empty() || b.is_empty() {
        return;
    }
    out.push(unsafe { (Key::text(a), Key::text(b)) });
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
    /// Per (campaign, day): sessions, purchasing sessions, revenue, assisted sessions.
    campaign_days: Vec<[u64; 4]>,
    /// Person name -> index into `names`, for this partition.
    people: HashTable<(Key, u32)>,
    names: Vec<Key>,
    events: Vec<PersonEvent>,
    clicks: Vec<PersonClick>,
    /// Clicks per campaign inside the current 7-day window, and the campaigns present.
    in_window: Vec<u32>,
    slot: Vec<u32>,
    active: Vec<u32>,
    /// Purchase cents per category for the current person and day.
    day_categories: Vec<(u32, u64)>,
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

    let mut campaigns = StrMap::default();
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

    // ---- 1. Linked IDs -------------------------------------------------------------------
    struct LinkReader {
        links: Vec<(Key, Key)>,
        buf: Vec<u8>,
        index: scan::Index,
    }
    let link_chunks: Vec<_> = chunks.iter().copied().filter(|c| c.0 == Kind::Links).collect();
    let links: Vec<Vec<(Key, Key)>> = par::run(
        threads,
        link_chunks.len(),
        |_| LinkReader { links: Vec::new(), buf: Vec::new(), index: scan::Index::default() },
        |out, i| {
            let (_, file, start, end) = link_chunks[i];
            let (len, first) = io::load_chunk(&files[file].0, files[file].1, start, end, &mut out.buf).expect("read failed");
            out.index.build(&out.buf, len);
            let links = &mut out.links;
            out.index.for_each_record(&out.buf, first, first + (end - start) as usize, |fields| parse_link(fields, links));
        },
    )
    .into_iter()
    .map(|reader| reader.links)
    .collect();
    let renames = link::resolve(threads, &links, person_bits);
    drop(links);
    let ctx = Ctx { window_start, window_end, base, products, unknown_category, campaigns, renames, event_bits, person_bits };
    log("link");

    // ---- 2. Parse ------------------------------------------------------------------------
    let chunks: Vec<_> = chunks.into_iter().filter(|c| c.0 != Kind::Links).collect();
    let parsed = par::run(
        threads,
        chunks.len(),
        |_| Parsed {
            events: Scatter::new(1 << event_bits),
            clicks: Scatter::new(1 << person_bits),
            waiting_events: Vec::with_capacity(BATCH),
            waiting_clicks: Vec::with_capacity(BATCH),
            buf: Vec::new(),
            index: scan::Index::default(),
            dates: ts::Dates::new(),
        },
        |out, i| {
            let (kind, file, start, end) = chunks[i];
            let mut buf = std::mem::take(&mut out.buf);
            let (len, first) = io::load_chunk(&files[file].0, files[file].1, start, end, &mut buf).expect("read failed");
            let last = first + (end - start) as usize;
            let mut index = std::mem::take(&mut out.index);
            index.build(&buf, len);
            if kind == Kind::Events {
                index.for_each_record(&buf, first, last, |fields| parse_event(fields, &ctx, out));
                out.settle_events(&ctx);
            } else {
                index.for_each_record(&buf, first, last, |fields| parse_click(fields, &ctx, out));
                out.settle_clicks(&ctx);
            }
            out.buf = buf;
            out.index = index;
        },
    );
    drop(files);
    let (events, clicks): (Vec<Scattered<Event>>, Vec<Scattered<Click>>) =
        parsed.into_iter().map(|thread| (thread.events.finish(), thread.clicks.finish())).unzip();
    log("parse");

    // ---- 3. Dedup events; scatter survivors by person ---------------------------------------
    struct Dedup {
        events: Vec<Event>,
        slots: Vec<u32>,
        out: Scatter<Survivor>,
    }
    let survivors: Vec<Scattered<Survivor>> = par::run(
        threads,
        1 << event_bits,
        |_| Dedup { events: Vec::new(), slots: Vec::new(), out: Scatter::new(1 << person_bits) },
        |state, part| {
            state.events.clear();
            for thread in &events {
                for slice in thread.slices(part) {
                    state.events.extend_from_slice(slice);
                }
            }
            let events = &state.events;
            if events.is_empty() {
                return;
            }
            let capacity = (events.len() * 2).next_power_of_two().max(16);
            let shift = 64 - capacity.trailing_zeros();
            state.slots.clear();
            state.slots.resize(capacity, EMPTY);
            for (i, event) in events.iter().enumerate() {
                // Events step 2: one copy per event_id, the one ingested first.
                let mut at = (remix(event.id.hash()) >> shift) as usize;
                loop {
                    let held = state.slots[at];
                    if held == EMPTY {
                        state.slots[at] = i as u32;
                        break;
                    }
                    let kept = &events[held as usize];
                    if kept.id == event.id {
                        if event.ingest_price >> 24 < kept.ingest_price >> 24 {
                            state.slots[at] = i as u32;
                        }
                        break;
                    }
                    at = (at + 1) & (capacity - 1);
                }
            }
            for &held in &state.slots {
                if held == EMPTY {
                    continue;
                }
                let event = &events[held as usize];
                if event.category & VALID != 0 {
                    state.out.push(
                        partition(event.who.hash(), person_bits),
                        Survivor { who: event.who, ts: event.ts, price: (event.ingest_price & 0xFF_FFFF) as u32, category: event.category & !VALID, pad: 0 },
                    );
                }
            }
        },
    )
    .into_iter()
    .map(|state| state.out.finish())
    .collect();
    drop(events);
    log("dedup");

    // ---- 4. Sessions, credit, assists, aggregation -----------------------------------------
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
            campaign_days: vec![[0; 4]; campaign_count * days],
            people: HashTable::new(),
            names: Vec::new(),
            events: Vec::new(),
            clicks: Vec::new(),
            in_window: vec![0; campaign_count],
            slot: vec![0; campaign_count],
            active: Vec::new(),
            day_categories: Vec::new(),
        },
        |s, part| {
            s.people.clear();
            s.names.clear();
            s.events.clear();
            for event in survivors.iter().flat_map(|thread| thread.slices(part)).flatten() {
                {
                    let h = remix(event.who.hash());
                    let person = match s.people.find(h, |e| e.0 == event.who) {
                        Some(e) => e.1,
                        None => {
                            let person = s.names.len() as u32;
                            s.names.push(event.who);
                            s.people.insert_unique(h, (event.who, person), |e| remix(e.0.hash()));
                            person
                        }
                    };
                    s.events.push(PersonEvent { key: (person as u64) << 32 | event.ts as u64, price: event.price, category: event.category });
                }
            }
            if s.events.is_empty() {
                return;
            }
            // Events step 5: each person's events in time order.
            s.events.sort_unstable_by_key(|e| e.key);

            s.clicks.clear();
            for click in clicks.iter().flat_map(|thread| thread.slices(part)).flatten() {
                {
                    // A person with no surviving events has no sessions to credit.
                    if let Some(e) = s.people.find(remix(click.who.hash()), |e| e.0 == click.who) {
                        s.clicks.push(PersonClick { key: (e.1 as u64) << 32 | click.ts as u64, id: click.id, campaign: click.campaign });
                    }
                }
            }
            // Within one millisecond the smallest click_id goes last, so "the last click at or
            // before the session start" is the one the tie rule picks.
            s.clicks.sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| b.id.cmp_bytes(&a.id)));
            // Clicks step 2: copies of a click are identical, so they are neighbours now.
            s.clicks.dedup_by(|b, a| a.key == b.key && a.id == b.id);

            let (events, clicks) = (&s.events, &s.clicks);
            let mut next_click = 0;
            let mut i = 0;
            while i < events.len() {
                let person = (events[i].key >> 32) as u32;
                while next_click < clicks.len() && ((clicks[next_click].key >> 32) as u32) < person {
                    next_click += 1;
                }
                let first_click = next_click;
                while next_click < clicks.len() && (clicks[next_click].key >> 32) as u32 == person {
                    next_click += 1;
                }
                let clicks = &clicks[first_click..next_click];
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
                        .map(|&(c, _)| category_names[c as usize]);
                    categories.clear();
                    s.rows.user_id.push(s.names[person as usize]);
                    s.rows.day.push(day as i32);
                    s.rows.sessions.push(sessions);
                    s.rows.events.push(events);
                    s.rows.revenue_cents.push(revenue);
                    s.rows.top_category.push(top);
                };

                while i < events.len() && (events[i].key >> 32) as u32 == person {
                    // One session: events up to the first gap over 30 minutes.
                    let start = events[i].key as u32;
                    // Step 6: the session's day is the UTC date of its start.
                    let day = (base + start as i64).div_euclid(DAY_MS);
                    if day != row_day {
                        flush(row_day, row_sessions, row_events, row_revenue, &mut s.day_categories);
                        (row_day, row_sessions, row_events, row_revenue) = (day, 0, 0, 0);
                    }
                    let mut previous = start;
                    let (mut count, mut revenue) = (0i64, 0u64);
                    while i < events.len() && (events[i].key >> 32) as u32 == person {
                        let event = events[i];
                        let t = event.key as u32;
                        if t - previous > GAP_MS {
                            break;
                        }
                        previous = t;
                        count += 1;
                        if event.price != 0 {
                            revenue += event.price as u64;
                            match s.day_categories.iter_mut().find(|c| c.0 == event.category) {
                                Some(c) => c.1 += event.price as u64,
                                None => s.day_categories.push((event.category, event.price as u64)),
                            }
                        }
                        i += 1;
                    }
                    row_sessions += 1;
                    row_events += count;
                    row_revenue += revenue as i64;

                    // Clicks steps 4 to 7. Slide the window to this session's start.
                    while hi < clicks.len() && clicks[hi].key as u32 <= start {
                        let c = clicks[hi].campaign as usize;
                        if s.in_window[c] == 0 {
                            s.slot[c] = s.active.len() as u32;
                            s.active.push(c as u32);
                        }
                        s.in_window[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && (start - clicks[lo].key as u32) as i64 > WEEK_MS {
                        let c = clicks[lo].campaign as usize;
                        s.in_window[c] -= 1;
                        if s.in_window[c] == 0 {
                            let at = s.slot[c] as usize;
                            let moved = s.active.pop().unwrap();
                            if moved as usize != c {
                                s.active[at] = moved;
                                s.slot[moved as usize] = at as u32;
                            }
                        }
                        lo += 1;
                    }
                    let credited = if hi > lo { clicks[hi - 1].campaign as usize } else { organic };
                    let day_index = (day - first_day) as usize;
                    let cell = &mut s.campaign_days[credited * days + day_index];
                    cell[0] += 1;
                    cell[1] += (revenue != 0) as u64;
                    cell[2] += revenue;
                    for &c in &s.active {
                        s.campaign_days[c as usize * days + day_index][3] += 1;
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

    // ---- 5. Write ------------------------------------------------------------------------
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
                for k in 0..4 {
                    total[k] += cell[k];
                }
            }
            if total[0] == 0 && total[3] == 0 {
                continue;
            }
            campaign_rows.campaign_id.push(campaign_names[campaign]);
            campaign_rows.channel.push(channels[campaign]);
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
                results.push(output::write_campaign_daily(&campaign_dir.join("part-000.parquet"), &campaign_rows));
            } else if i == 0 || !sessions[i].rows.user_id.is_empty() {
                // The first file is written even when empty, so the table always exists.
                results.push(output::write_user_daily(&user_dir.join(format!("part-{i:03}.parquet")), &sessions[i].rows));
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
