//! Shopping Sessions pipeline. See docs/rules.md for the rules and
//! docs/PLAN.md for the design.
//!
//! Every stage is parallel, and every lookup happens inside a partition small
//! enough to stay in the CPU cache: records are passed from stage to stage by
//! scattering them on the hash of whatever the next stage looks up.
//!
//!   1. parse     chunks of every CSV into records; events scattered by event_id,
//!                clicks by user ID
//!   2. link      IDs named in links/ -> each one's person name (union-find)
//!   3. dedup     per event_id partition; survivors scattered by user ID
//!   4. person    per user partition: user ID -> person name; scattered by person
//!   5. sessions  per person partition: sort, sessions, click credit, aggregation
//!   6. write     Parquet, one user_daily file per thread

mod csv;
mod io;
mod key;
mod output;
mod par;
mod scan;
mod table;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use hashbrown::HashTable;
use key::{Key, partition, remix};
use table::{Str, StrMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const GAP_MS: u32 = 30 * 60 * 1000;
const WEEK_MS: i64 = 7 * DAY_MS;
const CHUNK: u64 = 512 << 10;
/// Seconds from 0000-01-01 to the epoch: makes any F1 time non-negative.
const YEAR_ZERO: i64 = 62_167_219_200;
const VALID: u32 = 1 << 31;
const EMPTY: u32 = u32::MAX;

/// One parsed event line, before dedup. 48 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    id: Key,
    /// Meaningful only when valid, like `ts`.
    user: Key,
    /// Ingest time in seconds since year 0 (high 40 bits), price in cents (low 24; 0 unless a purchase).
    ingest_price: u64,
    /// Milliseconds since `Ctx::base`.
    ts: u32,
    /// Category, plus `VALID` if the event passes validation and is not late.
    category: u32,
}

/// A surviving event on its way to its person. `who` is the user ID, then the person's name.
#[derive(Clone, Copy)]
#[repr(C)]
struct Survivor {
    who: Key,
    ts: u32,
    price: u32,
    category: u32,
}

/// A valid click. `who` is the user ID, then the person's name.
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    who: Key,
    id: Key,
    ts: u32,
    campaign: u32,
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
    event_bits: u32,
    user_bits: u32,
}

#[derive(Clone, Copy)]
enum Kind {
    Events,
    Clicks,
    Links,
}

struct Parsed {
    /// By partition of the event ID.
    events: Vec<Vec<Event>>,
    /// By partition of the user ID.
    clicks: Vec<Vec<Click>>,
    links: Vec<(Key, Key)>,
    buf: Vec<u8>,
    tokens: Vec<u32>,
    dates: ts::Dates,
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
        user: Key::default(),
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
        event.user = unsafe { Key::text(user) };
        event.ingest_price |= cents as u64;
        event.category |= VALID;
    }
    out.events[partition(event.id.hash(), ctx.event_bits)].push(event);
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
    out.clicks[partition(who.hash(), ctx.user_bits)].push(Click {
        who,
        id: unsafe { Key::text(id) },
        ts: (t - ctx.base) as u32,
        campaign,
    });
}

#[inline]
fn parse_link(&[a, b]: &[&[u8]; 2], out: &mut Parsed) {
    if a.is_empty() || b.is_empty() {
        return;
    }
    out.links.push(unsafe { (Key::text(a), Key::text(b)) });
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

fn buckets<T>(bits: u32) -> Vec<Vec<T>> {
    (0..1usize << bits).map(|_| Vec::new()).collect()
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
                let end = (start + CHUNK).min(len);
                chunks.push((kind, files.len(), start, end));
                start = end;
            }
            files.push((file, len));
        }
    }
    let rough_events = (event_bytes / 64) as usize;
    let event_bits = bits_for(rough_events, 1 << 15, 12);
    let user_bits = bits_for(rough_events, 1 << 16, 11);
    let person_bits = user_bits;
    let ctx = Ctx { window_start, window_end, base, products, unknown_category, campaigns, event_bits, user_bits };
    log("open");

    // ---- 1. Parse ------------------------------------------------------------------------
    let mut parsed = par::run(
        threads,
        chunks.len(),
        |_| Parsed { events: buckets(event_bits), clicks: buckets(user_bits), links: Vec::new(), buf: Vec::new(), tokens: Vec::new(), dates: ts::Dates::new() },
        |out, i| {
            let (kind, file, start, end) = chunks[i];
            let mut buf = std::mem::take(&mut out.buf);
            let (len, first) = io::load_chunk(&files[file].0, files[file].1, start, end, &mut buf).expect("read failed");
            let (data, last) = (&buf[..len], first + (end - start) as usize);
            let mut tokens = std::mem::take(&mut out.tokens);
            scan::index(data, &mut tokens);
            match kind {
                Kind::Events => scan::for_each_record(data, first, last, &tokens, |fields| parse_event(fields, &ctx, out)),
                Kind::Clicks => scan::for_each_record(data, first, last, &tokens, |fields| parse_click(fields, &ctx, out)),
                Kind::Links => scan::for_each_record(data, first, last, &tokens, |fields| parse_link(fields, out)),
            }
            out.buf = buf;
            out.tokens = tokens;
        },
    );
    drop(files);
    log("parse");

    // ---- 2. Linked IDs -------------------------------------------------------------------
    // Give every ID that appears in a link a number, partition by partition.
    let user_parts = 1usize << user_bits;
    let mut link_base = vec![0usize; parsed.len() + 1];
    for (t, thread) in parsed.iter().enumerate() {
        link_base[t + 1] = link_base[t] + thread.links.len();
    }
    let link_count = link_base[parsed.len()];
    // An endpoint: the ID, and which side of which link it is.
    let ends = par::run(
        threads,
        parsed.len(),
        |_| buckets::<(Key, u32)>(user_bits),
        |out, t| {
            for (i, &(a, b)) in parsed[t].links.iter().enumerate() {
                let slot = ((link_base[t] + i) * 2) as u32;
                out[partition(a.hash(), user_bits)].push((a, slot));
                out[partition(b.hash(), user_bits)].push((b, slot + 1));
            }
        },
    );
    struct Nodes {
        ids: Vec<Key>,
        /// (endpoint slot, index into `ids`)
        ends: Vec<(u32, u32)>,
    }
    let nodes: Vec<Nodes> = par::map(threads, user_parts, |part| {
        let mut table: HashTable<(Key, u32)> = HashTable::new();
        let mut found = Nodes { ids: Vec::new(), ends: Vec::new() };
        for thread in &ends {
            for &(id, slot) in &thread[part] {
                let h = remix(id.hash());
                let index = match table.find(h, |e| e.0 == id) {
                    Some(e) => e.1,
                    None => {
                        let index = found.ids.len() as u32;
                        found.ids.push(id);
                        table.insert_unique(h, (id, index), |e| remix(e.0.hash()));
                        index
                    }
                };
                found.ends.push((slot, index));
            }
        }
        found
    });
    drop(ends);
    let mut node_base = vec![0u32; user_parts + 1];
    for part in 0..user_parts {
        node_base[part + 1] = node_base[part] + nodes[part].ids.len() as u32;
    }
    let node_count = node_base[user_parts] as usize;
    let mut endpoint = vec![0u32; link_count * 2];
    {
        let cells = par::Shared(endpoint.as_mut_ptr());
        par::run(threads, user_parts, |_| (), |_, part| {
            let cells = cells;
            for &(slot, index) in &nodes[part].ends {
                // SAFETY: every slot belongs to exactly one endpoint, so no two writes collide.
                unsafe { *cells.0.add(slot as usize) = node_base[part] + index };
            }
        });
    }
    // Links step 2: union-find over the link pairs.
    let mut parent: Vec<u32> = (0..node_count as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up;
            x = up;
        }
        x
    }
    for pair in endpoint.chunks_exact(2) {
        let (a, b) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
        if a != b {
            parent[a.max(b) as usize] = a.min(b);
        }
    }
    drop(endpoint);
    // Links step 3: each person is named by the byte-wise smallest of their IDs.
    let mut name: Vec<Key> = nodes.iter().flat_map(|n| n.ids.iter().copied()).collect();
    let all_ids = name.clone();
    for node in 0..node_count {
        let root = find(&mut parent, node as u32) as usize;
        if all_ids[node].cmp_bytes(&name[root]).is_lt() {
            name[root] = all_ids[node];
        }
    }
    drop(all_ids);
    // Per partition: linked ID -> person name, for the IDs that are not their own person's name.
    // An ID missing from these tables is a person on its own, or is itself the name.
    let renames: Vec<HashTable<(Key, Key)>> = par::map(threads, user_parts, |part| {
        let mut table = HashTable::new();
        for (index, &id) in nodes[part].ids.iter().enumerate() {
            // Roots point at themselves and every other node at most two steps away after
            // the pass above, so follow without compressing.
            let mut root = node_base[part] as usize + index;
            while parent[root] as usize != root {
                root = parent[root] as usize;
            }
            if name[root] != id {
                table.insert_unique(remix(id.hash()), (id, name[root]), |e: &(Key, Key)| remix(e.0.hash()));
            }
        }
        table
    });
    drop((nodes, parent, name));
    log("link");

    // ---- 3. Dedup events; scatter survivors by user ID --------------------------------------
    struct Dedup {
        events: Vec<Event>,
        slots: Vec<u32>,
        out: Vec<Vec<Survivor>>,
    }
    let deduped = par::run(
        threads,
        1 << event_bits,
        |_| Dedup { events: Vec::new(), slots: Vec::new(), out: buckets(user_bits) },
        |state, part| {
            state.events.clear();
            for thread in &parsed {
                state.events.extend_from_slice(&thread.events[part]);
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
                    state.out[partition(event.user.hash(), user_bits)].push(Survivor {
                        who: event.user,
                        ts: event.ts,
                        price: (event.ingest_price & 0xFF_FFFF) as u32,
                        category: event.category & !VALID,
                    });
                }
            }
        },
    );
    for thread in &mut parsed {
        thread.events = Vec::new();
    }
    log("dedup");

    // ---- 4. User ID -> person; scatter by person ---------------------------------------------
    struct ByPerson {
        events: Vec<Vec<Survivor>>,
        clicks: Vec<Vec<Click>>,
    }
    let by_person = par::run(
        threads,
        user_parts,
        |_| ByPerson { events: buckets(person_bits), clicks: buckets(person_bits) },
        |out, part| {
            let renames = &renames[part];
            let person = |id: Key| -> Key {
                if renames.is_empty() {
                    return id;
                }
                renames.find(remix(id.hash()), |e| e.0 == id).map_or(id, |e| e.1)
            };
            for thread in &deduped {
                for event in &thread.out[part] {
                    let who = person(event.who);
                    out.events[partition(who.hash(), person_bits)].push(Survivor { who, ..*event });
                }
            }
            for thread in &parsed {
                for click in &thread.clicks[part] {
                    let who = person(click.who);
                    out.clicks[partition(who.hash(), person_bits)].push(Click { who, ..*click });
                }
            }
        },
    );
    drop((parsed, deduped, renames));
    log("person");

    // ---- 5. Sessions, credit, assists, aggregation -----------------------------------------
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
            for thread in &by_person {
                for event in &thread.events[part] {
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
            for thread in &by_person {
                for click in &thread.clicks[part] {
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

    // ---- 6. Write ------------------------------------------------------------------------
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
