//! Shopping Sessions pipeline. See docs/rules.md for the rules and
//! docs/PLAN.md for the design. Stages, each parallel across all cores:
//!
//!   1. parse     every CSV chunk into compact records; intern user IDs
//!   2. link      union-find over the link pairs; name each person
//!   3. dedup     events by event_id (partitioned by its hash), then validate
//!   4. sessions  per person: sort, split into sessions, credit clicks, aggregate
//!   5. write     Parquet, one user_daily file per thread

mod csv;
mod output;
mod par;
mod table;
mod ts;

use std::path::{Path, PathBuf};
use std::time::Instant;

use hashbrown::HashTable;
use table::{Interner, Str, StrMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const GAP_MS: u32 = 30 * 60 * 1000;
const WEEK_MS: i64 = 7 * DAY_MS;
const CHUNK: usize = 4 << 20;

/// A reference to an event or click ID inside a mapped file: pointer plus length.
/// IDs are the first field of their line, so one is always followed by `,` or `"`,
/// which lets an over-long ID (65,535 bytes or more) be measured again on demand.
#[derive(Clone, Copy)]
struct IdRef(u64);

impl IdRef {
    #[inline]
    fn new(s: Str) -> Self {
        IdRef(s.as_ptr() as u64 | (s.len().min(0xFFFF) as u64) << 48)
    }

    #[inline]
    fn bytes(self) -> Str {
        let p = (self.0 & 0xFFFF_FFFF_FFFF) as *const u8;
        let mut n = (self.0 >> 48) as usize;
        // SAFETY: the pointer came from a mapped file that lives for the whole run,
        // and the ID is followed by a comma or its closing quote.
        unsafe {
            if n == 0xFFFF {
                while !matches!(*p.add(n), b',' | b'"') {
                    n += 1;
                }
            }
            std::slice::from_raw_parts(p, n)
        }
    }
}

/// One parsed event line, before dedup.
#[derive(Clone, Copy)]
struct Event {
    hash: u64,
    id: IdRef,
    /// Milliseconds since the epoch.
    ingest: i64,
    /// Milliseconds since `Ctx::base`. Meaningful only when valid.
    ts: u32,
    user: u32,
    /// `VALID` plus the price in cents (0 unless a purchase).
    meta: u32,
    category: u32,
}

const VALID: u32 = 1 << 31;

/// One valid click, before dedup.
#[derive(Clone, Copy)]
struct Click {
    id: IdRef,
    user: u32,
    ts: u32,
    campaign: u32,
}

/// A surviving event, keyed for the per-person sort: person in the high half, time in the low.
#[derive(Clone, Copy)]
struct PersonEvent {
    key: u64,
    price: u32,
    category: u32,
}

#[derive(Clone, Copy)]
struct PersonClick {
    key: u64,
    id: IdRef,
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
    users: Interner,
    event_bits: u32,
}

#[derive(Clone, Copy)]
enum Kind {
    Events,
    Clicks,
    Links,
}

struct Parsed {
    /// Events by partition of their ID's hash.
    events: Vec<Vec<Event>>,
    clicks: Vec<Click>,
    links: Vec<(u32, u32)>,
}

#[inline]
fn partition_of_hash(hash: u64, bits: u32) -> usize {
    // Middle bits: the hash table uses the low bits for its slot and the top 7 as a tag.
    ((hash >> 24) & ((1u64 << bits) - 1)) as usize
}

#[inline]
fn partition_of_person(person: u32, bits: u32) -> usize {
    if bits == 0 { 0 } else { (person.wrapping_mul(0x9E37_79B1) >> (32 - bits)) as usize }
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

#[inline]
fn parse_event(line: Str, ctx: &Ctx, out: &mut Parsed) {
    // Events step 1: malformed lines and empty event IDs are gone before dedup.
    let Some([id, user, ts, ingest, kind, product, price]) = csv::split::<7>(line) else { return };
    if id.is_empty() {
        return;
    }
    // The rules promise a valid F1 ingest_ts; a copy without one cannot be ordered, so it is dropped.
    let Some(ingest) = ts::parse(ingest) else { return };
    let mut event = Event { hash: table::hash(id), id: IdRef::new(id), ingest, ts: 0, user: 0, meta: 0, category: 0 };
    // Steps 3 and 4, decided now but applied only to the copy that survives dedup.
    'valid: {
        if user.is_empty() || product.is_empty() {
            break 'valid;
        }
        let Some(t) = ts::parse(ts) else { break 'valid };
        if t < ctx.window_start || t >= ctx.window_end || ingest - t > DAY_MS {
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
        event.user = ctx.users.intern(user);
        event.meta = VALID | cents;
    }
    out.events[partition_of_hash(event.hash, ctx.event_bits)].push(event);
}

#[inline]
fn parse_click(line: Str, ctx: &Ctx, out: &mut Parsed) {
    let Some([id, user, campaign, ts]) = csv::split::<4>(line) else { return };
    if id.is_empty() || user.is_empty() {
        return;
    }
    let Some(campaign) = ctx.campaigns.get(campaign) else { return };
    let Some(t) = ts::parse(ts) else { return };
    if t < ctx.base || t >= ctx.window_end {
        return;
    }
    out.clicks.push(Click { id: IdRef::new(id), user: ctx.users.intern(user), ts: (t - ctx.base) as u32, campaign });
}

#[inline]
fn parse_link(line: Str, ctx: &Ctx, out: &mut Parsed) {
    let Some([a, b]) = csv::split::<2>(line) else { return };
    if a.is_empty() || b.is_empty() {
        return;
    }
    out.links.push((ctx.users.intern(a), ctx.users.intern(b)));
}

/// Maps a file for the whole run. Its bytes are referenced until exit, so it is never unmapped.
fn map_file(path: &Path) -> Res<Str> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() == 0 {
        return Ok(b"");
    }
    // SAFETY: /data is read-only and nothing changes the files during a run.
    let map = unsafe { memmap2::Mmap::map(&file)? };
    #[cfg(unix)]
    let _ = map.advise(memmap2::Advice::Sequential);
    Ok(&Box::leak(Box::new(map))[..])
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

/// The timestamp stored under `key` in manifest.json.
fn manifest_time(text: &[u8], key: &str) -> Res<i64> {
    let needle = format!("\"{key}\"");
    let at = memchr::memmem::find(text, needle.as_bytes()).ok_or("manifest: key missing")?;
    let rest = &text[at + needle.len()..];
    let open = memchr::memchr(b'"', rest).ok_or("manifest: value missing")?;
    let close = memchr::memchr(b'"', &rest[open + 1..]).ok_or("manifest: value unterminated")?;
    ts::parse(&rest[open + 1..open + 1 + close]).ok_or_else(|| "manifest: bad timestamp".into())
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
    for (product, category) in pairs(map_file(&data_dir.join("products.csv"))?) {
        let category = category_id(category);
        products.get_or_insert(product, category);
    }
    let unknown_category = category_id(b"unknown");

    let mut campaigns = StrMap::default();
    let mut campaign_names: Vec<Str> = Vec::new();
    let mut channels: Vec<Str> = Vec::new();
    for (campaign, channel) in pairs(map_file(&data_dir.join("campaigns.csv"))?) {
        let id = campaigns.get_or_insert(campaign, campaign_names.len() as u32);
        if id as usize == campaign_names.len() {
            campaign_names.push(campaign);
            channels.push(channel);
        }
    }
    let organic = campaign_names.len();
    campaign_names.push(b"organic");
    channels.push(b"organic");

    let mut chunks: Vec<(Kind, Str, usize, usize)> = Vec::new();
    let mut event_bytes = 0;
    for (kind, folder) in [(Kind::Events, "events"), (Kind::Clicks, "clicks"), (Kind::Links, "links")] {
        for path in csv_files(&data_dir.join(folder))? {
            let data = map_file(&path)?;
            if matches!(kind, Kind::Events) {
                event_bytes += data.len();
            }
            let mut start = 0;
            while start < data.len() {
                let end = (start + CHUNK).min(data.len());
                chunks.push((kind, data, start, end));
                start = end;
            }
        }
    }
    let rough_events = event_bytes / 64;
    let event_bits = bits_for(rough_events, 1 << 16, 11);
    let person_bits = bits_for(rough_events, 1 << 17, 10);
    let ctx = Ctx {
        window_start,
        window_end,
        base,
        products,
        unknown_category,
        campaigns,
        users: Interner::new(),
        event_bits,
    };
    log("open");

    // ---- 1. Parse ------------------------------------------------------------------------
    let parsed = par::run(
        threads,
        chunks.len(),
        |_| Parsed { events: (0..1 << event_bits).map(|_| Vec::new()).collect(), clicks: Vec::new(), links: Vec::new() },
        |out, i| {
            let (kind, data, start, end) = chunks[i];
            match kind {
                Kind::Events => csv::for_each_line(data, start, end, |line| parse_event(line, &ctx, out)),
                Kind::Clicks => csv::for_each_line(data, start, end, |line| parse_click(line, &ctx, out)),
                Kind::Links => csv::for_each_line(data, start, end, |line| parse_link(line, &ctx, out)),
            }
        },
    );
    log("parse");

    // ---- 2. Linked IDs -------------------------------------------------------------------
    let names = ctx.users.into_names();
    let mut parent: Vec<u32> = (0..names.len() as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up;
            x = up;
        }
        x
    }
    for thread in &parsed {
        for &(a, b) in &thread.links {
            let (a, b) = (find(&mut parent, a), find(&mut parent, b));
            if a != b {
                parent[a.max(b) as usize] = a.min(b);
            }
        }
    }
    // Each ID's person, and each person's name: the byte-wise smallest of their IDs.
    let mut person_of = vec![0u32; names.len()];
    let mut person_name = names.clone();
    for id in 0..names.len() {
        let root = find(&mut parent, id as u32) as usize;
        person_of[id] = root as u32;
        if names[id] < person_name[root] {
            person_name[root] = names[id];
        }
    }
    drop(parent);
    log("link");

    // ---- 3. Dedup events, then scatter survivors and clicks by person ----------------------
    let person_parts = 1usize << person_bits;
    struct Dedup<'a> {
        seen: HashTable<&'a Event>,
        out: Vec<Vec<PersonEvent>>,
    }
    let deduped = par::run(
        threads,
        1 << event_bits,
        |_| Dedup { seen: HashTable::new(), out: (0..person_parts).map(|_| Vec::new()).collect() },
        |state, part| {
            state.seen.clear();
            state.seen.reserve(parsed.iter().map(|t| t.events[part].len()).sum(), |e| e.hash);
            for thread in &parsed {
                for event in &thread.events[part] {
                    // Events step 2: one copy per event_id, the one ingested first.
                    match state.seen.find_mut(event.hash, |o| o.hash == event.hash && o.id.bytes() == event.id.bytes()) {
                        Some(kept) => {
                            if event.ingest < kept.ingest {
                                *kept = event;
                            }
                        }
                        None => {
                            state.seen.insert_unique(event.hash, event, |e| e.hash);
                        }
                    }
                }
            }
            for event in state.seen.iter() {
                if event.meta & VALID != 0 {
                    let person = person_of[event.user as usize];
                    state.out[partition_of_person(person, person_bits)].push(PersonEvent {
                        key: (person as u64) << 32 | event.ts as u64,
                        price: event.meta & !VALID,
                        category: event.category,
                    });
                }
            }
        },
    );
    let person_events: Vec<Vec<Vec<PersonEvent>>> = deduped.into_iter().map(|d| d.out).collect();
    let person_clicks = par::run(
        threads,
        parsed.len(),
        |_| (0..person_parts).map(|_| Vec::new()).collect::<Vec<Vec<PersonClick>>>(),
        |out, t| {
            for click in &parsed[t].clicks {
                let person = person_of[click.user as usize];
                out[partition_of_person(person, person_bits)].push(PersonClick {
                    key: (person as u64) << 32 | click.ts as u64,
                    id: click.id,
                    campaign: click.campaign,
                });
            }
        },
    );
    drop(parsed);
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
        person_parts,
        |_| Sessions {
            rows: output::UserRows::default(),
            campaign_days: vec![[0; 4]; campaign_count * days],
            events: Vec::new(),
            clicks: Vec::new(),
            in_window: vec![0; campaign_count],
            slot: vec![0; campaign_count],
            active: Vec::new(),
            day_categories: Vec::new(),
        },
        |s, part| {
            s.events.clear();
            for thread in &person_events {
                s.events.extend_from_slice(&thread[part]);
            }
            if s.events.is_empty() {
                return;
            }
            // Events step 5: each person's events in time order.
            s.events.sort_unstable_by_key(|e| e.key);

            // Clicks step 2 happens here: copies of a click are identical, so they share a
            // person and a time and end up next to each other.
            s.clicks.clear();
            for thread in &person_clicks {
                s.clicks.extend_from_slice(&thread[part]);
            }
            // Within one millisecond the smallest click_id goes last, so "the last click at or
            // before the session start" is the one the tie rule picks.
            s.clicks.sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| b.id.bytes().cmp(a.id.bytes())));
            s.clicks.dedup_by(|b, a| a.key == b.key && a.id.bytes() == b.id.bytes());

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
                    s.rows.user_id.push(person_name[person as usize]);
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
