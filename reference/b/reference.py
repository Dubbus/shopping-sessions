#!/usr/bin/env python3
"""
Reference implementation B of the Shopping Sessions pipeline (docs/rules.md).

Written to be obviously correct, not fast. Building blocks, on purpose:
  * one strict regular expression per line shape, over bytes (CSV dialect);
  * strict regular expressions + datetime/calendar for timestamps;
  * breadth-first search over an adjacency map for linked IDs;
  * sort + itertools.groupby + bisect for sessions, credit, assists, aggregation.

All values stay `bytes` until the Parquet files are written, so every string
comparison (IDs, click_id ties, category ties, person names) is byte-wise.

Usage: python reference.py [DATA_DIR OUTPUT_DIR]   (else env DATA_DIR / OUTPUT_DIR)
"""
import bisect
import calendar
import collections
import datetime
import itertools
import json
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SESSION_GAP_MS = 30 * 60 * 1000          # 1,800,000
LATE_MS = 24 * 60 * 60 * 1000            # 24 hours
LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000    # 604,800,000
DAY_MS = 24 * 60 * 60 * 1000
MAX_PRICE = 10_000_000
EVENT_TYPES = (b"view", b"cart", b"purchase")
ORGANIC = b"organic"
UNKNOWN = b"unknown"


def log(*a):
    print(*a, file=sys.stderr)


# --------------------------------------------------------------------------
# CSV dialect
# --------------------------------------------------------------------------
# A field is either  "…"  (quotes are syntax) or bare; in both cases the value
# holds no comma and no quote.  Every comma separates fields.  Anything else
# (wrong field count, a quote not around a whole field, an empty line when more
# than one field is expected) fails the fullmatch and the line is malformed.
_FIELD = rb'(?:"([^",]*)"|([^",]*))'


def line_regex(n_fields):
    return re.compile(rb",".join([_FIELD] * n_fields))


def split_lines(data):
    """Only \\n ends a line.  The piece after a final \\n is not a line."""
    lines = data.split(b"\n")
    if lines[-1] == b"":
        lines.pop()
    return lines


def read_rows(path, n_fields):
    """Yield tuples of n_fields byte values for each well-formed data line."""
    rx = line_regex(n_fields)
    with open(path, "rb") as f:
        lines = split_lines(f.read())
    for line in lines[1:]:                       # first line is the header
        if line == b"":
            continue                             # an empty line is malformed
        m = rx.fullmatch(line)
        if m is None:
            continue                             # malformed
        g = m.groups()
        # for each field exactly one of the two groups took part in the match
        yield tuple(g[2 * i] if g[2 * i] is not None else g[2 * i + 1]
                    for i in range(n_fields))


def csv_files(folder):
    if not os.path.isdir(folder):
        return []
    return [os.path.join(folder, n) for n in sorted(os.listdir(folder))
            if n.endswith(".csv") and os.path.isfile(os.path.join(folder, n))]


# --------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------
_DT = rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
RX_F1 = re.compile(_DT + rb"Z")
RX_F2 = re.compile(_DT + rb"([+-])([0-9]{2}):([0-9]{2})")
RX_F3 = re.compile(rb"[0-9]+")
RX_DIGITS = RX_F3


def _civil_ms(parts):
    """Epoch ms of a UTC wall-clock time, or None if the calendar value is impossible."""
    y, mo, d, h, mi, s = (int(p) for p in parts)
    try:
        dt = datetime.datetime(y, mo, d, h, mi, s)   # rejects month 13, Feb 30, hour 24, second 60 ...
    except ValueError:
        return None
    return calendar.timegm(dt.timetuple()) * 1000


def parse_f1(v):
    m = RX_F1.fullmatch(v)
    return _civil_ms(m.groups()) if m else None


def parse_ts(v):
    """bytes -> epoch ms (int), or None for a bad timestamp."""
    m = RX_F1.fullmatch(v)
    if m:
        return _civil_ms(m.groups())
    m = RX_F2.fullmatch(v)
    if m:
        g = m.groups()
        base = _civil_ms(g[:6])
        oh, om = int(g[7]), int(g[8])
        if base is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60 * 1000
        return base - off if g[6] == b"+" else base + off
    if RX_F3.fullmatch(v):
        return int(v)                                # arbitrary precision: never overflows
    return None


def utc_day(ms):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=ms // DAY_MS)


# --------------------------------------------------------------------------
# Linked IDs: BFS over an adjacency map
# --------------------------------------------------------------------------
def build_person_names(data_dir):
    adj = collections.defaultdict(set)
    for path in csv_files(os.path.join(data_dir, "links")):
        for a, b in read_rows(path, 2):
            if a == b"" or b == b"":
                continue
            adj[a].add(b)
            adj[b].add(a)
    name = {}
    for root in adj:
        if root in name:
            continue
        seen = {root}
        queue = collections.deque([root])
        while queue:
            cur = queue.popleft()
            for nxt in adj[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        smallest = min(seen)                         # bytes: byte-wise
        for uid in seen:
            name[uid] = smallest
    return name


# --------------------------------------------------------------------------
# Lookup files
# --------------------------------------------------------------------------
def read_lookup(path):
    out = {}
    if os.path.isfile(path):
        for k, v in read_rows(path, 2):
            out.setdefault(k, v)
    return out


# --------------------------------------------------------------------------
# Events
# --------------------------------------------------------------------------
def load_events(data_dir, w_start, w_end, person_of, category_of):
    # 1 parse, 2 dedup on event_id by smallest ingest_ts (first seen wins ties)
    kept = {}
    bad_ingest = 0
    for path in csv_files(os.path.join(data_dir, "events")):
        for row in read_rows(path, 7):
            event_id = row[0]
            if event_id == b"":
                continue
            ingest = parse_f1(row[3])
            if ingest is None:
                # The rules promise ingest_ts is always valid F1; a copy that
                # breaks the promise cannot be ordered, so it is not kept.
                bad_ingest += 1
                continue
            cur = kept.get(event_id)
            if cur is None or ingest < cur[0]:
                kept[event_id] = (ingest, row)
    if bad_ingest:
        log(f"WARNING: {bad_ingest} event lines with an invalid ingest_ts were dropped")

    # 3 validate the kept copy, 4 drop late; result: (person, ts, price, category)
    out = []
    for ingest, (_eid, user_id, ts_raw, _ing, etype, product_id, price_raw) in kept.values():
        if user_id == b"":
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (w_start <= ts < w_end):
            continue
        if etype not in EVENT_TYPES:
            continue
        if product_id == b"":
            continue
        price = None                                 # None = not a purchase
        if etype == b"purchase":
            if not RX_DIGITS.fullmatch(price_raw):
                continue
            price = int(price_raw)
            if not (1 <= price <= MAX_PRICE):
                continue
        if ingest - ts > LATE_MS:
            continue
        out.append((person_of(user_id), ts, price, category_of.get(product_id, UNKNOWN)))
    return out


# --------------------------------------------------------------------------
# Clicks
# --------------------------------------------------------------------------
def load_clicks(data_dir, w_start, w_end, person_of, channel_of):
    kept = {}
    for path in csv_files(os.path.join(data_dir, "clicks")):
        for row in read_rows(path, 4):
            if row[0] == b"":
                continue
            kept.setdefault(row[0], row)             # copies are identical: keep the first
    by_person = collections.defaultdict(list)
    for click_id, user_id, campaign_id, ts_raw in kept.values():
        if user_id == b"":
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (w_start - LOOKBACK_MS <= ts < w_end):
            continue
        if campaign_id not in channel_of:
            continue
        by_person[person_of(user_id)].append((ts, click_id, campaign_id))
    for lst in by_person.values():
        lst.sort()                                   # by ts, then click_id byte-wise
    return by_person


# --------------------------------------------------------------------------
# Sessions
# --------------------------------------------------------------------------
Session = collections.namedtuple(
    "Session", "person start day n_events revenue has_purchase cat_revenue")


def sessionize(events):
    """events: (person, ts, price, category).  Returns a list of Session."""
    sessions = []
    events = sorted(events, key=lambda e: (e[0], e[1]))
    for person, grp in itertools.groupby(events, key=lambda e: e[0]):
        evs = list(grp)
        times = [e[1] for e in evs]
        # session number of each event: +1 whenever the gap to the previous
        # event is over 30 minutes (equal timestamps have gap 0, so they share)
        breaks = [0] + [1 if b - a > SESSION_GAP_MS else 0 for a, b in zip(times, times[1:])]
        numbers = itertools.accumulate(breaks)
        for _n, sgrp in itertools.groupby(zip(numbers, evs), key=lambda p: p[0]):
            members = [e for _, e in sgrp]
            start = min(e[1] for e in members)
            purchases = [(e[3], e[2]) for e in members if e[2] is not None]
            cat_rev = collections.Counter()
            for cat, price in purchases:
                cat_rev[cat] += price
            sessions.append(Session(person, start, utc_day(start), len(members),
                                    sum(p for _, p in purchases), bool(purchases), cat_rev))
    return sessions


def attribute(session, clicks):
    """clicks: the person's valid clicks sorted by (ts, click_id).
    Returns (credited campaign, set of assisting campaigns)."""
    times = [c[0] for c in clicks]
    hi = bisect.bisect_right(times, session.start)               # clicks[:hi] have ts <= start
    lo = bisect.bisect_left(times, session.start - LOOKBACK_MS)  # clicks[lo:] have ts >= start - 7d
    qualifying = clicks[lo:hi]
    if not qualifying:
        return ORGANIC, set()
    latest = qualifying[-1][0]
    first_of_latest = bisect.bisect_left(times, latest)          # smallest click_id at that ts
    return clicks[first_of_latest][2], {c[2] for c in qualifying}


# --------------------------------------------------------------------------
# Aggregation and output
# --------------------------------------------------------------------------
def build_tables(sessions, clicks_by_person, channel_of):
    # user_daily
    user_rows = []
    keyf = lambda s: (s.person, s.day)
    for (person, day), grp in itertools.groupby(sorted(sessions, key=keyf), key=keyf):
        grp = list(grp)
        cat_rev = collections.Counter()
        for s in grp:
            cat_rev.update(s.cat_revenue)
        top = None
        if cat_rev:
            best = max(cat_rev.values())
            top = min(c for c, v in cat_rev.items() if v == best)   # byte-wise smallest
        user_rows.append((person, day, len(grp), sum(s.n_events for s in grp),
                          sum(s.revenue for s in grp), top))

    # campaign_daily: one fact per (campaign, day, kind)
    facts = []   # (campaign, day, credited sessions, purchasing, revenue, assisted)
    for s in sessions:
        clicks = clicks_by_person.get(s.person, [])
        credited, assists = attribute(s, clicks)
        facts.append((credited, s.day, 1, 1 if s.has_purchase else 0, s.revenue, 0))
        for camp in assists:
            facts.append((camp, s.day, 0, 0, 0, 1))
    camp_rows = []
    keyf = lambda f: (f[0], f[1])
    for (camp, day), grp in itertools.groupby(sorted(facts, key=keyf), key=keyf):
        grp = list(grp)
        channel = ORGANIC if camp == ORGANIC else channel_of[camp]
        camp_rows.append((camp, channel, day, sum(f[2] for f in grp), sum(f[3] for f in grp),
                          sum(f[4] for f in grp), sum(f[5] for f in grp)))
    return user_rows, camp_rows


def text(b):
    # Input is specified as UTF-8; anything else is replaced rather than crashing.
    return None if b is None else b.decode("utf-8", errors="replace")


def write_table(folder, schema, columns):
    os.makedirs(folder, exist_ok=True)
    table = pa.table(columns, schema=schema)
    pq.write_table(table, os.path.join(folder, "part-0.parquet"))


def main():
    if len(sys.argv) >= 3:
        data_dir, out_dir = sys.argv[1], sys.argv[2]
    else:
        data_dir = os.environ.get("DATA_DIR", "/data")
        out_dir = os.environ.get("OUTPUT_DIR", "/output")

    with open(os.path.join(data_dir, "manifest.json"), "rb") as f:
        manifest = json.load(f)
    w_start = parse_ts(manifest["window_start"].encode("utf-8"))
    w_end = parse_ts(manifest["window_end"].encode("utf-8"))
    if w_start is None or w_end is None:
        sys.exit("manifest.json: unreadable window")

    name = build_person_names(data_dir)
    person_of = lambda uid: name.get(uid, uid)       # unlinked ID: a person on its own
    category_of = read_lookup(os.path.join(data_dir, "products.csv"))
    channel_of = read_lookup(os.path.join(data_dir, "campaigns.csv"))

    events = load_events(data_dir, w_start, w_end, person_of, category_of)
    clicks_by_person = load_clicks(data_dir, w_start, w_end, person_of, channel_of)
    sessions = sessionize(events)
    user_rows, camp_rows = build_tables(sessions, clicks_by_person, channel_of)

    write_table(
        os.path.join(out_dir, "user_daily"),
        pa.schema([("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
                   ("events", pa.int64()), ("revenue_cents", pa.int64()),
                   ("top_category", pa.string())]),
        {"user_id": [text(r[0]) for r in user_rows], "day": [r[1] for r in user_rows],
         "sessions": [r[2] for r in user_rows], "events": [r[3] for r in user_rows],
         "revenue_cents": [r[4] for r in user_rows],
         "top_category": [text(r[5]) for r in user_rows]})
    write_table(
        os.path.join(out_dir, "campaign_daily"),
        pa.schema([("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
                   ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
                   ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64())]),
        {"campaign_id": [text(r[0]) for r in camp_rows], "channel": [text(r[1]) for r in camp_rows],
         "day": [r[2] for r in camp_rows], "sessions": [r[3] for r in camp_rows],
         "purchasing_sessions": [r[4] for r in camp_rows],
         "revenue_cents": [r[5] for r in camp_rows],
         "assisted_sessions": [r[6] for r in camp_rows]})
    log(f"events kept {len(events)}, sessions {len(sessions)}, "
        f"user_daily {len(user_rows)}, campaign_daily {len(camp_rows)}")


if __name__ == "__main__":
    main()
