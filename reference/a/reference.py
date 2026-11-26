#!/usr/bin/env python3
"""Reference implementation A of the "Shopping Sessions" pipeline (docs/rules.md).

Deliberately slow and plain: record-at-a-time, bytes everywhere, the standard
library for all logic.  pyarrow is imported only at the very end to write the
two Parquet tables.  Every function names the rule in rules.md it implements.

Usage: python reference.py [DATA_DIR OUTPUT_DIR]   (else env DATA_DIR / OUTPUT_DIR)
"""
import bisect
import json
import os
import sys

MS_PER_SECOND = 1000
MS_PER_MINUTE = 60 * MS_PER_SECOND
MS_PER_HOUR = 60 * MS_PER_MINUTE
MS_PER_DAY = 24 * MS_PER_HOUR

SESSION_GAP_MS = 30 * MS_PER_MINUTE      # Events 5: 1,800,000 ms
LATE_LIMIT_MS = 24 * MS_PER_HOUR         # Events 4
CLICK_LOOKBACK_MS = 7 * MS_PER_DAY       # Clicks 4: 604,800,000 ms
MAX_PRICE_CENTS = 10_000_000             # Events 3

EVENT_FIELDS = 7   # event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
CLICK_FIELDS = 4   # click_id,user_id,campaign_id,ts
LINK_FIELDS = 2    # user_id_a,user_id_b
LOOKUP_FIELDS = 2  # product_id,category / campaign_id,channel

ORGANIC = b"organic"   # Clicks 5
UNKNOWN = b"unknown"   # Events 7
EVENT_TYPES = (b"view", b"cart", b"purchase")  # Events 3
PURCHASE = b"purchase"

QUOTE = 0x22  # '"'
ASCII_0 = 0x30
ASCII_9 = 0x39


# --------------------------------------------------------------------------
# CSV dialect
# --------------------------------------------------------------------------

def read_data_lines(path):
    """CSV dialect: only b"\\n" ends a line; the first line is the header.

    Returns the lines after the header, as bytes, without their b"\\n".
    A b"\\r" is left where it is: it is an ordinary character.
    """
    with open(path, "rb") as handle:
        data = handle.read()
    lines = data.split(b"\n")
    # split() yields one extra piece after the last b"\n".  If the file ends
    # with b"\n" that piece is empty and is not a line at all; if the file
    # does not end with b"\n" the piece is a real (unterminated) last line.
    if lines[-1] == b"":
        lines.pop()
    # "Every CSV file has a header row": line 1 is skipped whatever it holds.
    # Later lines are data even when they equal the header (fixture L3).
    return lines[1:]


def unquote_field(raw):
    """CSV dialect: a field may be wrapped in double quotes, which are syntax.

    Returns the value as bytes, or None if a quote appears anywhere except
    around the whole field (which makes the line malformed).
    """
    if QUOTE not in raw:
        return raw
    # There is at least one quote: the only legal shape is "<no quotes>".
    if len(raw) >= 2 and raw[0] == QUOTE and raw[-1] == QUOTE:
        inner = raw[1:-1]
        if QUOTE not in inner:
            return inner
    return None


def parse_line(line, field_count):
    """CSV dialect: split one line into exactly `field_count` values.

    Returns a list of bytes values (b"" means null), or None when the line is
    malformed: empty line, wrong number of fields, or a misplaced quote.
    Every comma separates fields (no value ever holds one, quoted or not).
    Spaces and b"\\r" are kept: they are part of the value.
    """
    if line == b"":
        return None  # "An empty line is malformed too."
    raw_fields = line.split(b",")
    if len(raw_fields) != field_count:
        return None
    values = []
    for raw in raw_fields:
        value = unquote_field(raw)
        if value is None:
            return None
        values.append(value)
    return values


def list_csv_files(folder):
    """Input files: every `*.csv` directly inside `folder`, in name order.

    Shell-glob meaning of `*.csv`: the name ends in ".csv" and does not start
    with a dot.  Order does not affect the result; sorting makes runs repeatable.
    """
    names = []
    for name in os.listdir(folder):
        if name.endswith(".csv") and not name.startswith("."):
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                names.append(path)
    names.sort()
    return names


# --------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------

def is_ascii_digits(value):
    """CSV dialect: "digits" means the ASCII bytes 0x30..0x39, at least one."""
    if len(value) == 0:
        return False
    for byte in value:
        if byte < ASCII_0 or byte > ASCII_9:
            return False
    return True


def is_leap_year(year):
    return (year % 4 == 0 and year % 100 != 0) or year % 400 == 0


def days_in_month(year, month):
    if month == 2:
        return 29 if is_leap_year(year) else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


def leap_years_through(year):
    """Number of Gregorian leap years in 1..year (proleptic)."""
    return year // 4 - year // 100 + year // 400


def days_from_epoch(year, month, day):
    """Days from 1970-01-01 to the given (already validated) calendar date."""
    days = 365 * (year - 1970) + leap_years_through(year - 1) - leap_years_through(1969)
    for earlier_month in range(1, month):
        days += days_in_month(year, earlier_month)
    return days + (day - 1)


def parse_calendar_prefix(value):
    """Timestamps F1/F2: the first 19 bytes, `YYYY-MM-DDTHH:MM:SS`.

    Returns UTC-naive epoch milliseconds of that wall-clock reading, or None
    if the shape is wrong or the calendar value is impossible.
    """
    if len(value) < 19:
        return None
    if not (is_ascii_digits(value[0:4]) and is_ascii_digits(value[5:7])
            and is_ascii_digits(value[8:10]) and is_ascii_digits(value[11:13])
            and is_ascii_digits(value[14:16]) and is_ascii_digits(value[17:19])):
        return None
    if value[4:5] != b"-" or value[7:8] != b"-":
        return None
    if value[10:11] != b"T":          # lowercase t or a space is bad
        return None
    if value[13:14] != b":" or value[16:17] != b":":
        return None
    year = int(value[0:4])
    month = int(value[5:7])
    day = int(value[8:10])
    hour = int(value[11:13])
    minute = int(value[14:16])
    second = int(value[17:19])
    if month < 1 or month > 12:       # month 13
        return None
    if day < 1 or day > days_in_month(year, month):  # Feb 30, Feb 29 non-leap
        return None
    if hour > 23 or minute > 59 or second > 59:      # hour 24, second 60
        return None
    days = days_from_epoch(year, month, day)
    return (days * MS_PER_DAY + hour * MS_PER_HOUR
            + minute * MS_PER_MINUTE + second * MS_PER_SECOND)


def parse_f1(value):
    """Timestamps F1: exactly `YYYY-MM-DDTHH:MM:SSZ` (20 bytes, uppercase Z)."""
    if len(value) != 20 or value[19:20] != b"Z":
        return None
    return parse_calendar_prefix(value)


def parse_f2(value):
    """Timestamps F2: `YYYY-MM-DDTHH:MM:SS` then `+HH:MM` or `-HH:MM` (25 bytes).

    Offset hours 00..23, minutes 00..59, colon required.  Subtract the offset
    to get UTC.  `-00:00` is UTC.
    """
    if len(value) != 25:
        return None
    sign = value[19:20]
    if sign != b"+" and sign != b"-":
        return None
    if not (is_ascii_digits(value[20:22]) and is_ascii_digits(value[23:25])):
        return None
    if value[22:23] != b":":
        return None
    offset_hours = int(value[20:22])
    offset_minutes = int(value[23:25])
    if offset_hours > 23 or offset_minutes > 59:
        return None
    local_ms = parse_calendar_prefix(value)
    if local_ms is None:
        return None
    offset_ms = offset_hours * MS_PER_HOUR + offset_minutes * MS_PER_MINUTE
    if sign == b"+":
        return local_ms - offset_ms
    return local_ms + offset_ms


def parse_ts(value):
    """Timestamps: F1, F2 or F3 -> epoch milliseconds UTC; None if bad.

    F3 is ASCII digits only (leading zeros allowed, any length): a plain
    number.  Python integers do not overflow, so a very long F3 simply ends
    up outside every window.
    """
    if is_ascii_digits(value):
        return int(value)
    if len(value) == 20:
        return parse_f1(value)
    if len(value) == 25:
        return parse_f2(value)
    return None  # empty, fractional seconds, no zone, +0200, ...


# --------------------------------------------------------------------------
# Small inputs: manifest, products, campaigns
# --------------------------------------------------------------------------

def read_manifest(data_dir):
    """Input files: manifest.json gives window_start and window_end."""
    with open(os.path.join(data_dir, "manifest.json"), "rb") as handle:
        manifest = json.loads(handle.read())
    window_start = parse_ts(manifest["window_start"].encode("utf-8"))
    window_end = parse_ts(manifest["window_end"].encode("utf-8"))
    if window_start is None or window_end is None:
        raise ValueError("manifest.json: unreadable window")
    return window_start, window_end


def read_lookup(path):
    """products.csv / campaigns.csv: key -> value (both files are clean)."""
    table = {}
    for line in read_data_lines(path):
        fields = parse_line(line, LOOKUP_FIELDS)
        if fields is None:
            continue
        key, value = fields
        if key not in table:
            table[key] = value
    return table


# --------------------------------------------------------------------------
# Linked IDs
# --------------------------------------------------------------------------

class UnionFind:
    """Linked IDs 2: transitive joining of IDs (bytes) into persons."""

    def __init__(self):
        self.parent = {}

    def add(self, item):
        if item not in self.parent:
            self.parent[item] = item

    def find(self, item):
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression (iterative: chains can be hundreds of links long).
        while self.parent[item] != root:
            next_item = self.parent[item]
            self.parent[item] = root
            item = next_item
        return root

    def union(self, a, b):
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a


def read_person_names(data_dir):
    """Linked IDs 1-3: map every linked ID to its person's name.

    The name is the byte-wise smallest ID of the person (Python compares
    bytes objects byte by byte, shorter-prefix first).  IDs that are in no
    kept link line are absent from the map: they name themselves.
    """
    union_find = UnionFind()
    for path in list_csv_files(os.path.join(data_dir, "links")):
        for line in read_data_lines(path):
            fields = parse_line(line, LINK_FIELDS)      # Linked IDs 1
            if fields is None:
                continue
            id_a, id_b = fields
            if id_a == b"" or id_b == b"":              # Linked IDs 1
                continue
            union_find.add(id_a)                        # Linked IDs 2
            union_find.add(id_b)
            union_find.union(id_a, id_b)

    smallest_of_root = {}                               # Linked IDs 3
    for user_id in union_find.parent:
        root = union_find.find(user_id)
        if root not in smallest_of_root or user_id < smallest_of_root[root]:
            smallest_of_root[root] = user_id
    name_of = {}
    for user_id in union_find.parent:
        name_of[user_id] = smallest_of_root[union_find.find(user_id)]
    return name_of


def person_of(user_id, name_of):
    """Linked IDs 3: an ID in no link line is a person on its own."""
    return name_of.get(user_id, user_id)


# --------------------------------------------------------------------------
# Events
# --------------------------------------------------------------------------

def read_deduped_events(data_dir):
    """Events 1-2: parse, then keep per event_id the copy with the smallest ingest_ts.

    Returns event_id -> (ingest_ms, fields).  Nothing but the line shape, the
    event_id and ingest_ts is looked at here: dedup comes before validation.
    """
    kept = {}
    for path in list_csv_files(os.path.join(data_dir, "events")):
        for line in read_data_lines(path):
            fields = parse_line(line, EVENT_FIELDS)     # Events 1
            if fields is None:
                continue
            event_id = fields[0]
            if event_id == b"":                         # Events 1
                continue
            # "ingest_ts is always valid F1"; refuse to guess if it is not.
            ingest_ms = parse_f1(fields[3])
            if ingest_ms is None:
                raise ValueError("%s: ingest_ts is not valid F1: %r" % (path, line))
            previous = kept.get(event_id)               # Events 2
            if previous is None or ingest_ms < previous[0]:
                kept[event_id] = (ingest_ms, fields)
            # Equal ingest_ts: the copies are identical, keep the first seen.
    return kept


def parse_price(value):
    """Events 3: purchase price, ASCII digits only, value 1..10,000,000."""
    if not is_ascii_digits(value):
        return None
    price = int(value)          # leading zeros are fine: 0250 is 250
    if price < 1 or price > MAX_PRICE_CENTS:
        return None
    return price


def validate_event(ingest_ms, fields, window_start, window_end):
    """Events 3-4: returns (user_id, ts_ms, product_id, price) or None.

    `price` is the purchase price in cents, or None for a view or cart.
    """
    _event_id, user_id, ts, _ingest_ts, event_type, product_id, price_cents = fields
    if user_id == b"":                                  # Events 3
        return None
    ts_ms = parse_ts(ts)
    if ts_ms is None:
        return None
    if not (window_start <= ts_ms < window_end):        # Timestamps: event window
        return None
    if event_type not in EVENT_TYPES:
        return None
    if product_id == b"":
        return None
    price = None
    if event_type == PURCHASE:
        price = parse_price(price_cents)
        if price is None:
            return None
    if ingest_ms - ts_ms > LATE_LIMIT_MS:               # Events 4: strictly over 24 h
        return None
    return user_id, ts_ms, product_id, price


def surviving_events_by_person(data_dir, name_of, window_start, window_end):
    """Events 1-4 and Linked IDs: person name -> list of (ts_ms, product_id, price)."""
    events_of = {}
    for ingest_ms, fields in read_deduped_events(data_dir).values():
        valid = validate_event(ingest_ms, fields, window_start, window_end)
        if valid is None:
            continue
        user_id, ts_ms, product_id, price = valid
        person = person_of(user_id, name_of)
        events_of.setdefault(person, []).append((ts_ms, product_id, price))
    return events_of


def sessionize(events):
    """Events 5: split one person's events into sessions.

    Sorted by ts; a new session starts when the gap from the previous event is
    over 30 minutes.  Equal timestamps have gap 0, so they always share a
    session, whatever their relative order.  Returns a list of sessions, each
    a list of (ts_ms, product_id, price) whose first element is the start.
    """
    ordered = sorted(events, key=lambda event: event[0])
    sessions = []
    previous_ts = None
    for event in ordered:
        if previous_ts is None or event[0] - previous_ts > SESSION_GAP_MS:
            sessions.append([])
        sessions[-1].append(event)
        previous_ts = event[0]
    return sessions


# --------------------------------------------------------------------------
# Clicks
# --------------------------------------------------------------------------

def valid_clicks_by_person(data_dir, name_of, campaigns, window_start, window_end):
    """Clicks 1-3 and Linked IDs: person name -> list of (ts_ms, click_id, campaign_id)."""
    first_copy = {}
    for path in list_csv_files(os.path.join(data_dir, "clicks")):
        for line in read_data_lines(path):
            fields = parse_line(line, CLICK_FIELDS)     # Clicks 1
            if fields is None:
                continue
            click_id = fields[0]
            if click_id == b"":                         # Clicks 1
                continue
            if click_id not in first_copy:              # Clicks 2: keep any one
                first_copy[click_id] = fields

    click_window_start = window_start - CLICK_LOOKBACK_MS   # Timestamps: click window
    clicks_of = {}
    for click_id, user_id, campaign_id, ts in first_copy.values():
        if user_id == b"":                              # Clicks 3
            continue
        ts_ms = parse_ts(ts)
        if ts_ms is None:
            continue
        if not (click_window_start <= ts_ms < window_end):
            continue
        if campaign_id not in campaigns:
            continue
        person = person_of(user_id, name_of)
        clicks_of.setdefault(person, []).append((ts_ms, click_id, campaign_id))
    return clicks_of


def qualifying_clicks(sorted_clicks, sorted_click_ts, session_start):
    """Clicks 4 / 7: the person's clicks with start - 7 days <= ts <= start.

    `sorted_clicks` is sorted by ts and `sorted_click_ts` holds just the ts
    values, so the qualifying clicks are one contiguous slice.
    """
    low = bisect.bisect_left(sorted_click_ts, session_start - CLICK_LOOKBACK_MS)
    high = bisect.bisect_right(sorted_click_ts, session_start)
    return sorted_clicks[low:high]


def credited_campaign(candidates):
    """Clicks 4-5: latest ts wins; on a tie the byte-wise smallest click_id."""
    best = None
    for click in candidates:
        ts_ms, click_id, _campaign_id = click
        if (best is None or ts_ms > best[0]
                or (ts_ms == best[0] and click_id < best[1])):
            best = click
    if best is None:
        return ORGANIC                                  # Clicks 5
    return best[2]


def assisting_campaigns(candidates):
    """Clicks 7: every campaign among the qualifying clicks, once each."""
    campaigns = set()
    for _ts_ms, _click_id, campaign_id in candidates:
        campaigns.add(campaign_id)
    return campaigns


# --------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------

def top_category(revenue_of_category):
    """Outputs: highest summed purchase price; tie -> byte-wise smallest; else None."""
    best = None
    for category, revenue in revenue_of_category.items():
        if (best is None or revenue > revenue_of_category[best]
                or (revenue == revenue_of_category[best] and category < best)):
            best = category
    return best


def build_tables(events_of, clicks_of, products, campaigns):
    """Events 5-7, Clicks 4-7, Outputs: the rows of both tables."""
    # (person, day) -> [sessions, events, revenue, {category: revenue}]
    user_daily = {}
    # (campaign_id, day) -> [sessions, purchasing_sessions, revenue, assisted_sessions]
    campaign_daily = {}

    for person, events in events_of.items():
        sorted_clicks = sorted(clicks_of.get(person, []), key=lambda click: click[0])
        sorted_click_ts = [click[0] for click in sorted_clicks]

        for session in sessionize(events):              # Events 5
            session_start = session[0][0]
            day = session_start // MS_PER_DAY           # Events 6: UTC date of the start

            user_row = user_daily.setdefault((person, day), [0, 0, 0, {}])
            user_row[0] += 1
            user_row[1] += len(session)
            session_revenue = 0
            session_has_purchase = False
            for _ts_ms, product_id, price in session:
                if price is None:
                    continue                            # view or cart
                session_has_purchase = True
                session_revenue += price
                category = products.get(product_id, UNKNOWN)   # Events 7
                user_row[3][category] = user_row[3].get(category, 0) + price
            user_row[2] += session_revenue

            candidates = qualifying_clicks(sorted_clicks, sorted_click_ts, session_start)
            credited = credited_campaign(candidates)    # Clicks 4-5
            credited_row = campaign_daily.setdefault((credited, day), [0, 0, 0, 0])
            credited_row[0] += 1
            if session_has_purchase:
                credited_row[1] += 1
            credited_row[2] += session_revenue
            for campaign_id in assisting_campaigns(candidates):   # Clicks 7
                campaign_daily.setdefault((campaign_id, day), [0, 0, 0, 0])[3] += 1

    user_rows = []
    for (person, day), (sessions, event_count, revenue, by_category) in user_daily.items():
        user_rows.append((person, day, sessions, event_count, revenue, top_category(by_category)))
    campaign_rows = []
    for (campaign_id, day), (sessions, purchasing, revenue, assisted) in campaign_daily.items():
        channel = ORGANIC if campaign_id == ORGANIC else campaigns[campaign_id]
        campaign_rows.append((campaign_id, channel, day, sessions, purchasing, revenue, assisted))
    return user_rows, campaign_rows


# --------------------------------------------------------------------------
# Output (the only place pyarrow is used)
# --------------------------------------------------------------------------

def write_table(folder, columns):
    """Outputs: one Parquet file; `columns` is a list of (name, kind, values)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    arrays = []
    names = []
    for name, kind, values in columns:
        if kind == "string":
            # Values are raw bytes from the input; the cast checks they are UTF-8.
            array = pa.array(values, type=pa.binary()).cast(pa.string())
        elif kind == "date":
            array = pa.array(values, type=pa.date32())  # days since 1970-01-01
        else:
            array = pa.array(values, type=pa.int64())
        arrays.append(array)
        names.append(name)
    os.makedirs(folder, exist_ok=True)
    pq.write_table(pa.Table.from_arrays(arrays, names=names),
                   os.path.join(folder, "part-0.parquet"))


def write_outputs(output_dir, user_rows, campaign_rows):
    write_table(os.path.join(output_dir, "user_daily"), [
        ("user_id", "string", [row[0] for row in user_rows]),
        ("day", "date", [row[1] for row in user_rows]),
        ("sessions", "int", [row[2] for row in user_rows]),
        ("events", "int", [row[3] for row in user_rows]),
        ("revenue_cents", "int", [row[4] for row in user_rows]),
        ("top_category", "string", [row[5] for row in user_rows]),
    ])
    write_table(os.path.join(output_dir, "campaign_daily"), [
        ("campaign_id", "string", [row[0] for row in campaign_rows]),
        ("channel", "string", [row[1] for row in campaign_rows]),
        ("day", "date", [row[2] for row in campaign_rows]),
        ("sessions", "int", [row[3] for row in campaign_rows]),
        ("purchasing_sessions", "int", [row[4] for row in campaign_rows]),
        ("revenue_cents", "int", [row[5] for row in campaign_rows]),
        ("assisted_sessions", "int", [row[6] for row in campaign_rows]),
    ])


def main():
    if len(sys.argv) >= 3:
        data_dir, output_dir = sys.argv[1], sys.argv[2]
    else:
        data_dir = os.environ.get("DATA_DIR", "/data")
        output_dir = os.environ.get("OUTPUT_DIR", "/output")

    window_start, window_end = read_manifest(data_dir)
    products = read_lookup(os.path.join(data_dir, "products.csv"))
    campaigns = read_lookup(os.path.join(data_dir, "campaigns.csv"))
    name_of = read_person_names(data_dir)
    events_of = surviving_events_by_person(data_dir, name_of, window_start, window_end)
    clicks_of = valid_clicks_by_person(data_dir, name_of, campaigns, window_start, window_end)
    user_rows, campaign_rows = build_tables(events_of, clicks_of, products, campaigns)
    write_outputs(output_dir, user_rows, campaign_rows)


if __name__ == "__main__":
    main()
