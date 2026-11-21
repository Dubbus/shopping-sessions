#!/usr/bin/env python3
"""
Adversarial test suite for the Shopping Sessions pipeline (docs/rules.md).

Writes every case input deterministically (bytes, no randomness beyond seeded
random.Random), in the fixture format of tests/fixtures:

    tests/adversarial/<case>/input/...            cases both references agree on
    tests/adversarial/<case>/expected/*.csv       (written by --expected)
    tests/adversarial-disputed/<case>/input/...   cases the references disagree on
    tests/adversarial-disputed/<case>/ref-a|ref-b/*.csv  (or crash.txt)

Usage:
    make_adversarial.py                 write all inputs and tests/adversarial/README.md
    make_adversarial.py --expected      also run both references on every case and
                                        write expected/ where they agree exactly
    make_adversarial.py --only NAME...  restrict either step to some cases (prefix match)
    make_adversarial.py --check-disputed -- CMD...
                                        run CMD on the disputed cases and say which
                                        reference it matches

Nothing here is random at run time: the same script always writes the same bytes.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import itertools
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
SUITE = ROOT / "tests" / "adversarial"
DISPUTED = ROOT / "tests" / "adversarial-disputed"
PYTHON = ROOT / ".venv" / "bin" / "python"
REFERENCES = {"ref-a": ROOT / "reference/a/reference.py", "ref-b": ROOT / "reference/b/reference.py"}

EH = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CH = b"click_id,user_id,campaign_id,ts"
LH = b"user_id_a,user_id_b"
HEADERS = {"events": EH, "clicks": CH, "links": LH}

S = 1000
M = 60 * S
H = 60 * M
D = 24 * H
UNIT = 2_097_152   # the fast binary reads files in chunks of this many bytes (2 MiB; it was 4 MiB earlier)
CHUNK = 2 * UNIT   # 4,194,304
STEP = 65_536      # ... and reads on in steps of this size to finish a line that crosses a chunk end


# ---------------------------------------------------------------------------
# Time helpers (own calendar code so years 0000 and 9999 work)
# ---------------------------------------------------------------------------
def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m - 3 if m > 2 else m + 9) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def civil_from_days(z):
    z += 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return y + (m <= 2), m, d


def utc(y, mo, d, h=0, mi=0, s=0, ms=0):
    return days_from_civil(y, mo, d) * D + h * H + mi * M + s * S + ms


def f1(t):
    """Epoch ms (whole seconds) -> F1 bytes."""
    days, rem = divmod(t, D)
    assert rem % S == 0, t
    y, mo, d = civil_from_days(days)
    assert 0 <= y <= 9999, y
    return b"%04d-%02d-%02dT%02d:%02d:%02dZ" % (y, mo, d, rem // H, rem % H // M, rem % M // S)


def f2(t, off_min, sign=None):
    """Epoch ms (whole seconds) -> F2 bytes at the given offset in minutes."""
    local = f1(t + off_min * M)[:-1]
    if sign is None:
        sign = b"+" if off_min >= 0 else b"-"
    a = abs(off_min)
    return local + sign + b"%02d:%02d" % (a // 60, a % 60)


def f3(t, width=0):
    return str(t).zfill(width).encode()


def stamp(t):
    return f1(t) if t % S == 0 else f3(t)


W0 = utc(2026, 10, 1)
W1 = utc(2026, 10, 15)
T0 = utc(2026, 10, 3, 10)


def B(x):
    if isinstance(x, bytes):
        return x
    return str(x).encode("utf-8")


def ev(eid, user="u1", ts=None, ingest=None, typ="view", prod="p1", price="", at=None):
    """One event line. `ts`/`ingest` are epoch ms (rendered F1, or F3 when not a whole
    second) or raw bytes/str. The default ingest_ts is one minute after `ts`
    (after `at`, default T0, when `ts` is raw)."""
    if ts is None:
        ts = T0
    if isinstance(ts, int):
        when, ts_b = ts, stamp(ts)
    else:
        when, ts_b = (T0 if at is None else at), B(ts)
    if ingest is None:
        ingest = when - when % S + M
    ing_b = f1(ingest) if isinstance(ingest, int) else B(ingest)
    return b",".join([B(eid), B(user), ts_b, ing_b, B(typ), B(prod), B(price)])


def ck(cid, user="u1", camp="c_1", ts=None):
    if ts is None:
        ts = T0 - H
    return b",".join([B(cid), B(user), B(camp), stamp(ts) if isinstance(ts, int) else B(ts)])


def lk(a, b):
    return B(a) + b"," + B(b)


def q(v):
    return b'"' + B(v) + b'"'


# ---------------------------------------------------------------------------
# Case registry
# ---------------------------------------------------------------------------
DEFAULT_PRODUCTS = [("p1", "shoes"), ("p2", "books")]
DEFAULT_CAMPAIGNS = [("c_1", "search"), ("c_2", "social")]
CASES = []


class Case:
    def __init__(self, name, what, rule, window=(W0, W1), products=None, campaigns=None,
                 disputed=False, ref_env=None, note="", lint=True):
        self.name, self.what, self.rule = name, what, rule
        self.window = window
        self.products = DEFAULT_PRODUCTS if products is None else products
        self.campaigns = DEFAULT_CAMPAIGNS if campaigns is None else campaigns
        self.disputed = disputed
        self.ref_env = ref_env or {}
        self.note = note
        self.lint = lint
        self.lines = {}   # (kind, file) -> [line bytes]
        self.files = {}   # relative path -> exact bytes

    # lines of a normal file: header + each line + "\n"
    def _add(self, kind, lines, file):
        bucket = self.lines.setdefault((kind, file), [])
        for line in lines:
            bucket.append(B(line))
        return self

    def ev(self, *lines, file="part-00000.csv"):
        return self._add("events", lines, file)

    def ck(self, *lines, file="part-00000.csv"):
        return self._add("clicks", lines, file)

    def ln(self, *lines, file="part-00000.csv"):
        return self._add("links", lines, file)

    def raw(self, path, data):
        """A file with exactly these bytes."""
        self.files[path] = data
        return self

    @property
    def root(self):
        return (DISPUTED if self.disputed else SUITE) / self.name

    def build(self):
        files = {}
        for (kind, file), lines in self.lines.items():
            files[f"{kind}/{file}"] = HEADERS[kind] + b"\n" + b"".join(line + b"\n" for line in lines)
        files.update(self.files)
        for kind in ("events", "clicks", "links"):
            if not any(path.startswith(kind + "/") for path in files):
                files[f"{kind}/part-00000.csv"] = HEADERS[kind] + b"\n"
        if "manifest.json" not in files:
            files["manifest.json"] = (b'{\n  "window_start": "' + f1(self.window[0])
                                      + b'",\n  "window_end": "' + f1(self.window[1]) + b'"\n}\n')
        if "products.csv" not in files:
            files["products.csv"] = b"product_id,category\n" + b"".join(
                B(p) + b"," + B(c) + b"\n" for p, c in self.products)
        if "campaigns.csv" not in files:
            files["campaigns.csv"] = b"campaign_id,channel\n" + b"".join(
                B(p) + b"," + B(c) + b"\n" for p, c in self.campaigns)
        return files

    def write(self):
        inp = self.root / "input"
        shutil.rmtree(inp, ignore_errors=True)
        built = self.build()
        if self.lint:
            lint(self.name, built)
        for path, data in built.items():
            target = inp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)


def lint_fields(line, count):
    """The rules' line parser: unquoted fields, or None for a malformed line."""
    fields = line.split(b",")
    if line == b"" or len(fields) != count:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) < 2 or f[:1] != b'"' or f[-1:] != b'"' or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        out.append(f)
    return out


def lint(name, files):
    """Checks that a case stays inside what the rules promise about the data, so that its
    expected output does not depend on choices the rules leave open:
    `ingest_ts` is always valid F1; copies of an event with equal `ingest_ts` are identical;
    copies of a click have identical values; input is valid UTF-8."""
    import re
    f1_shape = re.compile(rb"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
    events, clicks = {}, {}
    for path, data in files.items():
        data.decode("utf-8")
        kind = path.split("/")[0]
        if kind not in ("events", "clicks"):
            continue
        lines = data.split(b"\n")
        if lines[-1] == b"":
            lines.pop()
        for line in lines[1:]:
            fields = lint_fields(line, 7 if kind == "events" else 4)
            if fields is None or fields[0] == b"":
                continue
            if kind == "events":
                assert f1_shape.fullmatch(fields[3]), (name, path, line[:200])
                seen = events.setdefault((fields[0], fields[3]), fields)
                assert seen == fields, (name, "copies with equal ingest_ts differ", seen[:2], fields[:2])
            else:
                seen = clicks.setdefault(fields[0], fields)
                assert seen == fields, (name, "copies of a click differ", [x[:40] for x in seen], [x[:40] for x in fields])


def case(name, what, rule, **kw):
    assert all(c.name != name for c in CASES), name
    c = Case(name, what, rule, **kw)
    CASES.append(c)
    return c


# Rule sentences quoted in the README (docs/rules.md)
R_QUOTE = "A line with the wrong number of fields, or with a quote anywhere except around a whole field, is malformed and is dropped."
R_QUOTE_OK = 'Any field may be wrapped in double quotes, and the quotes are syntax, not part of the value: `"u1"` is `u1`, and `""` is an empty field.'
R_COMMA = "No value ever holds a comma, a quote or a newline, quoted or not: every comma separates fields."
R_EMPTY_LINE = "An empty line is malformed too."
R_CR = "Only `\\n` ends a line: a `\\r` is an ordinary character of the field it is in, so a line ending `\\r\\n` has a `\\r` at the end of its last field"
R_SPACE = "Spaces are part of a value, quoted or not: ` u1` and `\" u1\"` are one user, `u1 ` is another, and neither is `u1`."
R_DIGITS = "Wherever these rules say digits, they mean the ASCII characters `0` to `9`; `٢٥٠` and `２５０` are not digits."
R_HEADER = "Every CSV file has a header row."
R_FILES = "Read every `*.csv` file in `events/`, `clicks/` and `links/`. How many there are changes with the dataset, and so does their size: a few large files, many small ones, and some that hold only the header."
R_UTF8 = "Comma-separated, UTF-8, `\\n` line endings."
R_BADTS = "Anything else is a bad timestamp: empty, fractional seconds, a lowercase `z` or `t`, a space instead of `T`, no zone, any non-digit in F3 (`1791071890000.0`, `1.79e12`, `-5`), or an impossible calendar value (month 13, February 30, February 29 outside a leap year, hour 24, second 60), and an offset outside its range (`+24:00`, `+05:60`) or without its colon (`+0200`)."
R_F3 = "An F3 value is just a number, so one that is very short (`86400000` is 1970-01-02) or too long for a 64-bit integer is not malformed, only outside the window."
R_F3_ZERO = "Digits only: epoch milliseconds. Leading zeros are allowed (`0001791071890000` is the same instant)"
R_F2 = "Offset `+HH:MM` or `-HH:MM` (hours 00 to 23, minutes 00 to 59); subtract it to get UTC."
R_UTC0 = "`-00:00` is UTC."
R_EWIN = "An event's `ts` must satisfy `window_start <= ts < window_end`."
R_CWIN = "A click's `ts` must satisfy `window_start - 7 days <= ts < window_end` [...] A click at exactly `window_start - 7 days` is in its window; one at `window_end` is not."
R_MANIFEST = '`manifest.json`: `{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}`, different for every dataset'
R_ID_EMPTY = "Drop malformed lines and lines whose `event_id` is empty."
R_DEDUP = "Keep only the copy with the smallest `ingest_ts`. [...] Dedup comes before validation: if the kept copy is invalid the event is dropped, and a later valid copy does not rescue it."
R_DEDUP_MAL = "A malformed copy was already dropped in step 1, so it is never the kept copy."
R_DEDUP_SAME = "Copies with equal `ingest_ts` are identical."
R_INGEST = "`ingest_ts` is always valid F1."
R_VALID = "Drop the event unless `user_id` is non-empty, `ts` is valid and in the event window, `event_type` is exactly `view`, `cart` or `purchase` (case-sensitive), and `product_id` is non-empty."
R_PRICE = "A `purchase` also needs `price_cents` to be digits only with a value from 1 to 10,000,000 (`0250` is 250)."
R_PRICE_IGN = "For `view` and `cart`, `price_cents` is ignored whatever it holds."
R_LATE = "drop the event if `ingest_ts - ts` is over 24 hours. Exactly 24 hours is kept, and so is an `ingest_ts` before `ts`."
R_SESS = "A new session starts when the gap from the previous event is over 30 minutes (1,800,000 ms)."
R_SESS_EQ = "Equal timestamps share a session: the gap is measured between distinct timestamps, so when several events share a `ts` they all join the session the first of them joins, in whatever order they were read."
R_SESS_ALL = "Sessionize each person's surviving events, all of their IDs together, sorted by `ts`."
R_DAY = "One day per session: the UTC date of its start. All its events and purchases count on that day, even those after midnight."
R_CAT = "A product not in the file has the category `unknown`, a name no category in the file uses."
R_LINK_PARSE = "Drop malformed lines (the CSV dialect above) and lines where either field is empty."
R_LINK_JOIN = "joining is transitive: if `a,b` and `b,c` are lines, then `a`, `b` and `c` are one person, however many links apart and in whichever files and order the lines come."
R_LINK_REPEAT = "A line may repeat another, either way round, and may link an ID to itself; neither changes anything."
R_LINK_EXACT = "IDs compare exactly, as everywhere: `U1` is linked only to what a line links `U1` to."
R_LINK_NAME = "Name each person by the byte-wise smallest of their IDs. [...] An ID can be linked without appearing in any event or click, and can still be the smallest, and so the name."
R_LINK_FIRST = "Links come before everything else: an event or click is the person's as soon as it is parsed, whatever its own ID."
R_CLICK_PARSE = "Drop malformed lines and lines whose `click_id` is empty."
R_CLICK_DEDUP = "Copies of a click have identical values, so keep any one."
R_CLICK_VALID = "Drop the click unless `user_id` is non-empty, `ts` is valid and in the click window, and `campaign_id` is in `campaigns.csv`."
R_CREDIT = "Among the same person's valid clicks, from any of their IDs, with `click.ts <= session_start` and `session_start - click.ts <= 7 days` (604,800,000 ms), take the one with the latest `ts`; on a tie in `ts`, the smallest `click_id`, compared byte by byte. A click at exactly the session start counts; one even 1 ms after it does not."
R_ORGANIC = "No qualifying click credits the session to the campaign `organic` with the channel `organic`."
R_MANY = "One click can credit many sessions. Clicks never create, split or merge sessions."
R_ASSIST = "A session is assisted by every campaign with at least one of the person's valid clicks with `session_start - 7 days <= click.ts <= session_start` [...] Each such campaign assists the session once, however many of its clicks qualify. `organic` never assists."
R_ASSIST_ROW = "A campaign that only assisted that day has a row with 0 in the three credited columns."
R_STR = "Strings are never normalized: IDs compare exactly, case-sensitive, untrimmed. `u1` and `U1` are two IDs, and two people unless a link joins them."
R_TOP = "The category with the highest summed purchase `price_cents` in those sessions; a tie goes to the byte-wise smallest; null with no purchases. `unknown` competes like any category."
R_REV = "Sum of `price_cents` over purchases in those sessions; 0 if none"
R_CLEAN = "`products.csv`: `product_id,category`, clean, `product_id` unique / `campaigns.csv`: `campaign_id,channel`, clean, `campaign_id` unique, IDs like `c_123`"

BAD_TS = [
    # the examples of the "Anything else is a bad timestamp" sentence, in its order
    b"", b"2026-10-03T10:00:00.000Z", b"2026-10-03T10:00:00z", b"2026-10-03t10:00:00Z",
    b"2026-10-03 10:00:00Z", b"2026-10-03T10:00:00", b"1791021600000.0", b"1.79e12", b"-5",
    b"2026-13-03T10:00:00Z", b"2026-02-30T10:00:00Z", b"2026-02-29T10:00:00Z", b"2026-10-03T24:00:00Z",
    b"2026-10-03T10:00:60Z", b"2026-10-03T12:00:00+24:00", b"2026-10-03T12:00:00+05:60",
    b"2026-10-03T12:00:00+0200",
    # more of the same kinds
    b"2026-10-03T10:00:00.5Z", b"2026-10-03T10:00:00.000+02:00", b"2026-10-03T10:60:00Z",
    b"2026-00-03T10:00:00Z", b"2026-10-00T10:00:00Z", b"2026-10-32T10:00:00Z", b"2026-10-03T23:59:60Z",
    b"2026-10-03T12:00:00+02", b"2026-10-03T12:00:00 +02:00", b"2026-10-03T12:00:00 02:00",
    b"2026-10-3T10:00:00Z", b"2026-10-03T10:00Z", b"20261003T100000Z", b"2026-10-03T10:00:00ZZ",
    b"2026-10-03T10:00:00Z ", b" 2026-10-03T10:00:00Z", b"+1791021600000", b"0x1A10000000",
    b"1_791_021_600_000", b"1791021600000L", b"1791021600000 ", b" 1791021600000", b"1791021600e3",
    "２０２６-10-03T10:00:00Z".encode(), "2026-1０-03T10:00:00Z".encode(), "١٧٩١٠٢١٦٠٠٠٠٠".encode(),
    "１７９１０２１６０００００".encode(), b"2026-10-03T12:00:00+2:00", "2026-10-03T12:00:00−02:00".encode(),
    b"2026-10-03T12:00:00+02:00:00", b"2026-10-03T12:00:00+02.00", b"2026-10-03T12:00:00-24:00",
    b"2026-10-03T12:00:00-00:60", b"2026-10-03T12:00:00+99:99", b"2026/10/03T10:00:00Z",
    b"2026-10-03T10-00-00Z", b"2026-10-03T10:00:00UTC", b"2026-10-03T10:00:00 Z",
    b"2026-10-03T10:00:00+00:00Z", b"2026-10-03T10:00:00Z+00:00", b"1791021600000Z", b"Z", b"T",
    b"null", b"NaN", b"2026-10-03T1o:00:00Z", b"2026-10-03T-1:00:00Z", b"2026-10-03T+1:00:00Z",
    b"2026-10-03T 1:00:00Z", b"2026-10-+3T10:00:00Z", b"+026-10-03T10:00:00Z", b" 026-10-03T10:00:00Z",
    b"2026-10-03T10:00:00+ 2:00", b"2026-10-03T10:00:00+-2:00", b"2026-10-03T10:00:00+0200Z",
    b"2026-10-03T10:00:00\r", b"2026-10-03T10:00:00Z\t", b"2026-10-03T10:00:00+02:0", b"2026-10-03T10:00:0Z",
    b"26-10-03T10:00:00Z", b"02026-10-03T10:00:00Z", b"2026-10-03T10:00:00-0000", b"2026-W40-6T10:00:00Z",
    b"2026-276T10:00:00Z", b"2026-10-03T10:00:00GMT", b"2026-10-03T10:00:00+", b"2026-10-03T10:00:00-",
    b"1791021600000-", b"1791021600000+00:00", b"17910216 00000",
]

BAD_PRICES = [
    b"0", b"00", b"000000000", b"10000001", b"0010000001", b"99999999", b"123456789012345678901234567890",
    b"+5", b"-5", b"1e3", b" 5", b"5 ", b"5.0", b"19.99", b"", b"4294967796", b"18446744073709552116",
    b"9223372036854775808", "٢٥٠".encode(), "２５０".encode(), "१२३".encode(), b"0x10", b"1_000", b"$5",
    b"5\r", b"\t5", b"five", b"NaN", b"null", b".5", b"5.", b"1 000", b"--5", b"5e0", b"0.0",
]
GOOD_PRICES = [(b"1", 1), (b"0001", 1), (b"10000000", 10_000_000), (b"010000000", 10_000_000),
               (b"0000000000000000000000000000000000000250", 250), (b"9999999", 9_999_999),
               (b'"500"', 500), (b"0250", 250)]


# ---------------------------------------------------------------------------
# 1. CSV dialect
# ---------------------------------------------------------------------------
def dialect_case(name, what, rule, fn, skip=()):
    """Applies `fn` to one field at a time, in every column of every file kind.
    Each mutated line has its own user, so the output shows which lines were kept."""
    c = case(name, what, rule)
    for col in range(7):
        if col in skip:
            continue
        f = [b"e%d" % col, b"ev%d" % col, f1(T0), f1(T0 + M), b"purchase", b"p1", b"500"]
        f[col] = fn(f[col])
        c.ev(b",".join(f))
    c.ev(ev("eok", "evok", T0, typ="purchase", price=500))
    for col in range(4):
        user = b"ck%d" % col
        c.ev(ev("s%d" % col, user, T0))
        f = [b"k%d" % col, user, b"c_1", f1(T0 - H)]
        f[col] = fn(f[col])
        c.ck(b",".join(f))
    c.ev(ev("sok", "ckok", T0))
    c.ck(ck("kok", "ckok"))
    for col in range(2):
        f = [b"la%d" % col, b"lb%d" % col]
        f[col] = fn(f[col])
        c.ln(b",".join(f))
        c.ev(ev("l%d" % col, b"lb%d" % col, T0))
    return c


def dialect():
    each = "in each column of events, clicks and links in turn (one line and one user per column, plus a clean control line)"
    dialect_case("csv_quote_lone", f'A field that is just `"`, {each}', R_QUOTE, lambda v: b'"')
    dialect_case("csv_quote_start_only", f'A field with an opening quote only (`"u1`), {each}', R_QUOTE, lambda v: b'"' + v)
    dialect_case("csv_quote_end_only", f'A field with a closing quote only (`u1"`), {each}', R_QUOTE, lambda v: v + b'"')
    dialect_case("csv_quote_middle", f'A quote in the middle of a field (`u"1`), {each}', R_QUOTE, lambda v: v[:1] + b'"' + v[1:])
    dialect_case("csv_quote_triple", f'A field that is `"""`, {each}', R_QUOTE, lambda v: b'"""')
    dialect_case("csv_quote_quad", f'A field that is `""""` (an escaped quote in standard CSV), {each}', R_QUOTE, lambda v: b'""""')
    dialect_case("csv_quote_doubled_wrap", f'A field wrapped twice (`""u1""`), {each}', R_QUOTE, lambda v: b'""' + v + b'""')
    dialect_case("csv_quote_escaped_pair", f'A standard-CSV escaped quote inside a quoted field (`"u""1"`), {each}', R_QUOTE,
                 lambda v: b'"' + v[:1] + b'""' + v[1:] + b'"')
    dialect_case("csv_quote_then_text", f'Text after the closing quote (`"u1"x`), {each}', R_QUOTE, lambda v: b'"' + v + b'"x')
    dialect_case("csv_text_then_quote", f'Text before the opening quote (`x"u1"`), {each}', R_QUOTE, lambda v: b'x"' + v + b'"')
    dialect_case("csv_quote_space_before", f'A space before the opening quote (` "u1"`), {each}', R_QUOTE, lambda v: b' "' + v + b'"')
    dialect_case("csv_quote_space_after", f'A space after the closing quote (`"u1" `), {each}', R_QUOTE, lambda v: b'"' + v + b'" ')
    dialect_case("csv_quote_doubled_inside", f'A doubled quote inside a bare field (`u""1`), {each}',
                 R_QUOTE, lambda v: v[:1] + b'""' + v[1:])
    dialect_case("csv_quoted_each_column", f'A properly quoted field (`"u1"`), {each}: every line is kept', R_QUOTE_OK, q)
    dialect_case("csv_quoted_empty_each_column",
                 f'A quoted empty field (`""`), {each} (not `ingest_ts`, which the rules promise is valid)',
                 R_QUOTE_OK + " An empty field means null.", lambda v: b'""', skip=(3,))
    dialect_case("csv_quoted_spaces_inside", f'Spaces inside the quotes (`" u1 "`), {each}: a different value, never trimmed',
                 R_SPACE, lambda v: b'" ' + v + b' "', skip=(3,))

    c = case("csv_all_fields_quoted", "Every field of every line quoted in events, clicks and links; a purchase, a credited click and a link",
             R_QUOTE_OK)
    c.ev(b",".join(q(x) for x in [b"e1", b"u2", f1(T0), f1(T0 + M), b"purchase", b"p1", b"0750"]))
    c.ev(b",".join(q(x) for x in [b"e2", b"u2", f3(T0 + 5 * M + 1), f1(T0 + 6 * M), b"view", b"p2", b""]))
    c.ck(b",".join(q(x) for x in [b"k1", b"u1", b"c_2", f1(T0 - H)]))
    c.ln(b",".join(q(x) for x in [b"u2", b"u1"]))

    c = case("csv_quoted_comma_inside",
             'Quoted fields holding a comma: `e1,"qc1,x",...` (8 raw fields), `"e2,qc2",...` (7 raw fields, quotes split), '
             '`"p1,x"` as product, a purchase priced `"1,000"`; the same in clicks and links',
             R_COMMA)
    t, i = f1(T0), f1(T0 + M)
    c.ev(b'e1,"qc1,x",' + t + b"," + i + b",view,p1,")
    c.ev(b'"e2,qc2",' + t + b"," + i + b",view,p1,")
    c.ev(b"e3,qc3," + t + b"," + i + b',view,"p1,x",')
    c.ev(b"e4,qc4," + t + b"," + i + b',purchase,p1,"1,000"')
    c.ev(b'e5,qc5,"' + t + b"," + i + b'",view,p1,')
    c.ev(ev("e6", "qcok"))
    c.ev(ev("s1", "qk1"), ev("s2", "qk2"))
    c.ck(b'"k1,qk1",c_1,' + f1(T0 - H))
    c.ck(b'k2,"qk2,c_1",' + f1(T0 - H))
    c.ln(b'"qcok,zz"', b'"a,b",qcok', b'qcok,"a,b"')

    c = case("csv_field_count_events",
             "Event lines with 1, 2, 6 and 8 fields, a trailing comma, a leading comma, only commas (6, 7 and 8 of them), "
             "and a 7-field line shifted by a leading comma (empty event_id)", R_QUOTE)
    t, i = f1(T0), f1(T0 + M)
    c.ev(b"e1,fc1," + t + b"," + i + b",view,p1")                 # 6 fields
    c.ev(b"e2,fc2," + t + b"," + i + b",view,p1,,")               # 8 fields: trailing comma
    c.ev(b",e3,fc3," + t + b"," + i + b",view,p1,")               # 8 fields: leading comma
    c.ev(b"e4,fc4," + t + b"," + i + b",view,p1,,extra")          # 9 fields
    c.ev(b"e5")
    c.ev(b"e6,fc6")
    c.ev(b",,,,,")
    c.ev(b",,,,,,")
    c.ev(b",,,,,,,")
    c.ev(b",fc7," + t + b"," + i + b",view,p1")                   # 7 fields, event_id empty
    c.ev(b"e8,fc8," + t + b"," + i + b",view,p1,100,")            # purchase-like with trailing comma
    c.ev(b"e9,fc9," + t + b"," + i + b",purchase,p1")             # purchase without the price field
    c.ev(ev("e10", "fcok", typ="purchase", price=300))

    c = case("csv_field_count_clicks_links",
             "Click lines with 3 and 5 fields, trailing and leading commas; link lines with 1 and 3 fields, trailing and leading commas",
             R_QUOTE)
    for n in range(1, 8):
        c.ev(ev(f"s{n}", f"fk{n}"))
    t = f1(T0 - H)
    c.ck(b"k1,fk1,c_1")
    c.ck(b"k2,fk2,c_1," + t + b",")
    c.ck(b",k3,fk3,c_1," + t)
    c.ck(b"k4,fk4,c_1," + t + b",x")
    c.ck(b"k5,fk5," + t)
    c.ck(b",fk6,c_1," + t)                                         # 4 fields, click_id empty
    c.ck(ck("k7", "fk7"))
    for n in range(1, 6):
        c.ev(ev(f"m{n}", f"fl{n}"))
    c.ln(b"fl1", b"aa,fl2,", b",aa,fl3", b"aa,fl4,bb", b"aa,fl5")

    c = case("csv_empty_lines",
             "Empty lines right after the header, in the middle, consecutive and at the end (the file ends `\\n\\n\\n`) of events, "
             "clicks and links; lines holding only a space, a tab or a `\\r`", R_EMPTY_LINE)
    c.raw("events/part-00000.csv", EH + b"\n\n" + ev("e1", "u1") + b"\n\n\n \n\t\n\r\n" + ev("e2", "u2") + b"\n\n\n")
    c.raw("clicks/part-00000.csv", CH + b"\n\n\n" + ck("k1", "u1") + b"\n\n" + ck("k2", "u2", "c_2") + b"\n\n\n\n")
    c.raw("links/part-00000.csv", LH + b"\n\n" + lk("u2", "a0") + b"\n\n\r\n \n" + lk("u1", "a1") + b"\n\n")

    c = case("csv_crlf_everything",
             "All three files written with `\\r\\n` endings (header too): views and carts are kept (their price is `\\r`), purchases "
             "are dropped (price `500\\r`), every click is dropped (`ts` ends in `\\r`), and every link joins its first ID to "
             "`<second ID>\\r`; one event uses the ID `lb\\r`", R_CR)
    lines = [ev("e1", "u1", T0), ev("e2", "u1", T0 + M, typ="cart"), ev("e3", "u1", T0 + 2 * M, typ="purchase", price=500),
             ev("e4", "u2", T0, typ="purchase", price=500), ev("e5", "lb", T0), ev("e6", b"lb\r", T0 + M),
             ev("e7", "u3", T0, typ="view", price=500)]
    c.raw("events/part-00000.csv", EH + b"\r\n" + b"".join(l + b"\r\n" for l in lines))
    c.raw("clicks/part-00000.csv", CH + b"\r\n" + ck("k1", "u1") + b"\r\n" + ck("k2", "u3") + b"\r\n")
    c.raw("links/part-00000.csv", LH + b"\r\n" + lk("la", "lb") + b"\r\n" + lk("u3", "u0") + b"\r\n")

    c = case("csv_cr_each_column",
             "A lone `\\r` at the end, start and middle of each column (not `ingest_ts`): event IDs `e` and `e\\r` are two events, "
             "user `ucr\\r` is its own person, a `ts`/type/price with `\\r` is bad, product `p1\\r` is unknown; a line that is only "
             "`\\r`; two records joined by a lone `\\r` (old Mac line ending); the same for click columns", R_CR)
    c.ev(ev("ecr", "idcr", T0), ev(b"ecr\r", "idcr", T0 + M), ev(b"\recr", "idcr", T0 + 2 * M), ev(b"e\rcr", "idcr", T0 + 3 * M))
    c.ev(ev("u1", b"ucr\r", T0), ev("u2", b"\rucr", T0), ev("u3", b"u\rcr", T0), ev("u4", "ucr", T0))
    c.ev(ev("t1", "tscr", f1(T0) + b"\r"), ev("t2", "tscr", b"\r" + f1(T0)), ev("t3", "tscr", f3(T0) + b"\r"),
         ev("t4", "tscr", T0 + 3 * H))
    c.ev(ev("y1", "tycr", T0, typ=b"view\r"), ev("y2", "tycr", T0, typ=b"\rview"), ev("y3", "tycr", T0 + 3 * H))
    c.ev(ev("p1", "prcr", T0, typ="purchase", prod=b"p1\r", price=700), ev("p2", "prcr", T0 + M, typ="purchase", prod=b"p1", price=600))
    c.ev(ev("c1", "pccr", T0, typ="purchase", price=b"500\r"), ev("c2", "pccr", T0, typ="purchase", price=b"\r500"),
         ev("c3", "pccr", T0, typ="purchase", price=b"5\r00"), ev("c4", "pccr", T0 + 3 * H, typ="view", price=b"\r"),
         ev("c5", "pccr", T0 + 3 * H, typ="cart", price=b"500\r"))
    c.ev(b"\r")
    c.ev(ev("j1", "joined", T0) + b"\r" + ev("j2", "joined", T0 + M))
    c.ev(ev("j3", "joined", T0 + 5 * H))
    for u in ("kidcr", "kucr", "kcacr", "ktscr", "kok"):
        c.ev(ev("s" + u, u, T0))
    c.ck(ck(b"k1\r", "kidcr", "c_2", T0 - H), ck(b"k1", "kidcr", "c_1", T0 - H))   # tie: "k1" < "k1\r"
    c.ck(ck("k2", b"kucr\r"), ck("k3", "kcacr", b"c_1\r"), ck("k4", "ktscr", "c_1", f1(T0 - H) + b"\r"), ck("k5", "kok"))
    c.ck(b"\r")

    c = case("csv_no_trailing_newline_valid",
             "The final line of events, clicks and links files is valid and has no trailing `\\n` (two event files: one ends in a "
             "purchase, one in a view with an empty price, so the file ends with a comma)", R_UTF8 + " (a last line needs no `\\n` to be a line)")
    c.raw("events/part-00000.csv", EH + b"\n" + ev("e1", "u1", T0) + b"\n" + ev("e2", "u1", T0 + M, typ="purchase", price=450))
    c.raw("events/part-00001.csv", EH + b"\n" + ev("e3", "u2", T0) + b"\n" + ev("e4", "u2", T0 + M))
    c.raw("clicks/part-00000.csv", CH + b"\n" + ck("k1", "u2", "c_1", T0 - 2 * H) + b"\n" + ck("k2", "u1", "c_2"))
    c.raw("links/part-00000.csv", LH + b"\n" + lk("u9", "u8") + b"\n" + lk("u2", "u0"))

    c = case("csv_no_trailing_newline_malformed",
             "The final line has no trailing `\\n` and is malformed: 6 fields, a lone quote, an unbalanced quote, a line that stops "
             "mid-timestamp (a truncated file) in events; 3 fields in clicks; 1 field in links", R_QUOTE)
    c.raw("events/part-00000.csv", EH + b"\n" + ev("e1", "u1", T0) + b"\n" + b"e2,u1," + f1(T0 + M) + b"," + f1(T0 + M) + b",view,p1")
    c.raw("events/part-00001.csv", EH + b"\n" + ev("e3", "u2", T0) + b"\n" + b'"')
    c.raw("events/part-00002.csv", EH + b"\n" + ev("e4", "u3", T0) + b"\n" + ev("e5", "u3", T0 + M)[:-1] + b'"p1')
    c.raw("events/part-00003.csv", EH + b"\n" + ev("e6", "u4", T0) + b"\n" + b"e7,u4,2026-10-03T10:0")
    c.raw("clicks/part-00000.csv", CH + b"\n" + ck("k1", "u1") + b"\n" + b"k2,u2,c_1")
    c.raw("links/part-00000.csv", LH + b"\n" + lk("u4", "u0") + b"\n" + b"u3")

    c = case("csv_header_only_no_newline",
             "In each folder one file whose only content is the header without a `\\n`, next to a file with data", R_HEADER)
    c.raw("events/part-00000.csv", EH)
    c.raw("clicks/part-00000.csv", CH)
    c.raw("links/part-00000.csv", LH)
    c.ev(ev("e1", "u1"), file="part-00001.csv").ck(ck("k1", "u1"), file="part-00001.csv").ln(lk("u1", "u0"), file="part-00001.csv")

    c = case("csv_header_only_no_newline_alone",
             "Every events, clicks and links file is just the header without a `\\n`: both tables are empty", R_HEADER)
    c.raw("events/part-00000.csv", EH).raw("clicks/part-00000.csv", CH).raw("links/part-00000.csv", LH)

    c = case("csv_zero_byte_files",
             "Zero-byte files and a file holding a single `\\n` in each folder, next to files with data", R_FILES)
    for kind in ("events", "clicks", "links"):
        c.raw(f"{kind}/part-00000.csv", b"")
        c.raw(f"{kind}/part-00002.csv", b"\n")
        c.raw(f"{kind}/part-00003.csv", b"")
    c.ev(ev("e1", "u1"), file="part-00001.csv").ck(ck("k1", "u1"), file="part-00001.csv").ln(lk("u1", "u0"), file="part-00001.csv")

    c = case("csv_header_only_files", "Several header-only files (with `\\n`) around one file with data in each folder; "
             "copies of one event sit in the files before and after them", R_FILES)
    for kind in ("events", "clicks", "links"):
        for n in (0, 2, 3, 5):
            c.raw(f"{kind}/part-0000{n}.csv", HEADERS[kind] + b"\n")
    c.ev(ev("e1", "u1", T0, ingest=T0 + 5 * M), file="part-00001.csv")
    c.ev(ev("e1", "u2", T0, ingest=T0 + M), ev("e2", "u1", T0 + H * 2), file="part-00004.csv")
    c.ck(ck("k1", "u2"), file="part-00001.csv").ck(ck("k1", "u2"), file="part-00004.csv")
    c.ln(lk("u1", "u0"), file="part-00004.csv")

    c = case("csv_header_line_as_data",
             "A data line identical to the header in clicks and in links (also quoted, and repeated): the click has the bad `ts` "
             "`ts`; the link joins `user_id_a` to `user_id_b`, and `user_id_b` has an event", R_HEADER + " (only the first line is the header)")
    c.ev(ev("e1", "user_id_b"), ev("e2", "user_id"), ev("e3", "u1"))
    c.ck(CH, ck("k1", "user_id", "c_1"), b",".join(q(x) for x in CH.split(b",")), ck("k2", "u1", "campaign_id"))
    c.ln(LH, lk("u1", "u0"), LH, b'"user_id_a","user_id_b"')

    c = case("csv_event_header_line_as_data",
             "A data line identical to the events header (its `ingest_ts` is the text `ingest_ts`), alone and next to a later valid "
             "event whose ID is `event_id`", R_INGEST + " / " + R_VALID, disputed=True, lint=False,
             note="Outside the letter of the rules (`ingest_ts` is always valid F1), but the classic result of concatenating part files.")
    c.ev(ev("e1", "u1"), EH, ev("event_id", "u2", T0, ingest=T0 + M))

    c = case("csv_spaces_tabs",
             "Leading and trailing spaces and tabs in each column: event IDs ` e1`, `e1 `, `e1` (three events); users ` sp`, `sp `, "
             "`sp\\t`, `\\tsp`; a `ts`/type/purchase price with a space or tab (dropped); product ` p1` (unknown); a view priced "
             "` 5 ` (kept); clicks by ` sp` and for campaign `c_1 ` / ` c_1` (unknown); links to ` sp` and `sp `", R_SPACE)
    c.ev(ev("e1", "ids", T0), ev(" e1", "ids", T0 + M), ev("e1 ", "ids", T0 + 2 * M), ev("\te1", "ids", T0 + 3 * M),
         ev("e 1", "ids", T0 + 4 * M))
    for n, u in enumerate([" sp", "sp ", "sp\t", "\tsp", "sp", "s p", "  sp", " "]):
        c.ev(ev(f"u{n}", u, T0))
    c.ev(ev("t1", "tsp", b" " + f1(T0)), ev("t2", "tsp", f1(T0) + b" "), ev("t3", "tsp", f3(T0) + b"\t"),
         ev("t4", "tsp", b" " + f3(T0)), ev("t5", "tsp", T0 + 3 * H))
    c.ev(ev("y1", "typ", T0, typ=" view"), ev("y2", "typ", T0, typ="view "), ev("y3", "typ", T0, typ="cart\t"),
         ev("y4", "typ", T0, typ="pur chase", price=5), ev("y5", "typ", T0 + 3 * H, typ="cart"))
    c.ev(ev("p1", "prd", T0, typ="purchase", prod=" p1", price=300), ev("p2", "prd", T0, typ="purchase", prod="p1 ", price=300),
         ev("p3", "prd", T0, typ="purchase", prod="p1", price=500), ev("p4", "prd", T0, typ="view", prod=" "))
    c.ev(ev("c1", "prc", T0, typ="purchase", price=" 5"), ev("c2", "prc", T0, typ="purchase", price="5 "),
         ev("c3", "prc", T0, typ="purchase", price="5\t"), ev("c4", "prc", T0, typ="purchase", price=" "),
         ev("c5", "prc", T0 + 3 * H, typ="view", price=" 5 "), ev("c6", "prc", T0 + 3 * H, typ="purchase", price="5"))
    c.ck(ck("k1", " sp", "c_1"), ck("k2", "sp", "c_1 "), ck("k3", "sp", " c_1"), ck("k4", "sp\t", "c_2"),
         ck("k5", "s p", "c_1", b" " + f1(T0 - H)), ck(" k6", "  sp", "c_2"), ck("k6", "  sp", "c_1"))
    c.ln(lk(" sp", "a0"), lk("a1", "sp "), lk("a2 ", "sp"), lk(" ", "a3"))

    c = case("csv_very_long_lines",
             "Long lines: a view with a 500,000-byte product ID, a purchase with a 300,000-byte unknown product, a view with a "
             "300,000-byte junk price, a 100,000-byte user ID, a 600,000-byte line without commas, 500,000 commas, a 7-field line "
             "with a quote after 400,000 bytes, a click with a 200,000-byte click_id", R_QUOTE + " (no limit on line or field length is given)")
    c.ev(ev("e1", "longprod", T0, prod=b"p" * 500_000))
    c.ev(ev("e2", "longprod", T0 + M, typ="purchase", prod=b"q" * 300_000, price=900))
    c.ev(ev("e3", "longprice", T0, price=b"9" * 300_000))
    c.ev(ev("e4", b"x" * 100_000, T0))
    c.ev(b"a" * 600_000)
    c.ev(b"," * 500_000)
    c.ev(ev("e5", "longquote", T0, prod=b"z" * 400_000 + b'"'))
    c.ev(ev("e6", "longquote", T0 + 3 * H))
    c.ev(ev("e7", "longclick", T0))
    c.ck(ck(b"k" * 200_000, "longclick", "c_2"))
    c.ck(ck(b"k" * 200_000 + b"2", b"x" * 100_000, "c_1"))

    c = case("csv_utf8_ids",
             "Multi-byte UTF-8 in every ID column: `ü1`, `用户`, an emoji, `é` precomposed (NFC) and decomposed (NFD) as two "
             "different users, full-width `ｕ１`, a zero-width space, NBSP, a BOM inside an ID; UTF-8 products, categories, "
             "campaigns and channels; NFC `é` linked to NFD `é` (the NFD form is byte-wise smaller)", R_STR,
             products=[("p1", "shoes"), ("产品", "書籍"), ("pé", "café"), ("p😀", "😀")],
             campaigns=[("c_1", "search"), ("c_é", "réseau"), ("c_用", "社交")])
    nfc, nfd = "\u00e9", "e\u0301"
    users = ["ü1", "用户", "😀", nfc + "x", nfd + "x", "ｕ１", "u\u200b1", "u\u00a01", "\ufeffu1", "u1", "ÿ", "\u07ff", "\uffff", "\U00010000"]
    for n, u in enumerate(users):
        c.ev(ev(f"é{n}", u, T0, typ="purchase", prod=["产品", "pé", "p😀", "p1"][n % 4], price=100 + n))
    c.ev(ev("é0", "dup-of-é0", T0, ingest=T0 + 5 * M), ev("e\u03010", "ü1", T0 + M))
    c.ev(ev("l1", "link" + nfc), ev("l2", "link" + nfd, T0 + 10 * M), ev("l3", "ω", T0), ev("l4", "z", T0 + M))
    c.ln(lk("link" + nfc, "link" + nfd), lk("ω", "ö"), lk("z", "ω"))
    c.ck(ck("k用", "用户", "c_用"), ck("ké", "ü1", "c_é"), ck("k1", "😀", "c_É"), ck("k2", "ö", "c_é", T0 - 2 * H))

    c = case("csv_unicode_line_separators",
             "IDs holding characters that text-mode line splitting treats as line ends but the rules do not: vertical tab, form "
             "feed, FS/GS/RS (0x1C-0x1E), NEL (U+0085), U+2028, U+2029; and a NUL-free control character 0x01", R_CR)
    seps = ["\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\u0085", "\u2028", "\u2029", "\x01", "\x7f"]
    for n, sep in enumerate(seps):
        c.ev(ev(f"e{n}{sep}x", f"u{sep}{n}", T0, typ="purchase", price=100 + n))
        c.ev(ev(f"e{n}", f"u{sep}{n}", T0 + M))
        c.ck(ck(f"k{sep}{n}", f"u{sep}{n}", "c_1"))
    c.ln(lk("u\x0b0", "a\x0c0"), lk("u\u20297", "a\u20287"))

    c = case("csv_bom_at_file_start",
             "Events, clicks and links files that start with a UTF-8 byte-order mark: the first line is still the header and is "
             "skipped; a second file has a BOM at the start of a data line's event_id", R_HEADER)
    bom = b"\xef\xbb\xbf"
    c.raw("events/part-00000.csv", bom + EH + b"\n" + ev("e1", "u1", T0) + b"\n" + ev(bom + b"e1", "u1", T0 + M) + b"\n")
    c.raw("clicks/part-00000.csv", bom + CH + b"\n" + ck("k1", "u1") + b"\n")
    c.raw("links/part-00000.csv", bom + LH + b"\n" + lk("u1", "u0") + b"\n")

    c = case("csv_user_ids_near_equal",
             "Users that differ only by case, a trailing or leading space, a tab, a leading zero or quoting: `u1`, `U1`, `u1 `, "
             "` u1`, `u1\\t`, `u01`, `\"u1\"` (the same as `u1`), `\"u1 \"` (the same as `u1 `); a click by each of `U1` and `u1 `", R_STR)
    for n, u in enumerate([b"u1", b"U1", b"u1 ", b" u1", b"u1\t", b"u01", b'"u1"', b'"u1 "', b"u1.", b"u1\xc2\xa0"]):
        c.ev(ev(f"e{n}", u, T0 + n * M))
    c.ck(ck("k1", "U1", "c_1"), ck("k2", "u1 ", "c_2"))

    c = case("csv_event_ids_near_equal",
             "Event IDs that differ only by case, spaces, a leading zero or a `\\r` are different events and must not dedup: `e1`, "
             "`E1`, `e1 `, ` e1`, `e01`, `e1\\r`, `e1.`; `\"e1\"` is a copy of `e1` (later ingest, other user, so it loses) and "
             "`\"E1\"` a copy of `E1` (earlier ingest, user `u3`, so it wins)",
             R_STR + " / " + R_DEDUP)
    for n, e in enumerate([b"e1", b"E1", b"e1 ", b" e1", b"e01", b"e1\r", b"e1.", b"e\xc2\xa01"]):
        c.ev(ev(e, "u1", T0 + n * M, ingest=T0 + H))
    c.ev(ev(b'"e1"', "u2", T0, ingest=T0 + 2 * H), ev(b'"E1"', "u3", T0, ingest=T0 + 30 * M))

    c = case("csv_long_event_ids",
             "event_ids of 65,534, 65,535, 65,536 and 70,000 bytes of `a` (each a prefix of the next: four events); two IDs "
             "sharing a 70,000-byte prefix and differing in the last byte; later copies of each (quoted, other file, other user: "
             "must dedup); one long ID whose earliest copy is invalid", R_DEDUP + " (no limit on ID length is given)")
    n = 0
    for length in (65_534, 65_535, 65_536, 70_000):
        c.ev(ev(b"a" * length, "u1", T0 + n * M, ingest=T0 + H, typ="purchase", price=100 + n))
        c.ev(ev(q(b"a" * length), "other", T0 + n * M, ingest=T0 + 2 * H, typ="purchase", price=9000), file="part-00001.csv")
        n += 1
    prefix = b"b" * 70_000
    c.ev(ev(prefix + b"1", "u2", T0, ingest=T0 + H), ev(prefix + b"2", "u2", T0 + M, ingest=T0 + H))
    c.ev(ev(prefix + b"2", "other", T0, ingest=T0 + 3 * H), ev(q(prefix + b"1"), "other", T0, ingest=T0 + 3 * H), file="part-00001.csv")
    c.ev(ev(b"c" * 66_000, "u3", "bad", ingest=T0 + M), file="part-00001.csv")
    c.ev(ev(b"c" * 66_000, "u3", T0, ingest=T0 + 5 * M), ev(b"c" * 66_001, "u3", T0 + 3 * H))
    c.ev(ev(b"d" * 65_535, "u4", T0, ingest=T0 + 5 * M), ev(b"d" * 65_535 + b"x", "u4", T0 + M, ingest=T0 + 5 * M),
         ev(q(b"d" * 65_535), "other", T0, ingest=T0 + M - S, typ="nope"))

    c = case("click_long_ids",
             "click_ids over 65,535 bytes: two clicks at one `ts` whose IDs share a 70,000-byte prefix (the smaller last byte "
             "wins), a 65,535-byte ID tied with the same ID one byte longer, and copies of the long clicks quoted differently",
             R_CREDIT + " / " + R_CLICK_DEDUP)
    prefix = b"k" * 70_000
    c.ev(ev("e1", "u1"), ev("e2", "u2"), ev("e3", "u3"))
    c.ck(ck(prefix + b"2", "u1", "c_2"), ck(prefix + b"1", "u1", "c_1"), ck(q(prefix + b"2"), "u1", "c_2"))
    c.ck(ck(b"j" * 65_536, "u2", "c_1"), ck(b"j" * 65_535, "u2", "c_2"), ck(q(b"j" * 65_535), q("u2"), "c_2"))
    c.ck(ck(b"m" * 65_534 + b"b", "u3", "c_1"), ck(b"m" * 65_534 + b"a", "u3", "c_2"), file="part-00001.csv")
    c.ck(ck(q(prefix + b"1"), "u1", "c_1"), file="part-00001.csv")

    c = case("link_long_user_ids",
             "User IDs of 70,000 bytes that share a 69,999-byte prefix, linked to each other and to a short ID; events and a click "
             "by the long IDs", R_LINK_NAME)
    long_a, long_b = b"u" * 69_999 + b"a", b"u" * 69_999 + b"b"
    c.ev(ev("e1", long_b, T0), ev("e2", long_a, T0 + 40 * M), ev("e3", b"v" * 70_000, T0))
    c.ln(lk(long_b, long_a), lk("zz", long_b), lk(b"v" * 70_000, b"v" * 69_999))
    c.ck(ck("k1", long_a, "c_1"), ck("k2", b"v" * 69_999, "c_2"))


# ---------------------------------------------------------------------------
# 2. Which files are read
# ---------------------------------------------------------------------------
def files():
    c = case("files_many",
             "300 event files, 120 click files and 60 link files, most with one or two lines, every seventh header-only, some "
             "zero-byte; copies of events and clicks spread over files; a link chain with one link per file", R_FILES)
    rng = random.Random(101)
    for n in range(300):
        name = f"part-{n:05d}.csv"
        if n % 7 == 3:
            c.raw(f"events/{name}", EH + b"\n")
        elif n % 41 == 5:
            c.raw(f"events/{name}", b"")
        else:
            u = f"u{n % 37}"
            c.ev(ev(f"e{n}", u, T0 + (n % 50) * 17 * M, typ="purchase", prod=f"p{n % 3}", price=100 + n), file=name)
            if n % 5 == 0:   # a copy in another file, earlier ingest, another user
                c.ev(ev(f"e{n}", f"u{(n + 1) % 37}", T0 + (n % 50) * 17 * M, ingest=T0 - H),
                     file=f"part-{(n * 13 + 1) % 300:05d}x.csv")
    for n in range(120):
        name = f"part-{n:05d}.csv"
        if n % 7 == 2:
            c.raw(f"clicks/{name}", CH + b"\n")
        else:
            c.ck(ck(f"k{n}", f"u{n % 37}", f"c_{1 + n % 2}", T0 - (n % 9) * 20 * H), file=name)
            if n % 4 == 0:
                c.ck(ck(q(f"k{n}"), f"u{n % 37}", q(f"c_{1 + n % 2}"), T0 - (n % 9) * 20 * H), file=f"part-{(n + 60) % 120:05d}b.csv")
    order = list(range(60))
    rng.shuffle(order)
    for n in order:
        name = f"part-{n:05d}.csv"
        if n % 7 == 1:
            c.raw(f"links/{name}", LH + b"\n")
        else:
            c.ln(lk(f"u{n % 37}", f"u{(n + 5) % 37}") if n % 2 else lk(f"u{(n + 5) % 37}", f"u{n % 37}"), file=name)

    c = case("files_odd_names",
             "Next to `part-00000.csv`: files that do not end in `.csv` and must not be read (`x.csv.bak`, `PART.CSV`, `data.Csv`, "
             "`notes.txt`, `part.csv~`, `acsv`, `part.csv.gz`, `part.csvx`, `part.csv ` with a trailing space, `csv`) and files "
             "that do and must be (`a.b.csv`, `my file.csv`, `ünï.csv`, `-.csv`, `part-00001.csv.csv`, `x.CSV.csv`). Each holds "
             "one event, click or link by a user named after the file",
             R_FILES + " (read as a case-sensitive shell glob: the name must end in exactly `.csv`)")
    skip = ["x.csv.bak", "PART.CSV", "data.Csv", "notes.txt", "part.csv~", "acsv", "part.csv.gz", "part.csvx", "part.csv ", "csv"]
    read = ["part-00000.csv", "a.b.csv", "my file.csv", "ünï.csv", "-.csv", "part-00001.csv.csv", "x.CSV.csv"]
    for n, name in enumerate(skip + read):
        tag = ("skip" if name in skip else "read") + str(n)
        c.raw(f"events/{name}", EH + b"\n" + ev(f"e{n}", "ev_" + tag, T0) + b"\n" + ev(f"s{n}", "ck_" + tag, T0) + b"\n"
              + ev(f"l{n}", "lb_" + tag, T0) + b"\n" if name in read else
              EH + b"\n" + ev(f"e{n}", "ev_" + tag, T0) + b"\n")
        c.raw(f"clicks/{name}", CH + b"\n" + ck(f"k{n}", "ck_" + tag if name in read else f"ck_read{len(skip) + n % len(read)}",
                                                "c_1" if name in read else "c_2") + b"\n")
        c.raw(f"links/{name}", LH + b"\n" + (lk("la_" + tag, "lb_" + tag) if name in read else
                                              lk("a_" + tag, f"lb_read{len(skip) + n % len(read)}")) + b"\n")

    c = case("files_underscore_name",
             "A file named `_part-00001.csv` in each folder (Spark and Hadoop skip names starting with `_`; a shell glob `*.csv` "
             "matches them), each with one more event, click and link",
             R_FILES + " (judgement call: every implementation here reads `_x.csv`, because `*.csv` matches it)")
    c.ev(ev("e1", "u1", T0))
    c.ev(ev("e2", "u2", T0), ev("e3", "u1", T0 + M), file="_part-00001.csv")
    c.ck(ck("k1", "u1", "c_1"), file="_part-00001.csv")
    c.ln(lk("u2", "u0"), file="_part-00001.csv")

    c = case("files_subdirectory",
             "A subdirectory `sub/` holding `part-00000.csv`, and a directory that is itself named `nested.csv/` holding a csv, in "
             "each of events/, clicks/ and links/. Each nested file has an event, click or link that would change the output",
             R_FILES + " (judgement call: `*.csv` in a folder does not descend into subfolders, and a directory is not a file)")
    c.ev(ev("e1", "u1", T0))
    c.ck(ck("k0", "u1", "c_2", T0 - 5 * H))
    c.ln(lk("u1", "u1"))
    for sub in ("sub/part-00000.csv", "nested.csv/part-00000.csv"):
        c.raw(f"events/{sub}", EH + b"\n" + ev("e2", "nested", T0) + b"\n" + ev("e1", "nested", T0, ingest=T0 - H) + b"\n")
        c.raw(f"clicks/{sub}", CH + b"\n" + ck("k1", "u1", "c_1") + b"\n")
        c.raw(f"links/{sub}", LH + b"\n" + lk("u1", "a0") + b"\n")

    c = case("files_hidden",
             "Hidden files `.hidden.csv` and `.csv` in each folder next to `part-00000.csv`; each hidden file holds an event by "
             "another user, a click that would take the credit, and a link that would rename `u1`",
             R_FILES + " (ambiguous: a shell glob `*.csv` does not match names starting with `.`; a plain suffix test does)",
             disputed=True)
    c.ev(ev("e1", "u1", T0))
    c.ck(ck("k0", "u1", "c_2", T0 - 5 * H))
    c.ln(lk("u1", "u1"))
    for n, name in enumerate((".hidden.csv", ".csv")):
        c.raw(f"events/{name}", EH + b"\n" + ev(f"h{n}", f"hidden{n}", T0) + b"\n")
        c.raw(f"clicks/{name}", CH + b"\n" + ck(f"k{n + 1}", "u1", "c_1") + b"\n")
        c.raw(f"links/{name}", LH + b"\n" + lk("u1", f"a{n}") + b"\n")


# ---------------------------------------------------------------------------
# 3. Timestamps and windows
# ---------------------------------------------------------------------------
def timestamps():
    c = case("ts_bad_forms_events",
             f"{len(BAD_TS)} bad `ts` values, one event and one user each: every example of the sentence (empty, fractional "
             "seconds, `z`, `t`, a space for `T`, no zone, `1791021600000.0`, `1.79e12`, `-5`, month 13, February 30, February 29 "
             "of 2026, hour 24, second 60, `+24:00`, `+05:60`, `+0200`) and more of each kind (`+5`-style signs, `0x`, `_`, "
             "non-ASCII digits, a Unicode minus, week dates, padding); two controls", R_BADTS)
    for n, raw in enumerate(BAD_TS):
        c.ev(ev(f"e{n}", f"bad{n:03d}", raw))
    c.ev(ev("ok1", "ok", T0), ev("ok2", "ok2", f2(T0, 120)))

    c = case("ts_bad_forms_clicks",
             f"The same {len(BAD_TS)} bad `ts` values on clicks: each user has one session and one click with a bad `ts`, so "
             "every session is organic; one control click is credited", R_BADTS + " / " + R_CLICK_VALID)
    for n, raw in enumerate(BAD_TS):
        c.ev(ev(f"e{n}", f"bad{n:03d}", T0))
        c.ck(ck(f"k{n}", f"bad{n:03d}", "c_1", raw))
    c.ev(ev("ok1", "ok", T0))
    c.ck(ck("kok", "ok", "c_1", f2(T0 - H, -300)))

    c = case("ts_feb29_2027_invalid",
             "Window 2027-02-20 to 2027-03-06, so only the calendar makes these bad: `2027-02-29T10:00:00Z`, `2027-02-29` with "
             "offsets that would land on Feb 28 or Mar 1, `2027-02-30`, `2027-02-31`; valid controls at Feb 28 23:59:59 and Mar 1 "
             "00:00:00, and clicks with the same bad dates", R_BADTS, window=(utc(2027, 2, 20), utc(2027, 3, 6)))
    bad = [b"2027-02-29T10:00:00Z", b"2027-02-29T00:30:00+01:00", b"2027-02-29T23:30:00-01:00", b"2027-02-30T10:00:00Z",
           b"2027-02-31T10:00:00Z", b"2027-02-29T00:00:00Z", b"2027-02-29T23:59:59Z", b"2027-02-29T12:00:00+00:00"]
    for n, raw in enumerate(bad):
        c.ev(ev(f"e{n}", f"bad{n}", raw, at=utc(2027, 2, 28, 10)))
        c.ev(ev(f"s{n}", f"ck{n}", utc(2027, 3, 2, 10)))
        c.ck(ck(f"k{n}", f"ck{n}", "c_1", raw))
    c.ev(ev("ok1", "ok", utc(2027, 2, 28, 23, 59, 59)), ev("ok2", "ok", utc(2027, 3, 1)),
         ev("ok3", "ok3", b"2027-03-01T00:30:00+01:00", at=utc(2027, 2, 28, 23, 30)))
    c.ck(ck("kok", "ok", "c_2", utc(2027, 2, 28, 23, 59, 59)))

    c = case("ts_leap_day_2028",
             "Window 2028-02-20 to 2028-03-05: events on 2028-02-29 in F1, F2 and F3; offsets that move a Feb 29 local date to "
             "Mar 1 UTC and back; a session crossing Feb 29 to Mar 1; `2028-02-30` is bad; a click on Feb 29", R_BADTS,
             window=(utc(2028, 2, 20), utc(2028, 3, 5)))
    leap = utc(2028, 2, 29, 10)
    c.ev(ev("e1", "u1", leap), ev("e2", "u2", f2(leap, 330), at=leap), ev("e3", "u3", f3(leap + 7)),
         ev("e4", "u4", b"2028-02-29T23:30:00-01:00", at=utc(2028, 3, 1, 0, 30)),
         ev("e5", "u5", b"2028-03-01T00:30:00+01:00", at=utc(2028, 2, 29, 23, 30)),
         ev("e6", "u6", utc(2028, 2, 29, 23, 50)), ev("e7", "u6", utc(2028, 3, 1, 0, 10), typ="purchase", price=640),
         ev("e8", "u7", b"2028-02-30T10:00:00Z", at=leap), ev("e9", "u8", utc(2028, 2, 28, 23, 59, 59)),
         ev("e10", "u8", utc(2028, 2, 29, 0, 29, 59)), ev("e11", "u9", b"2028-02-29T24:00:00Z", at=leap))
    c.ck(ck("k1", "u1", "c_1", utc(2028, 2, 29)), ck("k2", "u6", "c_2", b"2028-02-29T23:59:59+23:59"))

    c = case("ts_f3_leading_zeros",
             "F3 with leading zeros padded to 14, 16, 18, 19, 20, 21, 24, 25, 26, 40 and 100 bytes (20 and 25 are the lengths of "
             "F1 and F2), each a valid instant with milliseconds, one user each; 20- and 25-byte digit strings that are out of "
             "window; 20- and 25-byte strings that end like F1/F2 but are neither; the same padded values on clicks", R_F3_ZERO)
    t = T0 + 123
    for n, width in enumerate((14, 16, 18, 19, 20, 21, 24, 25, 26, 40, 100)):
        c.ev(ev(f"e{n}", f"pad{width:03d}", f3(t, width), at=t))
        c.ck(ck(f"k{n}", f"pad{width:03d}", "c_1", f3(t - H, width)))
    for n, raw in enumerate([b"0" * 20, b"0" * 25, b"00000000000086400000", b"0000000000000000086400000",
                             b"0000000179102160000Z", b"0000000000001791021+00:00", b"0000000000001791021-00:00",
                             b"20261003100000000000", b"2026100310000000000000000"]):
        c.ev(ev(f"x{n}", f"out{n}", raw))
    c.ev(ev("s1", "ckout", T0))
    c.ck(ck("kx1", "ckout", "c_1", b"0" * 20), ck("kx2", "ckout", "c_1", b"0" * 25))

    c = case("ts_f3_digit_counts",
             "F3 values with 18, 19 and 20 significant digits, `9223372036854775807`, `9223372036854775808`, "
             "`18446744073709551616` (2^64), 2^64 + a valid instant and 2^63 + a valid instant (wrap to valid in a 64-bit "
             "parser), 2^32-style wraps, 30 and 400 digits, seconds and microseconds since the epoch, `0`, `00`, `86400000`: all "
             "outside the window, on events and clicks; one valid 13-digit control", R_F3)
    t = T0 + 500
    out = [b"9" * 18, b"1" + b"0" * 17, b"9" * 19, b"1" + b"0" * 18, b"9" * 20, b"1" + b"0" * 19, b"9223372036854775807",
           b"9223372036854775808", b"18446744073709551615", b"18446744073709551616", f3(2 ** 64 + t), f3(2 ** 63 + t),
           f3(2 ** 64 * 10 + t), f3(2 ** 32 + t % 2 ** 32), f3(t % 2 ** 32), b"7" * 30, b"3" * 400, f3(t // 1000),
           f3(t * 1000), f3(t * 1_000_000), b"0", b"00", b"86400000", f3(t)[:-1], f3(t) + b"0", b"1" + f3(t)]
    for n, raw in enumerate(out):
        c.ev(ev(f"e{n}", f"out{n:02d}", raw))
        c.ev(ev(f"s{n}", f"ck{n:02d}", T0))
        c.ck(ck(f"k{n}", f"ck{n:02d}", "c_1", raw))
    c.ev(ev("ok", "ok", f3(t)))
    c.ck(ck("kok", "ok", "c_1", f3(t)))

    c = case("ts_f3_5000_digits",
             "F3 `ts` values of 5,000 and 20,000 digits (outside the window), a valid instant padded with zeros to 5,000 bytes, and "
             "purchases priced with 5,000 digits (bad) and `250` padded to 5,000 bytes (valid), on events and clicks",
             R_F3 + " / " + R_F3_ZERO + " / " + R_PRICE, ref_env={"PYTHONINTMAXSTRDIGITS": "0"},
             note="Both references crash on this input with Python's default 4,300-digit limit on int(); the expected output "
                  "was produced with PYTHONINTMAXSTRDIGITS=0.")
    t = T0 + 77
    c.ev(ev("e1", "huge1", b"9" * 5000), ev("e2", "huge2", b"1" + b"0" * 19_999), ev("e3", "padded", f3(t, 5000), at=t),
         ev("e4", "price1", T0, typ="purchase", price=b"9" * 5000), ev("e5", "price2", T0, typ="purchase", price=b"250".zfill(5000)),
         ev("e6", "price3", T0, typ="view", price=b"8" * 5000), ev("e7", "ok", T0))
    c.ck(ck("k1", "ok", "c_1", b"9" * 5000), ck("k2", "padded", "c_2", f3(t - H, 5000)))

    c = case("ts_event_window_edges",
             "Events at exactly `window_start`, 1 ms before it, 1 s before it, `window_end` - 1 ms, `window_end` - 1 s, exactly "
             "`window_end` and 1 ms after, in F3, F1 and F2 forms, one user each", R_EWIN)
    edge = [("start", W0, True), ("start-1ms", W0 - 1, False), ("start-1s", W0 - S, False), ("start+1ms", W0 + 1, True),
            ("end-1ms", W1 - 1, True), ("end-1s", W1 - S, True), ("end", W1, False), ("end+1ms", W1 + 1, False)]
    n = 0
    for label, t, _ in edge:
        forms = [f3(t)]
        if t % S == 0:
            forms += [f1(t), f2(t, 345), f2(t, -600), f3(t, 20)]
        for form in forms:
            c.ev(ev(f"e{n}", f"{label}/{n}", form, at=t))
            n += 1

    c = case("ts_click_window_edges",
             "Clicks at exactly `window_start - 7 days` (credits the session at `window_start`), 1 ms before that, at "
             "`window_end - 1 ms` (credits the session starting at that instant), and at `window_end`; one user each", R_CWIN)
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W0), ev("e3", "u3", f3(W1 - 1)), ev("e4", "u4", f3(W1 - 1)), ev("e5", "u5", W0),
         ev("e6", "u6", f3(W1 - 1)))
    c.ck(ck("k1", "u1", "c_1", W0 - 7 * D), ck("k2", "u2", "c_1", f3(W0 - 7 * D - 1)), ck("k3", "u3", "c_1", f3(W1 - 1)),
         ck("k4", "u4", "c_1", W1), ck("k5", "u5", "c_2", f2(W0 - 7 * D, -720)), ck("k6", "u6", "c_2", f2(W1, 840)),
         ck("k7", "u6", "c_1", W1 - S))

    c = case("ts_offsets_cross_window_edge",
             "F2 values whose local date is outside the window but whose UTC instant is inside, and the reverse, at both edges, "
             "with offsets `+01:00`, `-01:00`, `+23:59`, `-23:59`; the same for the click window", R_F2 + " / " + R_EWIN)
    rows = [("in1", b"2026-09-30T23:00:00-01:00", W0), ("out1", b"2026-10-01T00:30:00+01:00", W0 - 30 * M),
            ("in2", b"2026-10-15T00:30:00+01:00", W1 - 30 * M), ("out2", b"2026-10-14T23:30:00-01:00", W1 + 30 * M),
            ("in3", b"2026-10-15T23:58:59+23:59", W1 - S), ("out3", b"2026-10-15T23:59:00+23:59", W1),
            ("in4", b"2026-09-30T00:01:00-23:59", W0), ("out4", b"2026-09-30T00:00:59-23:59", W0 - S),
            ("in5", b"2026-10-01T23:59:00+23:59", W0), ("out5", b"2026-10-14T00:01:00-23:59", W1)]
    for n, (user, raw, t) in enumerate(rows):
        c.ev(ev(f"e{n}", user, raw, at=t))
    c.ev(ev("s1", "ck1", W0), ev("s2", "ck2", W0))
    c.ck(ck("k1", "ck1", "c_1", b"2026-09-23T23:00:00-01:00"), ck("k2", "ck2", "c_1", b"2026-09-24T00:59:59+01:00"))

    c = case("ts_offsets_cross_midnight",
             "Offsets that change the UTC date, so the session's day differs from the date written: `+02:00`, `-02:00`, `+23:59`, "
             "`-23:59`, `+00:00`, `-00:00`, `+00:01`, `-00:01`, `+05:30`, `+12:45`, `-09:30`, `+14:00`; one user whose three "
             "events are one instant written as F1, F2 and F3", R_F2 + " / " + R_UTC0 + " / " + R_DAY)
    mid = utc(2026, 10, 4)
    offs = [(120, None), (-120, None), (1439, None), (-1439, None), (0, b"+"), (0, b"-"), (1, None), (-1, None), (330, None),
            (765, None), (-570, None), (840, None), (-720, None)]
    for n, (off, sign) in enumerate(offs):
        c.ev(ev(f"a{n}", f"before{n:02d}", f2(mid - M, off, sign), at=mid - M))      # Oct 3 23:59 UTC
        c.ev(ev(f"b{n}", f"after{n:02d}", f2(mid, off, sign), at=mid))               # Oct 4 00:00 UTC
    c.ev(ev("s1", "same", f1(T0)), ev("s2", "same", f2(T0, -240)), ev("s3", "same", f3(T0)), ev("s4", "same", f2(T0, 1439)),
         ev("s5", "same", f2(T0 + 30 * M, -1439), at=T0 + 30 * M), ev("s6", "same", f3(T0 + 60 * M + 1), at=T0 + H))

    c = case("ts_year_0000_9999",
             "`ts` in the years 0000 and 9999 on events and clicks (F1 and F2, including `0000-02-29`, `0000-01-01T00:00:00+23:59` "
             "and `9999-12-31T23:59:59-23:59`, which fall outside 0000-9999 in UTC) and `0000-00-00T00:00:00Z`: all outside the "
             "window or bad; an event ingested in 9999 (late)", R_BADTS + " / " + R_EWIN)
    far = [b"0000-01-01T00:00:00Z", b"0000-02-29T12:00:00Z", b"0000-01-01T00:00:00+23:59", b"0000-12-31T23:59:59-23:59",
           b"9999-12-31T23:59:59Z", b"9999-12-31T23:59:59-23:59", b"9999-01-01T00:00:00+00:00", b"0000-00-00T00:00:00Z",
           b"0001-01-01T00:00:00Z", b"1969-12-31T23:59:59Z", b"1970-01-01T00:00:00Z", b"0000-10-03T10:00:00Z", b"9999-10-03T10:00:00Z"]
    for n, raw in enumerate(far):
        c.ev(ev(f"e{n}", f"far{n:02d}", raw))
        c.ev(ev(f"s{n}", f"ck{n:02d}", T0))
        c.ck(ck(f"k{n}", f"ck{n:02d}", "c_1", raw))
    c.ev(ev("late", "late9999", T0, ingest=b"9999-12-31T23:59:59Z"), ev("ok", "ok", T0))

    c = case("ingest_year_0000",
             "Events whose `ingest_ts` is `0000-01-01T00:00:00Z` (valid F1, long before `ts`): alone (kept, since an early "
             "ingest is fine), and as the earliest copy with a bad `ts` next to a later valid copy (the event is dropped)",
             R_INGEST + " / " + R_LATE + " / " + R_DEDUP, disputed=True,
             note="Year 0000 is not in the sentence listing impossible calendar values.")
    c.ev(ev("e1", "u1", T0, ingest=b"0000-01-01T00:00:00Z"))
    c.ev(ev("e2", "u2", "bad", ingest=b"0000-02-29T00:00:00Z"), ev("e2", "u2", T0, ingest=T0 + M))
    c.ev(ev("e3", "u3", T0, ingest=b"0001-01-01T00:00:00Z"), ev("e4", "u4", T0))


def windows():
    w = (utc(1969, 12, 25), utc(1970, 1, 8))
    c = case("win_epoch_1970",
             "Window 1969-12-25 to 1970-01-08, across the Unix epoch: F3 `0`, `00` and `1` are valid instants; events before 1970 "
             "have negative epoch values (F1/F2 only); a session crossing the epoch; `-0` and `-1` are bad; late at exactly 24 h "
             "after F3 `0`; clicks before and at the epoch", R_F3 + " / " + R_MANIFEST, window=w)
    c.ev(ev("e1", "zero", b"0", ingest=b"1970-01-01T00:01:00Z"), ev("e2", "zero", b"00", ingest=b"1970-01-01T00:01:00Z"),
         ev("e3", "zero", b"1", ingest=b"1970-01-01T00:01:00Z"), ev("e4", "epochf1", b"1970-01-01T00:00:00Z", ingest=b"1970-01-01T00:01:00Z"),
         ev("e5", "neg", utc(1969, 12, 31, 23, 59, 59)), ev("e6", "cross", utc(1969, 12, 31, 23, 50)),
         ev("e7", "cross", b"600000", ingest=b"1970-01-01T00:11:00Z", typ="purchase", price=1969),
         ev("e8", "minus", b"-0", ingest=b"1970-01-01T00:01:00Z"), ev("e9", "minus", b"-1", ingest=b"1970-01-01T00:01:00Z"),
         ev("e10", "late0", b"0", ingest=b"1970-01-02T00:00:00Z"), ev("e11", "late1", b"0", ingest=b"1970-01-02T00:00:01Z"),
         ev("e12", "first", w[0]), ev("e13", "last", f3(w[1] - 1), ingest=w[1]), ev("e14", "f2neg", f2(utc(1969, 12, 28, 10), -480), at=utc(1969, 12, 28, 10)),
         ev("e15", "small", b"86400000", ingest=b"1970-01-02T00:01:00Z"), ev("e16", "smallpad", b"0000000000000", ingest=b"1970-01-01T00:00:00Z"))
    c.ck(ck("k1", "zero", "c_1", b"0"), ck("k2", "cross", "c_2", utc(1969, 12, 24, 23, 50)), ck("k3", "first", "c_1", w[0] - 7 * D),
         ck("k4", "neg", "c_2", b"1969-12-31T23:59:59+00:00"), ck("k5", "small", "c_1", b"86400000"), ck("k6", "epochf1", "c_2", b"000"))

    w = (utc(9999, 12, 1), utc(9999, 12, 15))
    c = case("win_year_9999", "Window 9999-12-01 to 9999-12-15: events in F1, F2 and 15-digit F3, a click 7 days before the window",
             R_MANIFEST, window=w)
    t = utc(9999, 12, 3, 10)
    c.ev(ev("e1", "u1", t), ev("e2", "u1", f3(t + 5 * M + 1), at=t + 5 * M), ev("e3", "u2", f2(t, 600), at=t),
         ev("e4", "u3", w[1] - S, typ="purchase", price=9999), ev("e5", "u4", w[1], ingest=w[1] - S), ev("e6", "u5", w[0]))
    c.ck(ck("k1", "u5", "c_1", w[0] - 7 * D), ck("k2", "u1", "c_2", t))

    w = (utc(2026, 10, 1, 6, 30), utc(2026, 10, 15, 6, 30))
    c = case("win_not_midnight",
             "Window 2026-10-01T06:30:00Z to 2026-10-15T06:30:00Z: events just inside and outside both edges, sessions on 15 "
             "calendar days (Oct 1 to Oct 15), clicks at the click window's start (Sep 24 06:30:00) and 1 ms before", R_MANIFEST + " / " + R_EWIN, window=w)
    c.ev(ev("e1", "u1", w[0] - S), ev("e2", "u2", w[0]), ev("e3", "u3", w[1] - S), ev("e4", "u4", w[1]),
         ev("e5", "u5", utc(2026, 10, 1, 0, 0)), ev("e6", "u6", utc(2026, 10, 15, 0, 0)), ev("e7", "u7", f3(w[1] - 1), ingest=w[1]))
    for day in range(1, 16):
        c.ev(ev(f"d{day}", "daily", utc(2026, 10, day, 6, 30) if day < 15 else utc(2026, 10, 15, 6, 0)))
    c.ck(ck("k1", "u2", "c_1", w[0] - 7 * D), ck("k2", "daily", "c_2", f3(w[0] - 7 * D - 1)), ck("k3", "u3", "c_2", w[1] - 7 * D - S),
         ck("k4", "u6", "c_1", utc(2026, 10, 8, 0, 0)), ck("k5", "u7", "c_1", w[1]))

    w = (utc(2026, 10, 1, 13, 7, 9), utc(2026, 10, 2, 13, 7, 9))
    c = case("win_one_day_odd_start", "A 24-hour window starting at 13:07:09: events at both edges, on two calendar days; clicks up to 7 days before",
             R_MANIFEST, window=w)
    c.ev(ev("e1", "u1", w[0]), ev("e2", "u1", w[0] + 20 * M), ev("e3", "u2", w[1] - S), ev("e4", "u3", w[1]), ev("e5", "u4", w[0] - S),
         ev("e6", "u5", utc(2026, 10, 2, 0, 0, 0), typ="purchase", price=77), ev("e7", "u5", utc(2026, 10, 1, 23, 45)))
    c.ck(ck("k1", "u1", "c_1", w[0] - 7 * D), ck("k2", "u2", "c_2", w[1] - S - 7 * D), ck("k3", "u5", "c_1", w[0] - 7 * D - S))

    w = (T0, T0 + S)
    c = case("win_one_second", "A one-second window: events at its first millisecond, its last millisecond, and just outside", R_MANIFEST, window=w)
    c.ev(ev("e1", "u1", T0), ev("e2", "u1", f3(T0 + 999)), ev("e3", "u2", f3(T0 + 1000)), ev("e4", "u3", f3(T0 - 1)), ev("e5", "u4", f3(T0 + 500)))
    c.ck(ck("k1", "u4", "c_1", f3(T0 + 500)), ck("k2", "u1", "c_2", f3(T0 + 1)), ck("k3", "u1", "c_1", T0 - 7 * D))

    for days, tag in ((3, "win_3_days"), (30, "win_30_days"), (42, "win_42_days"), (43, "win_43_days"), (60, "win_60_days"), (366, "win_366_days")):
        w = (W0, W0 + days * D)
        c = case(tag, f"A {days}-day window from 2026-10-01: events and credited clicks on its first day, its last day and at "
                 "both edges; a click 7 days before the first session; a session on every 5th day",
                 R_MANIFEST + (" (the fast binary stores times as u32 ms from `window_start - 7 days`: about 49.7 days)" if days > 14 else ""),
                 window=w)
        c.ev(ev("e1", "first", w[0]), ev("e2", "last", w[1] - S, typ="purchase", price=days), ev("e3", "after", w[1]),
             ev("e4", "before", w[0] - S), ev("e5", "lastms", f3(w[1] - 1), ingest=w[1]))
        for d in range(0, days, 5):
            c.ev(ev(f"d{d}", "every5", w[0] + d * D + 10 * H))
        c.ck(ck("k1", "first", "c_1", w[0] - 7 * D), ck("k2", "last", "c_2", w[1] - S), ck("k3", "every5", "c_1", w[0] + 9 * H),
             ck("k4", "lastms", "c_1", w[1] - 7 * D - S), ck("k5", "lastms", "c_2", f3(w[1] - 7 * D - 1)))

    w = (utc(2026, 12, 25), utc(2027, 1, 8))
    c = case("win_cross_year", "Window 2026-12-25 to 2027-01-08: a session crossing New Year's midnight, clicks from the week before the window",
             R_MANIFEST + " / " + R_DAY, window=w)
    c.ev(ev("e1", "u1", utc(2026, 12, 31, 23, 50)), ev("e2", "u1", utc(2027, 1, 1, 0, 10), typ="purchase", price=2027),
         ev("e3", "u2", utc(2027, 1, 1)), ev("e4", "u3", b"2027-01-01T00:30:00+01:00", at=utc(2026, 12, 31, 23, 30)))
    c.ck(ck("k1", "u1", "c_1", utc(2026, 12, 24, 23, 50)), ck("k2", "u2", "c_2", utc(2026, 12, 18)))

    c = case("win_manifest_layout",
             "manifest.json on one line without spaces, with `window_end` before `window_start`", R_MANIFEST)
    c.raw("manifest.json", b'{"window_end":"2026-10-15T00:00:00Z","window_start":"2026-10-01T00:00:00Z"}')
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W1 - S), ev("e3", "u3", W1), ev("e4", "u4", W0 - S))

    c = case("win_manifest_whitespace",
             "manifest.json with tabs, blank lines, `\\r\\n` and spaces around every token, keys in reverse order, no final newline", R_MANIFEST)
    c.raw("manifest.json", b'\r\n\t{\r\n\r\n\t"window_end"\t:\r\n   "2026-10-15T00:00:00Z"   ,\r\n\t"window_start" :\t"2026-10-01T00:00:00Z"\r\n}\t ')
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W1 - S), ev("e3", "u3", W1), ev("e4", "u4", W0 - S))

    c = case("win_manifest_extra_keys",
             "manifest.json with extra keys before, between and after the two windows (a seed, counts, a generation time)",
             R_MANIFEST + " (beyond the rules: they show only the two keys)")
    c.raw("manifest.json", b'{"seed": 7, "events": 1000000, "window_start": "2026-10-01T00:00:00Z", '
          b'"clicks": 250000, "window_end": "2026-10-15T00:00:00Z", "generated": "2026-11-01T00:00:00Z"}\n')
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W1 - S), ev("e3", "u3", W1), ev("e4", "u4", W0 - S))

    c = case("win_manifest_tricky_keys",
             "manifest.json where the text `window_start` is also the value of a `note` key and `window_end` is also a key of a "
             "nested object, both before the real keys: only a real JSON parser reads it right",
             R_MANIFEST + " (well beyond the rules: they show only the two keys)")
    c.raw("manifest.json", b'{"note": "window_start", "sizes": {"events": 4, "window_end": 3}, '
          b'"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}\n')
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W1 - S), ev("e3", "u3", W1), ev("e4", "u4", W0 - S))


# ---------------------------------------------------------------------------
# 4. Dedup, validation, late
# ---------------------------------------------------------------------------
def dedup():
    kinds = [
        ("bad_ts", "a bad `ts`", dict(ts="2026-10-03 10:00:00")),
        ("empty_user", "an empty `user_id`", dict(user="")),
        ("bad_type", "the event_type `Purchase`", dict(typ="Purchase", price=100)),
        ("bad_price", "a purchase priced `12.50`", dict(typ="purchase", price="12.50")),
        ("late", "a `ts` two days before its ingest (late)", dict(ts=T0 - 2 * D)),
        ("empty_product", "an empty `product_id`", dict(prod="")),
        ("out_of_window", "a `ts` before the window", dict(ts=W0 - S)),
    ]
    for tag, text, change in kinds:
        c = case(f"dedup_earliest_{tag}",
                 f"`d1` three times: the copy ingested first has {text}; two later copies are valid (one listed before it, one in "
                 "another file). The event is dropped; `u1` keeps only another event three hours later", R_DEDUP)
        fields = dict(eid="d1", user="u1", ts=T0, ingest=T0 + M, typ="view", prod="p1", price="")
        bad = dict(fields, **change)
        c.ev(ev("d1", "u1", T0, ingest=T0 + 5 * M), ev(**bad), ev("ok", "u1", T0 + 3 * H))
        c.ev(ev("d1", "u1", T0, ingest=T0 + 9 * M, typ="purchase", price=500), file="part-00001.csv")
        c.ck(ck("k1", "u1", "c_1", T0 - H))

    c = case("dedup_malformed_earlier_copy",
             "The copy ingested first is malformed (6 fields; 8 fields; a stray quote; an unbalanced quote) or has an empty "
             "event_id: it must not block the later valid copy (`m1`-`m4`, `m6` kept). `m5`'s first copy is well-formed with "
             "empty fields, so it is the kept copy and the event is dropped", R_DEDUP_MAL)
    t, i0 = f1(T0), f1(T0 + M)
    c.ev(b"m1,u1," + t + b"," + i0 + b",view,p1", ev("m1", "u1", T0, ingest=T0 + 5 * M))
    c.ev(ev("m2", "u2", T0, ingest=T0 + 5 * M), b"m2,u2," + t + b"," + i0 + b",view,p1,,")
    c.ev(b'm3,u3,' + t + b"," + i0 + b',vi"ew,p1,', ev("m3", "u3", T0, ingest=T0 + 5 * M))
    c.ev(b'"m4,u4,' + t + b"," + i0 + b",view,p1,", ev("m4", "u4", T0, ingest=T0 + 5 * M))
    c.ev(b"m5,,," + i0 + b",,,", ev("m5", "u5", T0, ingest=T0 + 5 * M))    # 7 fields, well-formed but invalid: this one does block
    c.ev(b",u6," + t + b"," + i0 + b",view,p1,", ev("m6", "u6", T0, ingest=T0 + 5 * M))

    c = case("dedup_across_files",
             "Copies of an event in three files: the earliest copy is in the last file for `x1`, the first file for `x2`, the "
             "middle file for `x3`; each copy names another user and price", R_DEDUP)
    for n, order in enumerate(itertools.permutations(range(3))):
        for k, f in enumerate(order):
            c.ev(ev(f"x{n}", f"u{n}_{k}", T0, ingest=T0 + (k + 1) * M, typ="purchase", price=100 * (k + 1)), file=f"part-0000{f}.csv")

    c = case("dedup_different_users",
             "Copies naming different users, timestamps, types, products and prices: only the earliest counts, for its user, at "
             "its `ts`, with its price; the later users get nothing from it", R_DEDUP)
    c.ev(ev("d1", "late_user", T0 + D, ingest=T0 + D + M, typ="purchase", prod="p2", price=9000),
         ev("d1", "first_user", T0, ingest=T0 + M, typ="purchase", prod="p1", price=111),
         ev("d1", "mid_user", T0 + H, ingest=T0 + H + M, typ="view"))
    c.ev(ev("d2", "first_user", T0 + 10 * M, ingest=T0 + 11 * M, typ="view"),
         ev("d2", "late_user", T0 + 10 * M, ingest=T0 + 20 * M, typ="purchase", prod="p2", price=5000))
    c.ev(ev("d3", "late_user", T0 + 2 * D))
    c.ck(ck("k1", "late_user", "c_1", T0 + D - H), ck("k2", "first_user", "c_2", T0 - H))

    c = case("dedup_three_copies_orders",
             "Three copies of an event in every one of the 6 line orders (one file) and 4 copies in all 24 orders; the earliest "
             "is a purchase of 100, later ones of 200, 300, 400", R_DEDUP)
    for n, order in enumerate(itertools.permutations(range(3))):
        for k in order:
            c.ev(ev(f"t{n}", f"three{n}", T0, ingest=T0 + (k + 1) * M, typ="purchase", price=100 * (k + 1)))
    for n, order in enumerate(itertools.permutations(range(4))):
        for k in order:
            c.ev(ev(f"f{n}", f"four{n:02d}", T0, ingest=T0 + (k + 1) * S, typ="purchase", price=100 * (k + 1)), file=f"part-0000{k % 2}.csv")

    c = case("dedup_quoting_only",
             "Copies with equal `ingest_ts` that differ only in which fields are quoted, in one file and across files: the event "
             "counts once", R_DEDUP_SAME)
    base = [b"q1", b"u1", f1(T0), f1(T0 + M), b"purchase", b"p1", b"250"]
    for mask in (0, 1, 2, 5, 64, 127, 85):
        c.ev(b",".join(q(v) if mask >> k & 1 else v for k, v in enumerate(base)), file=f"part-0000{mask % 2}.csv")
    base = [b"q2", b"u1", f3(T0 + 10 * M + 1), f1(T0 + 11 * M), b"view", b"p2", b""]
    for mask in (0, 127, 4):
        c.ev(b",".join(q(v) if mask >> k & 1 else v for k, v in enumerate(base)))

    c = case("dedup_earliest_valid_later_invalid",
             "The earliest copy is valid and later copies are invalid in every way (bad `ts`, empty user, bad type, bad price, "
             "late, malformed): the event is kept", R_DEDUP)
    c.ev(ev("d1", "", T0, ingest=T0 + 5 * M), ev("d1", "u1", "bad", ingest=T0 + 6 * M), ev("d1", "u1", T0, ingest=T0 + M, typ="purchase", price=321),
         ev("d1", "u1", T0, ingest=T0 + 7 * M, typ="PURCHASE", price=1), ev("d1", "u1", T0, ingest=T0 + 8 * M, typ="purchase", price="x"),
         ev("d1", "u1", T0, ingest=T0 + 3 * D), b"d1,u1")

    c = case("dedup_late_uses_kept_copy",
             "Lateness is judged on the kept copy: `a` has an on-time first copy and a copy ingested 3 days later (kept); `b`'s "
             "first copy carries a `ts` 25 hours before its ingest while the later copy is on time (dropped); `c`'s first copy "
             "was ingested before its `ts` (kept)", R_DEDUP + " / " + R_LATE)
    c.ev(ev("a", "ua", T0, ingest=T0 + M), ev("a", "ua", T0, ingest=T0 + 3 * D))
    c.ev(ev("b", "ub", T0 + 25 * H, ingest=T0 + 25 * H + M), ev("b", "ub", T0, ingest=T0 + 25 * H))
    c.ev(ev("c", "uc", T0, ingest=T0 - 5 * D), ev("c", "uc", T0 - D, ingest=T0 + M))
    c.ev(ev("d", "ud", T0, ingest=T0 + D), ev("d", "ud", T0, ingest=T0 + D + S))

    c = case("dedup_ingest_one_second_apart",
             "Copies ingested exactly one second apart, the smallest whole-second difference, listed latest first; and copies "
             "whose ingest differs only in the year or the day", R_DEDUP)
    c.ev(ev("s1", "wrong", T0, ingest=T0 + M + S), ev("s1", "right1", T0, ingest=T0 + M))
    c.ev(ev("s2", "wrong", T0, ingest=b"2027-10-03T10:00:00Z"), ev("s2", "right2", T0, ingest=b"2026-10-03T10:00:00Z"))
    c.ev(ev("s3", "wrong", T0, ingest=b"2026-10-04T09:59:59Z"), ev("s3", "right3", T0, ingest=b"2026-10-03T23:59:59Z"))
    c.ev(ev("s4", "wrong", T0, ingest=b"2026-10-03T10:10:00Z"), ev("s4", "right4", T0, ingest=b"2026-10-03T10:09:59Z"))


def validation():
    c = case("late_boundaries",
             "`ingest_ts - ts` of exactly 24 h (kept), 24 h - 1 ms (kept), 24 h + 1 ms with an F3 `ts` (dropped), 24 h + 1 s "
             "(dropped), 24 h + 999 ms (dropped), 0 (kept), ingest 1 s, 1 day and 1 year before `ts` (kept), in F1, F2 and F3",
             R_LATE)
    rows = [("exact24h", T0, T0 + D), ("minus1ms", T0 + 1, T0 + D), ("plus1ms", T0 - 1, T0 + D - S + S), ("plus1s", T0, T0 + D + S),
            ("plus999ms", T0 + 1, T0 + D + S), ("zero", T0, T0), ("before1s", T0, T0 - S), ("before1d", T0, T0 - D),
            ("before1y", T0, T0 - 365 * D), ("days5", T0, T0 + 5 * D)]
    for n, (user, t, ing) in enumerate(rows):
        c.ev(ev(f"e{n}", user, t, ingest=ing))
    c.ev(ev("f1", "f2exact24h", f2(T0, 600), ingest=T0 + D, at=T0), ev("f2", "f2plus1s", f2(T0, -600), ingest=T0 + D + S, at=T0),
         ev("f3", "f3exact24h", f3(T0, 20), ingest=T0 + D), ev("f4", "f3plus1ms", f3(T0 + 999), ingest=T0 + D + 2 * S),
         ev("f5", "edge_start", W0, ingest=W0 + D), ev("f6", "edge_end", f3(W1 - 1), ingest=W1 + D - S),
         ev("f7", "edge_end2", f3(W1 - 1), ingest=W1 + D))

    c = case("price_values",
             f"Purchases with {len(BAD_PRICES)} bad prices (`0`, `00`, `10000001`, a 30-digit number, `+5`, `1e3`, ` 5`, `5.0`, "
             "empty, 2^32 + 500, 2^64 + 500, non-ASCII digits, hex, underscores ...) and "
             f"{len(GOOD_PRICES)} valid ones (`1`, `0001`, `10000000`, `010000000`, `250` with 37 leading zeros, `\"500\"`), one user each",
             R_PRICE)
    for n, raw in enumerate(BAD_PRICES):
        c.ev(ev(f"b{n}", f"bad{n:02d}", T0, typ="purchase", price=raw))
    for n, (raw, _) in enumerate(GOOD_PRICES):
        c.ev(ev(f"g{n}", f"good{n}", T0, typ="purchase", price=raw))

    c = case("price_ignored_view_cart",
             "Views and carts carrying every bad and good price from `price_values`: all kept, revenue 0, no top category; one "
             "purchase per user after them", R_PRICE_IGN)
    for n, raw in enumerate(BAD_PRICES + [g for g, _ in GOOD_PRICES]):
        c.ev(ev(f"v{n}", "viewer", T0 + n * S, typ="view", price=raw), ev(f"c{n}", "carter", T0 + n * S, typ="cart", price=raw))
    c.ev(ev("p1", "carter", T0 + 20 * M, typ="purchase", price=42))

    c = case("event_type_variants",
             "event_type `view`, `cart`, `purchase`, `\"view\"` (kept) and `View`, `VIEW`, `Purchase`, `PURCHASE`, `Cart`, `cart `, "
             "` view`, `views`, `purchased`, `vie`, empty, `click`, `purchase\\t`, full-width `ｖｉｅｗ`, `v\u0456ew` with a "
             "Cyrillic `і`, `view;` (dropped), each with a valid price, one user each", R_VALID)
    good = [b"view", b"cart", b"purchase", b'"view"', b'"purchase"']
    bad = [b"View", b"VIEW", b"Purchase", b"PURCHASE", b"Cart", b"cart ", b" view", b"views", b"purchased", b"vie", b"", b"click",
           b"purchase\t", "ｖｉｅｗ".encode(), "v\u0456ew".encode(), b"view;", b"add_to_cart", b"page_view", b"'view'", b"view."]
    for n, raw in enumerate(good):
        c.ev(ev(f"g{n}", f"good{n}", T0, typ=raw, price=100))
    for n, raw in enumerate(bad):
        c.ev(ev(f"b{n}", f"bad{n:02d}", T0, typ=raw, price=100))

    c = case("validate_empty_fields",
             "Each of `user_id`, `ts`, `event_type`, `product_id` empty in turn, bare and as `\"\"`, on views and purchases; an "
             "empty price on a view (kept) and on a purchase (dropped); an empty and a quoted-empty event_id", R_VALID + " / " + R_ID_EMPTY)
    n = 0
    for empty in (b"", b'""'):
        for col in (0, 1, 2, 4, 5, 6):
            for typ, price in ((b"view", b""), (b"purchase", b"100")):
                f = [b"e%d" % n, b"col%d_%d" % (col, n), f1(T0), f1(T0 + M), typ, b"p1", price]
                f[col] = empty
                c.ev(b",".join(f))
                n += 1

    c = case("validate_then_sessions",
             "Dropped events must not leave traces: a user whose only events are invalid has no row; an invalid purchase adds no "
             "revenue and no event count; an invalid event between two valid ones 50 minutes apart does not bridge them", R_VALID)
    c.ev(ev("a1", "ghost", "nope"), ev("a2", "ghost", T0, typ="purchase", price=0), ev("a3", "ghost", T0, ingest=T0 + 2 * D))
    c.ev(ev("b1", "u1", T0), ev("b2", "u1", T0 + 25 * M, typ="purchase", price="1.00"), ev("b3", "u1", T0 + 50 * M))
    c.ev(ev("c1", "u2", T0), ev("c2", "u2", T0 + 25 * M, ingest=T0 + 2 * D), ev("c3", "u2", T0 + 50 * M))
    c.ev(ev("d1", "u3", T0), ev("d2", "u3", T0 + 25 * M, ingest=T0 + 26 * M), ev("d2", "u3", "bad", ingest=T0 + 25 * M), ev("d3", "u3", T0 + 50 * M))
    c.ev(ev("f1", "u4", T0), ev("f2", "u4", T0 + 25 * M, typ="cart"), ev("f3", "u4", T0 + 50 * M))
    c.ck(ck("k1", "ghost", "c_1"), ck("k2", "u1", "c_1", T0 + 40 * M), ck("k3", "u4", "c_2", T0 + 40 * M))


# ---------------------------------------------------------------------------
# 5. Linked IDs
# ---------------------------------------------------------------------------
def links():
    c = case("link_long_chain",
             "A chain of 500 IDs `n001`..`n500` linked one after the next, the link lines shuffled over three files and in random "
             "direction; events by 25 of them, never by `n001`, which names the person", R_LINK_JOIN)
    rng = random.Random(7)
    pairs = [(f"n{i:03d}", f"n{i + 1:03d}") for i in range(1, 500)]
    rng.shuffle(pairs)
    for n, (a, b) in enumerate(pairs):
        c.ln(lk(a, b) if rng.random() < 0.5 else lk(b, a), file=f"part-0000{n % 3}.csv")
    for i in range(20, 501, 20):
        c.ev(ev(f"e{i}", f"n{i:03d}", T0 + (i // 20) * 20 * M))
    c.ck(ck("k1", "n499", "c_1"), ck("k2", "n002", "c_2", T0 - 2 * H))

    c = case("link_cycle",
             "Links forming cycles (`a,b`, `b,c`, `c,a`; a 2-cycle `x,y`, `y,x`), self-links on every ID, and the same lines "
             "repeated many times in both directions", R_LINK_REPEAT)
    c.ln(lk("b", "c"), lk("c", "a"), lk("a", "b"), lk("a", "a"), lk("b", "b"), lk("c", "c"), lk("b", "a"), lk("c", "b"), lk("a", "c"),
         lk("y", "x"), lk("x", "y"), lk("y", "x"), lk("y", "y"), lk("solo", "solo"), lk("solo", "solo"))
    c.ln(*[lk("a", "b")] * 20, file="part-00001.csv")
    c.ev(ev("e1", "c", T0), ev("e2", "b", T0 + 10 * M), ev("e3", "y", T0), ev("e4", "solo", T0), ev("e5", "x", T0 + 31 * M))

    c = case("link_unused_smallest",
             "The byte-wise smallest ID of a person appears in no event or click: once directly linked, once three links away, "
             "once as an upper-case ID (`U9` sorts before `u1`), once as a digit ID", R_LINK_NAME)
    c.ln(lk("u5", "u1"), lk("v9", "v8"), lk("v8", "v7"), lk("v7", "v0"), lk("w1", "U9"), lk("x5", "9"), lk("x5", "10"), lk("y2", "y3"))
    c.ev(ev("e1", "u5"), ev("e2", "v9"), ev("e3", "w1"), ev("e4", "x5"), ev("e5", "y3"))
    c.ck(ck("k1", "v8", "c_1"))

    c = case("link_bytewise_order",
             "Persons whose smallest ID depends on byte order, not locale or UTF-16 order: `Z`<`a`, `_x`<`ax` but `Zx`<`_x`, "
             "`u10`<`u9`, `u1`<`u1 `, `z`<`è`, `zz`<`ä`, `B`<`a`<`b`, `u-1`<`u.1`<`u_1`, `10`<`9`, `~`>`z`, U+FFFF < U+10000 "
             "(the reverse in UTF-16), ` u`<`u`, `A`<`a`, `u1`<`u1a`, `u\\t`<`u `", R_LINK_NAME)
    groups = [["a", "Z"], ["ax", "_x"], ["_y", "Zy", "ay"], ["u9", "u10"], ["u1 ", "u1"], ["è", "z"], ["ä", "zz"], ["b", "a2", "B"],
              ["u_1", "u.1", "u-1"], ["9", "10"], ["~", "zzz"], ["\U00010000", "\uffff"], ["u", " u"], ["a3", "A3"], ["u1a!", "u1a"],
              ["u ", "u\t"], ["ÿ", "Ā"], ["Ω", "ω", "ö"], ["e\u0301", "\u00e9"]]
    for n, ids in enumerate(groups):
        for a, b in zip(ids, ids[1:]):
            c.ln(lk(a, b))
        c.ev(ev(f"e{n}", ids[0], T0), ev(f"f{n}", ids[-1], T0 + 5 * M))

    c = case("link_multi_files",
             "Two groups joined only by a link in a third file; link files read in any order give the same people", R_LINK_JOIN)
    c.ln(lk("g1", "g2"), lk("g3", "g4"), file="part-00000.csv")
    c.ln(lk("h1", "h2"), lk("h2", "h3"), file="part-00001.csv")
    c.ln(lk("h3", "g4"), lk("g2", "g3"), file="part-00002.csv")
    c.raw("links/part-00003.csv", LH + b"\n")
    c.ev(ev("e1", "h1", T0), ev("e2", "g1", T0 + 20 * M), ev("e3", "h3", T0 + 40 * M), ev("e4", "g4", T0 + 3 * H))
    c.ck(ck("k1", "g3", "c_1"))

    c = case("link_empty_sides",
             "Link lines with an empty side (`u1,`, `,u2`, `,`, `\"\",u2`, `u1,\"\"`, `\"\",\"\"`): dropped, so they do not "
             "join `u1` and `u2` through the empty ID", R_LINK_PARSE)
    c.ln(b"u1,", b",u2", b",", b'"",u2', b'u1,""', b'"",""', b"u3,", b",u3", lk("u4", "u5"))
    c.ev(ev("e1", "u1"), ev("e2", "u2"), ev("e3", "u3"), ev("e4", "u5"), ev("e5", "", T0))

    c = case("link_interleaved_sessions",
             "One person with three IDs whose events interleave 30 minutes apart: one session across all three; the same pattern "
             "for three unlinked IDs gives separate rows", R_SESS_ALL)
    c.ln(lk("a2", "a1"), lk("a3", "a2"))
    for prefix in "ab":
        for k, uid in enumerate([1, 2, 3, 1, 2, 3, 1]):
            c.ev(ev(f"{prefix}{k}", f"{prefix}{uid}", T0 + k * 30 * M, typ="purchase" if k == 4 else "view", price=500 if k == 4 else ""))
        c.ev(ev(f"{prefix}9", f"{prefix}3", f3(T0 + 7 * 30 * M + 1)))

    c = case("link_shared_device",
             "A shared device ID `dev` links 40 people, each with two IDs of their own, into one person; their events interleave "
             "across two days; clicks come from many of the IDs", R_LINK_JOIN + " / " + R_SESS_ALL)
    rng = random.Random(11)
    for p in range(40):
        c.ln(lk(f"p{p:02d}a", f"p{p:02d}b"), file="part-00000.csv")
        c.ln(lk("dev", f"p{p:02d}{'ab'[p % 2]}") if p % 3 else lk(f"p{p:02d}{'ab'[p % 2]}", "dev"), file="part-00001.csv")
        for k in range(4):
            t = T0 + rng.randrange(0, 2 * D // (5 * M)) * 5 * M
            c.ev(ev(f"e{p}_{k}", f"p{p:02d}{'ab'[k % 2]}", t, typ="purchase" if k == 3 else "view", prod=f"p{1 + p % 2}", price=100 + p if k == 3 else ""))
        if p % 4 == 0:
            c.ck(ck(f"k{p}", f"p{p:02d}b", f"c_{1 + p % 2}", T0 - rng.randrange(0, 5 * D // H) * H))
    c.ev(ev("solo1", "q1", T0), ev("dev1", "dev", T0 + M))

    c = case("link_case_split",
             "Upper-cased link sides are other IDs: `U1,u2` and `u1,u3` make two people (`U1`+`u2`, `u1`+`u3`); `A,b` and `a,B` "
             "do not join `a` and `b`", R_LINK_EXACT)
    c.ln(lk("U1", "u2"), lk("u1", "u3"), lk("A", "b"), lk("a", "B"), lk("m", "M"))
    for n, u in enumerate(["u1", "u2", "u3", "U1", "a", "b", "m", "M"]):
        c.ev(ev(f"e{n}", u, T0 + n * M))

    c = case("link_duplicates_both_ways",
             "Every link line repeated three times, reversed, quoted and across two files", R_LINK_REPEAT)
    for a, b in (("u1", "u2"), ("u2", "u3"), ("u7", "u8")):
        c.ln(lk(a, b), lk(b, a), lk(q(a), q(b)), lk(a, b))
        c.ln(lk(b, a), lk(q(b), a), file="part-00001.csv")
    c.ev(ev("e1", "u3"), ev("e2", "u1", T0 + M), ev("e3", "u8"), ev("e4", "u9"))

    c = case("link_click_only_id",
             "A person whose clicks all come from an ID with no events, which is also the smallest ID; another whose linked ID "
             "appears only in a click that is too old", R_LINK_FIRST + " / " + R_CREDIT)
    c.ln(lk("m5", "a_click"), lk("n5", "b_click"))
    c.ev(ev("e1", "m5", T0), ev("e2", "n5", T0))
    c.ck(ck("k1", "a_click", "c_1"), ck("k2", "b_click", "c_2", f3(T0 - 7 * D - 1)))

    c = case("link_quoted_and_spaces",
             "Links with quoted sides and with spaces: `\"u1\",\"u2\"` joins u1 and u2; ` u1,u4` joins the different ID ` u1`; "
             "`\"u1 \",u5` joins `u1 `", R_SPACE)
    c.ln(b'"u1","u2"', b'"u1",u3', b" u1,u4", b'"u1 ",u5', b"u6 ,u7", b"u8,u9 ")
    for n, u in enumerate(["u1", "u2", "u3", "u4", "u5", " u1", "u1 ", "u6", "u7", "u8", "u9"]):
        c.ev(ev(f"e{n}", u, T0 + n * M))

    c = case("link_merges_change_sessions",
             "Linking changes sessions and credit, not just names: two IDs with events 20 minutes apart become one session; a "
             "purchase by one ID sets the other's top category; unlinked twins behave separately", R_LINK_FIRST)
    c.ln(lk("j2", "j1"))
    for prefix in "jx":
        c.ev(ev(f"{prefix}a", f"{prefix}1", T0), ev(f"{prefix}b", f"{prefix}2", T0 + 20 * M, typ="purchase", price=800),
             ev(f"{prefix}c", f"{prefix}1", T0 + 45 * M), ev(f"{prefix}d", f"{prefix}2", T0 + 5 * H))
        c.ck(ck(f"k{prefix}", f"{prefix}2", "c_1", T0 - H))

    c = case("link_self_only", "IDs linked only to themselves stay separate people named by themselves", R_LINK_REPEAT)
    c.ln(lk("u5", "u5"), lk("u6", "u6"), lk("u0", "u0"))
    c.ev(ev("e1", "u5"), ev("e2", "u6"))

    c = case("link_many_small_groups",
             "300 people of two to four IDs each, links shuffled; every ID has one event at the same time of day, so each person "
             "has one session", R_LINK_JOIN)
    rng = random.Random(13)
    lines = []
    for p in range(300):
        size = 2 + p % 3
        ids = [f"g{p:03d}{'dcba'[k]}" for k in range(size)]
        for k in range(1, size):
            lines.append(lk(ids[k], ids[rng.randrange(k)]))
        for k, uid in enumerate(ids):
            c.ev(ev(f"e{p}_{k}", uid, T0 + k * 10 * M))
    rng.shuffle(lines)
    c.ln(*lines)


# ---------------------------------------------------------------------------
# 6. Sessions
# ---------------------------------------------------------------------------
def sessions():
    c = case("sess_gap_exact",
             "Gaps of exactly 30:00.000 (one session) and 30:00.001 (two), 29:59.999, 30:01; a chain of ten events each exactly "
             "30 minutes apart (one session) and each 30:00.001 apart (ten sessions); events listed in reverse order; the gap is "
             "from the previous event, not from the session start; F2 forms 30 minutes apart", R_SESS)
    g = 30 * M
    c.ev(ev("a1", "exact", T0), ev("a2", "exact", T0 + g))
    c.ev(ev("b1", "over1ms", T0), ev("b2", "over1ms", f3(T0 + g + 1)))
    c.ev(ev("c1", "under1ms", T0), ev("c2", "under1ms", f3(T0 + g - 1)))
    c.ev(ev("d1", "over1s", T0), ev("d2", "over1s", T0 + g + S))
    for k in range(10):
        c.ev(ev(f"e{k}", "chain_exact", T0 + k * g), ev(f"f{k}", "chain_over", f3(T0 + k * (g + 1)) if k else f1(T0)))
    for k in reversed(range(6)):
        c.ev(ev(f"g{k}", "reverse", T0 + k * 20 * M))
    c.ev(ev("h1", "f2", f2(T0, 120)), ev("h2", "f2", f2(T0 + g, -240), at=T0 + g), ev("h3", "f2", f3(T0 + 2 * g + 1), at=T0 + 2 * g))
    c.ev(ev("i1", "msbase", f3(T0 + 999)), ev("i2", "msbase", f3(T0 + 999 + g)), ev("i3", "msbase", f3(T0 + 1000 + 2 * g)))

    c = case("sess_equal_ts_around_gap",
             "Many events at one timestamp on both sides of a 30:00.000 and a 30:00.001 gap, listed in shuffled order and split "
             "over two files; identical instants written in different forms", R_SESS_EQ)
    g = 30 * M
    rng = random.Random(17)
    lines = []
    for k in range(5):
        lines += [ev(f"a{k}", "u1", [f1(T0), f3(T0), f2(T0, 60)][k % 3]), ev(f"b{k}", "u1", f3(T0 + g + 1), typ="purchase" if k == 2 else "view", price=300 if k == 2 else ""),
                  ev(f"c{k}", "u1", f3(T0 + 2 * g + 1), at=T0 + 2 * g)]
    for k in range(4):
        lines += [ev(f"d{k}", "u2", T0), ev(f"e{k}", "u2", [f1(T0 + g), f3(T0 + g)][k % 2], at=T0 + g), ev(f"f{k}", "u2", f3(T0 + 2 * g + 1), at=T0 + 2 * g)]
    rng.shuffle(lines)
    c.ev(*lines[:len(lines) // 2])
    c.ev(*lines[len(lines) // 2:], file="part-00001.csv")
    c.ck(ck("k1", "u1", "c_1", f3(T0 + g + 1)), ck("k2", "u2", "c_2", f3(T0 + 2 * g + 1)))

    c = case("sess_cross_midnight",
             "Sessions crossing midnight count on their start day with all their events and purchases; starts at 23:59:59, "
             "23:59:59.999 and 00:00:00.000; an F2 start whose local date is the next day", R_DAY)
    mid = utc(2026, 10, 4)
    c.ev(ev("a1", "u1", mid - 10 * M), ev("a2", "u1", mid + 10 * M, typ="purchase", prod="p1", price=700),
         ev("a3", "u1", mid + 35 * M, typ="purchase", prod="p2", price=300), ev("a4", "u1", mid + 12 * H))
    c.ev(ev("b1", "u2", mid - S), ev("b2", "u2", mid), ev("c1", "u3", f3(mid - 1)), ev("c2", "u3", f3(mid + 1)), ev("d1", "u4", mid),
         ev("d2", "u4", mid + S), ev("f1", "u5", b"2026-10-04T01:30:00+02:00", at=mid - 30 * M), ev("f2", "u5", mid + 20 * M, typ="purchase", price=50))
    c.ck(ck("k1", "u1", "c_1", mid - H), ck("k2", "u1", "c_2", mid + 5 * M), ck("k3", "u4", "c_2", mid))

    c = case("sess_window_last_midnight",
             "A session that would cross the window's last midnight: its events at and after `window_end` are dropped; one that "
             "would start before `window_start`: it starts at its first event inside; one crossing an inner midnight", R_EWIN + " / " + R_DAY)
    c.ev(ev("a1", "u1", W1 - 15 * M), ev("a2", "u1", W1 - S, typ="purchase", price=900), ev("a3", "u1", W1, typ="purchase", price=5000),
         ev("a4", "u1", W1 + 10 * M))
    c.ev(ev("b1", "u2", W0 - 10 * M), ev("b2", "u2", W0 + 10 * M), ev("b3", "u2", W0 + 20 * M))
    c.ev(ev("c1", "u3", W0 - S), ev("c2", "u3", W1), ev("d1", "u4", W1 - D - 10 * M), ev("d2", "u4", W1 - D + 20 * M))
    c.ck(ck("k1", "u2", "c_1", W0 - 5 * M), ck("k2", "u1", "c_2", W1 - 10 * M), ck("k3", "u2", "c_2", W0 + 5 * M))

    c = case("sess_multi_id_interleave",
             "A person with IDs `a1`, `a2`, `a3` (linked in a chain): events alternate between the IDs exactly 30:00.000 apart, "
             "then one 30:00.001 later; a second person's two IDs each have gaps over 30 minutes that the other ID bridges",
             R_SESS_ALL)
    g = 30 * M
    c.ln(lk("a3", "a2"), lk("a1", "a2"), lk("b1", "b2"))
    for k in range(6):
        c.ev(ev(f"a{k}", f"a{1 + k % 3}", T0 + k * g))
    c.ev(ev("a9", "a2", f3(T0 + 6 * g + 1)))
    for k in range(8):
        c.ev(ev(f"b{k}", f"b{1 + k % 2}", T0 + k * 20 * M))

    c = case("sess_long_chain_days",
             "One session lasting three days (an event every 30 minutes from Oct 3 10:00), with a purchase on each later day: "
             "one row on Oct 3 with 145 events; a second user with the same events 30:00.001 apart has 145 sessions on 4 days", R_DAY)
    for k in range(145):
        c.ev(ev(f"a{k}", "marathon", T0 + k * 30 * M, typ="purchase" if k in (60, 120) else "view", price=1000 if k in (60, 120) else ""))
        c.ev(ev(f"b{k}", "sprinter", f3(T0 + k * (30 * M + 1)) if k else f1(T0), typ="purchase" if k in (60, 120) else "view", price=1000 if k in (60, 120) else ""))
    c.ck(ck("k1", "marathon", "c_1", T0 + D), ck("k2", "sprinter", "c_2", T0 + D))

    c = case("sess_duplicates_count_once",
             "Duplicated events inside sessions: copies (same or other files, later ingest) add nothing to `events` or revenue", R_DEDUP)
    for k in range(5):
        c.ev(ev(f"e{k}", "u1", T0 + k * M, ingest=T0 + H, typ="purchase", price=100))
        c.ev(ev(f"e{k}", "u1", T0 + k * M, ingest=T0 + H + (k + 1) * S, typ="purchase", price=100), file="part-00001.csv")
        c.ev(ev(f"e{k}", "u1", T0 + k * M, ingest=T0 + 2 * H, typ="purchase", price=100))

    c = case("sess_many_per_day",
             "Twenty sessions in one day (events 31 minutes apart) and sessions on all 14 days of the window for one user", R_SESS)
    for k in range(20):
        c.ev(ev(f"a{k}", "busy", utc(2026, 10, 5) + k * 31 * M))
    for d in range(14):
        c.ev(ev(f"b{d}", "daily", W0 + d * D + 12 * H, typ="purchase", prod=f"p{1 + d % 2}", price=100 + d))
    c.ck(ck("k1", "daily", "c_1", W0 + 11 * H), ck("k2", "daily", "c_2", W0 + 8 * D))


# ---------------------------------------------------------------------------
# 7. Clicks, credit, assists
# ---------------------------------------------------------------------------
def clicks():
    c = case("click_start_edges",
             "A click exactly at the session start (credits), 1 ms after (does not), 1 ms before, 1 s after; the same with an F3 "
             "session start at .500 and an F2 click at the same instant", R_CREDIT)
    for u in ("at", "after1ms", "before1ms", "after1s", "msat", "msafter", "f2at"):
        c.ev(ev("e_" + u, u, f3(T0 + 500) if u.startswith("ms") else T0))
    c.ck(ck("k1", "at", "c_1", T0), ck("k2", "after1ms", "c_1", f3(T0 + 1)), ck("k3", "before1ms", "c_1", f3(T0 - 1)),
         ck("k4", "after1s", "c_1", T0 + S), ck("k5", "msat", "c_1", f3(T0 + 500)), ck("k6", "msafter", "c_1", f3(T0 + 501)),
         ck("k7", "f2at", "c_2", f2(T0, -765)))

    c = case("click_7d_edges",
             "A click exactly 7 days before the session start (credits), 7 days + 1 ms (does not), 7 days - 1 ms, 7 days + 1 s; a "
             "session at .001 with a click exactly 7 days before the whole second; one click that is in range for the first of "
             "two sessions and out of range for the second", R_CREDIT)
    w = 7 * D
    for u in ("exact", "over1ms", "under1ms", "over1s"):
        c.ev(ev("e_" + u, u, T0))
    c.ev(ev("e_ms", "msstart", f3(T0 + 1)), ev("e_two1", "two", T0), ev("e_two2", "two", T0 + 2 * H))
    c.ck(ck("k1", "exact", "c_1", T0 - w), ck("k2", "over1ms", "c_1", f3(T0 - w - 1)), ck("k3", "under1ms", "c_1", f3(T0 - w + 1)),
         ck("k4", "over1s", "c_1", T0 - w - S), ck("k5", "msstart", "c_1", T0 - w), ck("k6", "two", "c_2", T0 - w + H))

    camps = [(f"c_{n}", f"ch{n}") for n in range(1, 7)]
    c = case("click_tie_ids",
             "Clicks at one `ts` for different campaigns; the byte-wise smallest click_id takes the credit: `K9` < `k1` < `k10`, "
             "`k10` < `k9`, `kAa` < `k_a`, ` k1` < `k1` < `k1 `, `z` < `é`, `10` < `9`, `B` < `a`, `k-1` < `k.1` < `k_1`, U+FFFF "
             "< U+1F600, a quoted `\"a\"` is `a` (not smaller than `B`), `k` < `k0`; and a later click with a larger ID beats an "
             "earlier one with a smaller ID. Each group's IDs carry a common prefix `<n>:` so that click IDs stay unique",
             R_CREDIT, campaigns=camps)
    ties = [["k1", "k10", "K9"], ["k10", "k1"], ["k9", "k10"], ["k_a", "kAa"], ["k1", " k1"], ["k1 ", "k1"], ["é", "z"], ["9", "10"],
            ["a", "B"], ["k_1", "k.1", "k-1"], ["😀", "\uffff"], ['"a', "B"], ["k0", "k"], ["kk", "kK", "Kk"], ["0", "00", "000"],
            ["k\t", "k "], ["Ā", "ÿ"]]
    for n, ids in enumerate(ties):
        user = f"tie{n:02d}"
        c.ev(ev(f"e{n}", user, T0))
        for k, cid in enumerate(ids):
            # click IDs are unique across the file: each group shares the prefix `<n>:`, which leaves the order within it alone
            full = q(f"{n}:{cid[1:]}") if cid.startswith('"') else f"{n}:{cid}"
            c.ck(ck(full, user, f"c_{k + 1}", T0 - H), file=f"part-0000{(n + k) % 2}.csv")
    c.ev(ev("el", "latest", T0))
    c.ck(ck("zz", "latest", "c_5", T0 - H), ck("aa", "latest", "c_6", T0 - 2 * H))

    c = case("click_dup_quoted",
             "A click repeated five times with different quoting, in two files, among other clicks: one credit and one assist; "
             "copies of a tied click do not change the tie", R_CLICK_DEDUP)
    c.ev(ev("e1", "u1", T0), ev("e2", "u1", T0 + 2 * H), ev("e3", "u2", T0))
    base = [b"k1", b"u1", b"c_1", f1(T0 - H)]
    for mask in (0, 1, 6, 15, 8):
        c.ck(b",".join(q(v) if mask >> k & 1 else v for k, v in enumerate(base)), file=f"part-0000{mask % 2}.csv")
    c.ck(ck("k3", "u2", "c_2", T0 - H), ck("k2", "u2", "c_1", T0 - H), ck(q("k3"), "u2", q("c_2"), T0 - H), ck("k3", "u2", "c_2", T0 - H))

    c = case("click_from_linked_id",
             "Clicks from linked IDs: directly linked, three links away, from the ID that names the person, and a tie between "
             "clicks of two IDs of one person", R_CREDIT + " / " + R_LINK_FIRST)
    c.ln(lk("u1", "u2"), lk("v1", "v2"), lk("v2", "v3"), lk("v3", "v4"), lk("w2", "w1"), lk("x1", "x2"))
    c.ev(ev("e1", "u1"), ev("e2", "v1"), ev("e3", "w2"), ev("e4", "x1"), ev("e5", "y1"))
    c.ck(ck("k1", "u2", "c_1"), ck("k2", "v4", "c_2"), ck("k3", "w1", "c_1"), ck("k5", "x2", "c_2"), ck("k4", "x1", "c_1"),
         ck("k6", "y2", "c_1"))

    c = case("click_before_event_window",
             "Clicks in the 7 days before `window_start` crediting sessions on the window's first day; one exactly 7 days before "
             "a session at `window_start`; one 7 days + 1 s before a session at 00:00:01", R_CWIN + " / " + R_CREDIT)
    c.ev(ev("e1", "u1", W0), ev("e2", "u2", W0 + S), ev("e3", "u3", W0 + 10 * H), ev("e4", "u4", W0 + 3 * D))
    c.ck(ck("k1", "u1", "c_1", W0 - 7 * D), ck("k2", "u2", "c_1", W0 - 7 * D), ck("k3", "u3", "c_2", W0 - S),
         ck("k4", "u4", "c_2", W0 - 4 * D), ck("k5", "u4", "c_1", f3(W0 - 4 * D - 1)))

    c = case("click_unknown_campaign_variants",
             "Clicks for campaigns not in campaigns.csv: `C_1`, `c_1 `, ` c_1`, `c_01`, `c_999`, empty, `organic`, `c_1\\r`, "
             "`c_10`, `c_`, `\"c_1 \"`; every session stays organic; `\"c_1\"` (quoted) is known", R_CLICK_VALID)
    names = [b"C_1", b"c_1 ", b" c_1", b"c_01", b"c_999", b"", b"organic", b"c_1\r", b"c_10", b"c_", b'"c_1 "', b"c-1", b"c_1.0", b"1"]
    for n, name in enumerate(names):
        c.ev(ev(f"e{n}", f"unk{n:02d}", T0))
        c.ck(ck(f"k{n}", f"unk{n:02d}", name))
    c.ev(ev("eok", "known", T0))
    c.ck(ck("kok", "known", b'"c_1"'))

    c = case("click_unknown_does_not_shadow",
             "An invalid click (unknown campaign, bad `ts`, empty user) later than a valid one must not take the credit or hide "
             "it; an invalid click must not win a tie", R_CLICK_VALID + " / " + R_CREDIT)
    c.ev(ev("e1", "u1", T0), ev("e2", "u2", T0), ev("e3", "u3", T0))
    c.ck(ck("k1", "u1", "c_1", T0 - 2 * H), ck("k2", "u1", "c_999", T0 - H), ck("k3", "u2", "c_2", T0 - 2 * H), ck("k4", "u2", "c_1", "2026-10-03 09:30:00"),
         ck("a0", "u3", "C_1", T0 - H), ck("a1", "u3", "c_2", T0 - H), ck("a2", "", "c_1", T0 - M))

    c = case("click_assist_only_campaign",
             "A campaign that only assists (an older click) has a row with 0 sessions, 0 purchasing sessions and 0 revenue even "
             "though the session has a purchase", R_ASSIST_ROW)
    c.ev(ev("e1", "u1", T0, typ="purchase", price=1234), ev("e2", "u2", T0 + D))
    c.ck(ck("k1", "u1", "c_1", T0 - 2 * H), ck("k2", "u1", "c_2", T0 - H), ck("k3", "u2", "c_2", T0 + D - H))

    c = case("click_same_campaign_many",
             "Five clicks of one campaign and two of another in the 7 days before a session: each campaign assists once", R_ASSIST)
    c.ev(ev("e1", "u1", T0), ev("e2", "u1", T0 + D))
    for k in range(5):
        c.ck(ck(f"a{k}", "u1", "c_1", T0 - (k + 1) * H))
    c.ck(ck("b1", "u1", "c_2", T0 - 6 * D), ck("b2", "u1", "c_2", T0 - 6 * D - H))

    c = case("click_one_credits_many",
             "One click and sessions 1 hour, 6 hours, 1 day, 6 days, exactly 7 days and 7 days + 31 min after it: it credits "
             "and assists every session within 7 days, each on its own day; the last session is organic", R_MANY)
    k = utc(2026, 10, 3, 9)
    for n, delta in enumerate((H, 6 * H, D, 6 * D, 7 * D, 7 * D + 31 * M)):
        c.ev(ev(f"e{n}", "u1", k + delta, typ="purchase", price=100 * (n + 1)))
    c.ck(ck("k1", "u1", "c_1", k))

    c = case("click_assist_older_same_campaign",
             "The credited click's campaign also has older clicks (inside and outside the 7 days), and another campaign has one "
             "inside and one outside: assists count campaigns, once each, only from clicks inside", R_ASSIST)
    c.ev(ev("e1", "u1", T0 + 6 * D))
    t = T0 + 6 * D
    c.ck(ck("k1", "u1", "c_1", t - 3 * D), ck("k2", "u1", "c_1", t - H), ck("k3", "u1", "c_2", t - 2 * D),
         ck("k4", "u1", "c_1", t - 8 * D), ck("k5", "u1", "c_2", f3(t - 7 * D - 1)))
    c.ev(ev("e2", "u2", t))
    c.ck(ck("k6", "u2", "c_2", t - 7 * D - S), ck("k7", "u2", "c_1", t - 7 * D))

    c = case("click_only_person",
             "People with clicks and no surviving events (no events at all; only invalid events; only late events) produce no "
             "row in either table", R_MANY)
    c.ev(ev("e1", "u2", T0), ev("e2", "bad", "nope"), ev("e3", "late", T0, ingest=T0 + 3 * D))
    c.ck(ck("k1", "u1", "c_1"), ck("k2", "bad", "c_1"), ck("k3", "late", "c_2"), ck("k4", "ghost", "c_2"))
    c.ln(lk("ghost", "ghost2"))

    c = case("click_campaign_odd_chars",
             "campaigns.csv with odd names: `c 1`, `ç_1`, `c_1!`, `C_1` next to `c_1`, an emoji, `c_1 ` with a trailing space, "
             "`c-1`, `Organic`, `unknown`, a 300-byte ID; channels with spaces, upper case and UTF-8; one credited session each",
             R_CLEAN, campaigns=[("c_1", "search"), ("C_1", "Search"), ("c 1", "paid social"), ("ç_1", "réseau"), ("c_1!", "e-mail"),
                                 ("c_😀", "😀"), ("c_1 ", " display"), ("c-1", "search"), ("Organic", "Organic"), ("unknown", "unknown"),
                                 ("c_" + "9" * 298, "x" * 300), ("0", "0")])
    for n, (camp, _) in enumerate(c.campaigns):
        c.ev(ev(f"e{n}", f"u{n:02d}", T0, typ="purchase", price=100 + n))
        c.ck(ck(f"k{n}", f"u{n:02d}", camp))

    c = case("click_empty_fields",
             "Clicks with an empty click_id, user_id, campaign_id or `ts` (bare and `\"\"`): none credits", R_CLICK_PARSE + " / " + R_CLICK_VALID)
    n = 0
    for empty in (b"", b'""'):
        for col in range(4):
            user = b"col%d_%d" % (col, n)
            c.ev(ev(f"e{n}", user, T0))
            f = [b"k%d" % n, user, b"c_1", f1(T0 - H)]
            f[col] = empty
            c.ck(b",".join(f))
            n += 1
    c.ev(ev("eok", "", T0), ev("eok2", "ok", T0))
    c.ck(ck("kok", "ok", "c_2"))

    c = case("click_mid_session",
             "A click after the session start credits only later sessions, even when it shares a `ts` with a later event of the "
             "session; clicks never split a session", R_MANY)
    c.ev(ev("e1", "u1", T0), ev("e2", "u1", T0 + 25 * M), ev("e3", "u1", T0 + 50 * M), ev("e4", "u1", T0 + 2 * H),
         ev("e5", "u2", T0), ev("e6", "u2", T0 + 29 * M), ev("e7", "u2", T0 + 58 * M))
    c.ck(ck("k1", "u1", "c_1", T0 + 25 * M), ck("k2", "u2", "c_2", T0 + 30 * M), ck("k3", "u2", "c_1", f3(T0 + 1)))

    camps = [(f"c_{n}", f"ch{n % 3}") for n in range(1, 9)]
    c = case("click_window_slide",
             "One person, 60 sessions over the 14 days and 150 clicks of 8 campaigns over 21 days, at seeded pseudo-random "
             "whole-minute times: campaigns enter and leave the 7-day window from session to session", R_ASSIST, campaigns=camps)
    rng = random.Random(23)
    for n in range(60):
        c.ev(ev(f"e{n}", f"u{n % 2}", W0 + rng.randrange(14 * 24 * 60) * M, typ="purchase" if n % 4 == 0 else "view", price=100 + n if n % 4 == 0 else ""))
    for n in range(150):
        c.ck(ck(f"k{rng.randrange(10 ** 6):06d}", f"u{n % 2}", f"c_{1 + min(rng.randrange(8), rng.randrange(9)) % 8}",
                W0 - 7 * D + rng.randrange(21 * 24 * 60) * M))
    c.ln(lk("u0", "u1"))

    c = case("click_user_case", "Clicks by `U1`, `u1 ` and ` u1` do not credit `u1`'s session; `\"u1\"` does", R_STR)
    c.ev(ev("e1", "u1", T0), ev("e2", "u2", T0))
    c.ck(ck("k1", "U1", "c_1"), ck("k2", "u1 ", "c_1"), ck("k3", " u1", "c_1"), ck("k4", b'"u2"', "c_2"))

    c = case("click_latest_wins",
             "The latest qualifying click wins whatever its ID, campaign or file order; a later click 1 ms after the start is "
             "ignored; with millisecond timestamps 1 ms apart", R_CREDIT)
    c.ev(ev("e1", "u1", T0), ev("e2", "u2", T0))
    c.ck(ck("z9", "u1", "c_2", f3(T0 - 1)), ck("a1", "u1", "c_1", f3(T0 - 2)), ck("a0", "u1", "c_1", f3(T0 + 1)),
         ck("b1", "u2", "c_1", T0 - 6 * D), ck("b2", "u2", "c_2", T0 - 5 * D), ck("b0", "u2", "c_1", T0 - 5 * D - S))

    c = case("click_start_after_dropped_event",
             "The session start is the first surviving event: a click between a dropped first event and the first surviving one "
             "credits the session; a click after a deduplicated-away later `ts` copy does not", R_CREDIT + " / " + R_VALID)
    c.ev(ev("e1", "u1", "bad"), ev("e2", "u1", T0 + 10 * M), ev("e3", "u1", T0 + 20 * M))
    c.ck(ck("k1", "u1", "c_1", T0 + 5 * M))
    c.ev(ev("d1", "u2", T0, ingest=T0 + H), ev("d1", "u2", T0 + 20 * M, ingest=T0 + 2 * H))
    c.ck(ck("k2", "u2", "c_2", T0 + 10 * M))
    c.ev(ev("f1", "u3", T0, ingest=T0 + 3 * D), ev("f2", "u3", T0 + 10 * M))
    c.ck(ck("k3", "u3", "c_1", T0 + 5 * M))

    c = case("click_never_qualifies",
             "Clicks that are valid but never qualify (after every session; another person's; more than 7 days before): their "
             "campaigns get no row at all", R_ASSIST_ROW + " / " + R_ORGANIC)
    c.ev(ev("e1", "u1", T0), ev("e2", "u2", T0 + 9 * D))
    c.ck(ck("k1", "u1", "c_1", T0 + H), ck("k2", "u3", "c_1", T0 - H), ck("k3", "u2", "c_2", T0), ck("k4", "u2", "c_2", T0 + 2 * D - S))

    c = case("click_organic_mix",
             "Organic and credited sessions on the same days, with purchases: `organic` rows carry sessions, purchasing sessions "
             "and revenue but 0 assists; credited + organic sums equal user_daily", R_ORGANIC)
    for n in range(8):
        c.ev(ev(f"e{n}", f"u{n}", T0 + (n % 2) * D, typ="purchase" if n % 3 else "view", prod=f"p{1 + n % 2}", price=100 * (n + 1) if n % 3 else ""))
    c.ck(ck("k1", "u1", "c_1"), ck("k2", "u2", "c_1"), ck("k3", "u5", "c_2", T0 + D - H), ck("k4", "u5", "c_1", T0 + D - 2 * H))


# ---------------------------------------------------------------------------
# 8. Aggregation
# ---------------------------------------------------------------------------
def aggregation():
    prods = [("p1", "shoes"), ("p2", "books"), ("p3", "Shoes"), ("p4", "zebra"), ("p5", "toys"), ("p6", "unknowm"), ("p7", "v"),
             ("p8", "UNKNOWN"), ("p9a", "unknown "), ("p10", "é"), ("p11", "z")]
    c = case("agg_top_category_ties",
             "top_category ties: `books`=`shoes` -> `books`; `Shoes`=`shoes` -> `Shoes`; `unknown`=`shoes` -> `shoes`; "
             "`unknown`=`zebra` -> `unknown`; `unknown` winning outright; `unknown`=`unknowm`; `unknown`=`v`; `UNKNOWN`=`unknown`; "
             "`unknown `=`unknown`; `z`=`é` -> `z`; a three-way tie; a tie made by several purchases summing equal", R_TOP, products=prods)
    ties = [("t01", [("p1", 500), ("p2", 500)]), ("t02", [("p3", 500), ("p1", 500)]), ("t03", [("zz", 500), ("p1", 500)]),
            ("t04", [("p4", 500), ("zz", 500)]), ("t05", [("zz", 501), ("p1", 500)]), ("t06", [("zz", 500), ("p6", 500)]),
            ("t07", [("p7", 9), ("zz", 9)]), ("t08", [("zz", 7), ("p8", 7)]), ("t09", [("p9a", 7), ("zz", 7)]),
            ("t10", [("p10", 5), ("p11", 5)]), ("t11", [("p5", 3), ("p4", 3), ("p2", 3)]),
            ("t12", [("p1", 100), ("p2", 300), ("p1", 200), ("p4", 299)]), ("t13", [("qq", 100), ("rr", 100), ("p4", 200)]),
            ("t14", [("p4", 10_000_000), ("p1", 9_999_999), ("p1", 1)])]
    for user, purchases in ties:
        for k, (prod, price) in enumerate(purchases):
            c.ev(ev(f"{user}_{k}", user, T0 + k * M, typ="purchase", prod=prod, price=price))

    c = case("agg_revenue_across_sessions",
             "Revenue and top_category are summed over all sessions of a day: `shoes` leads the first session, `toys` the second, "
             "`books` wins the day; the next day's purchases are separate; views and carts carrying prices add nothing", R_REV + " / " + R_TOP,
             products=[("p1", "shoes"), ("p2", "books"), ("p3", "toys")])
    c.ev(ev("a1", "u1", T0, typ="purchase", prod="p1", price=600), ev("a2", "u1", T0 + M, typ="purchase", prod="p2", price=500),
         ev("a3", "u1", T0 + 3 * H, typ="purchase", prod="p2", price=500), ev("a4", "u1", T0 + 3 * H + M, typ="purchase", prod="p3", price=550),
         ev("a5", "u1", T0 + 3 * H + 2 * M, typ="cart", prod="p3", price=99999), ev("a6", "u1", T0 + 3 * H + 3 * M, typ="view", prod="p3", price=99999),
         ev("a7", "u1", T0 + D, typ="purchase", prod="p3", price=1))
    c.ck(ck("k1", "u1", "c_1", T0 + 2 * H))

    c = case("agg_midnight_split",
             "Purchases after midnight belong to the session's start day for user_daily and for campaign_daily, while a new "
             "session after midnight belongs to the new day", R_DAY)
    mid = utc(2026, 10, 6)
    c.ev(ev("a1", "u1", mid - 5 * M), ev("a2", "u1", mid + 5 * M, typ="purchase", prod="p1", price=1000),
         ev("a3", "u1", mid + 2 * H, typ="purchase", prod="p2", price=2000), ev("a4", "u1", mid - 3 * H, typ="purchase", prod="p2", price=10))
    c.ck(ck("k1", "u1", "c_1", mid - H), ck("k2", "u1", "c_2", mid + H))

    odd = ["Unknown", "UNKNOWN", "null", "None", "NULL", "NaN", "123", " shoes", "shoes ", "sho es", "", "-", "organic", "true", "N/A",
           "catégorie", "カテゴリ", "a" * 500, "top_category", "0", "1e5", "2026-10-03"]
    odd = [o for o in odd if o != ""]
    c = case("agg_odd_category_names",
             "Categories with odd names (`Unknown`, `null`, `None`, `NaN`, `123`, ` shoes`, `shoes `, `organic`, `true`, UTF-8, "
             "a 500-byte name, a date): one purchasing user per category", R_CLEAN, products=[(f"p{n}", o) for n, o in enumerate(odd)])
    for n in range(len(odd)):
        c.ev(ev(f"e{n}", f"u{n:02d}", T0, typ="purchase", prod=f"p{n}", price=100 + n))

    c = case("agg_products_spacing",
             "products.csv listing ` p1`, `p1 `, `P1`, `p 1`, `p1` (five products, five categories), a product ID with a tab, "
             "one with UTF-8, one that is `product_id`, one 300 bytes long; a purchase of each and of `p1  ` (unknown)", R_CLEAN + " / " + R_SPACE,
             products=[("p1", "plain"), (" p1", "space before"), ("p1 ", "space after"), ("P1", "upper"), ("p 1", "inner"),
                       ("p1\t", "tab"), ("pé", "utf8"), ("product_id", "header-like"), ("p" * 300, "long"), ("01", "digits"), ("1", "digit")])
    for n, (prod, _) in enumerate(c.products):
        c.ev(ev(f"e{n}", f"u{n:02d}", T0, typ="purchase", prod=prod, price=100 + n))
    c.ev(ev("x1", "unk", T0, typ="purchase", prod="p1  ", price=5), ev("x2", "unk2", T0, typ="purchase", prod="001", price=5))

    c = case("agg_unknown_product_variants",
             "Purchases of products not in products.csv (`P1`, ` p1`, `p1 `, `p01`, `p9`, `unknown`, a UTF-8 name) all count as "
             "`unknown` and sum together against `shoes`", R_CAT)
    for n, prod in enumerate(["P1", " p1", "p1 ", "p01", "p9", "unknown", "pé"]):
        c.ev(ev(f"e{n}", "u1", T0 + n * M, typ="purchase", prod=prod, price=100))
    c.ev(ev("s1", "u1", T0 + 10 * M, typ="purchase", prod="p1", price=699), ev("s2", "u2", T0, typ="purchase", prod="p1", price=701),
         ev("s3", "u2", T0 + M, typ="purchase", prod="zz", price=700))

    c = case("agg_lookup_no_trailing_newline", "products.csv and campaigns.csv whose last row has no trailing `\\n`: the last product and "
             "campaign are still known", R_CLEAN)
    c.raw("products.csv", b"product_id,category\np1,shoes\np2,books")
    c.raw("campaigns.csv", b"campaign_id,channel\nc_1,search\nc_2,social")
    c.ev(ev("e1", "u1", T0, typ="purchase", prod="p2", price=100))
    c.ck(ck("k1", "u1", "c_2"))

    c = case("agg_lookup_quoted_fields",
             "products.csv and campaigns.csv with quoted fields (`\"p1\",\"shoes\"`): the quotes are syntax here too",
             R_QUOTE_OK + " (judgement call: the lookup files are `clean`, but they are CSV files of the same dialect)")
    c.raw("products.csv", b'product_id,category\n"p1","shoes"\np2,"books"\n"p3",toys\n')
    c.raw("campaigns.csv", b'campaign_id,channel\n"c_1","search"\nc_2,"social"\n"c_3",email\n')
    for n in range(1, 4):
        c.ev(ev(f"e{n}", f"u{n}", T0, typ="purchase", prod=f"p{n}", price=100 * n))
        c.ck(ck(f"k{n}", f"u{n}", f"c_{n}"))

    c = case("agg_lookup_header_only", "products.csv and campaigns.csv holding only their header: every purchase is `unknown`, every click is dropped",
             R_CAT + " / " + R_CLICK_VALID, products=[], campaigns=[])
    c.ev(ev("e1", "u1", T0, typ="purchase", prod="p1", price=100), ev("e2", "u2", T0))
    c.ck(ck("k1", "u1", "c_1"), ck("k2", "u2", "organic"))

    c = case("agg_revenue_over_u32",
             "One session with 500 purchases of 10,000,000 (5,000,000,000 cents, over 2^32), credited to one campaign; a second "
             "user with 215 (just over 2^31)", R_REV)
    for n in range(500):
        c.ev(ev(f"a{n}", "whale", T0 + n * S, typ="purchase", prod=f"p{1 + n % 2}", price=10_000_000))
    for n in range(215):
        c.ev(ev(f"b{n}", "dolphin", T0 + n * S, typ="purchase", prod="p9", price="010000000"))
    c.ck(ck("k1", "whale", "c_1"), ck("k2", "dolphin", "c_1"))

    c = case("agg_user_named_like_keywords",
             "Users named `organic`, `unknown`, `user_id`, `null`, `NULL`, `NaN`, `None`, `true`, `0`, `-1`, `1e5`, `2026-10-03`, "
             "`day`, `c_1`, `#`, `'`, `;`, `\\\\`, `%s`, `{}`, `$HOME`, `a|b`: plain strings, one row each",
             R_STR)
    names = ["organic", "unknown", "user_id", "null", "NULL", "NaN", "None", "true", "0", "-1", "1e5", "2026-10-03", "day", "c_1", "#",
             "'", ";", "\\", "%s", "{}", "$HOME", "a|b", "\\N", "nan", "N/A", "<NA>", "-", "--", "0.0", "00", "+1", "é", "\\n", "\\r\\n", "*", "?"]
    for n, name in enumerate(names):
        c.ev(ev(f"e{n}", name, T0, typ="purchase", price=100 + n))
        c.ck(ck(f"k{n}", name, "c_1"))

    c = case("agg_empty_everything", "No data lines at all: both tables exist and are empty", R_FILES)

    c = case("agg_nothing_survives", "Events exist but none survives (bad `ts`, late, out of window, empty user): both tables are empty; valid clicks alone make no rows",
             R_VALID)
    c.ev(ev("e1", "u1", "bad"), ev("e2", "u1", T0, ingest=T0 + 2 * D), ev("e3", "u1", W1), ev("e4", "", T0), ev("e5", "u1", T0, typ="purchase", price=0))
    c.ck(ck("k1", "u1", "c_1"))


# ---------------------------------------------------------------------------
# 9. Seeded fuzz: every kind of dirt at once, on small ID pools
# ---------------------------------------------------------------------------
F2_OFFSETS = [(-1439, None), (-600, None), (-240, None), (0, b"-"), (0, b"+"), (60, None), (330, None), (720, None), (1439, None)]


def fuzz_fill(c, rng, users, n_events, n_clicks, link_pairs, n_files=(3, 2, 2), products=("p1", "p2", "p3", "P1", "p9"),
              camps=("c_1", "c_2", "c_3", "C_1", "c_9"), dirt=1.0, id_prefix="", slots=7):
    """Dirty rows that stay inside the rules' promises: `ingest_ts` is always valid F1, copies of an
    event with equal `ingest_ts` are identical (apart from quoting), copies of a click are identical."""
    def home(ui):
        # each user ID has a home time; with slots > 7 every ID roams the whole window instead
        return W0 + ((ui * 7919) % 600) * 30 * M if slots <= 7 else W0

    def pick_ts(ui):
        if rng.random() < 0.03 * dirt:
            return rng.choice([W0, W0 - 1, W0 - S, W1 - 1, W1 - S, W1, W0 + 1])
        t = home(ui) + rng.randrange(slots) * 30 * M
        return t + rng.choice([0, 0, 0, 0, 0, 1, -1, 999, S, -S, M, 12_345, 5 * M, 29 * M + 59 * S, 30 * M + 1])

    def render(t):
        if rng.random() < 0.04 * dirt:
            return rng.choice(BAD_TS)
        if t % S:
            return f3(t, rng.choice([0, 0, 0, 16, 20, 25]))
        r = rng.random()
        if r < 0.55:
            return f1(t)
        if r < 0.75:
            off, sign = rng.choice(F2_OFFSETS)
            return f2(t, off, sign)
        return f3(t, rng.choice([0, 0, 15, 20]))

    def ingest_for(t):
        if t % S == 999 and rng.random() < 0.3:
            return t + 1 + D                                  # 24 h + 1 ms: late
        base = t - t % S
        return base + rng.choice([M] * 10 + [10 * M] * 4 + [D, D, D + S, -H, 3 * D, S, 0])

    def user_name(ui):
        r = rng.random()
        if r < 0.004 * dirt:
            return b""
        name = B(users[ui])
        if r < 0.008 * dirt:
            return name.upper()
        return name

    def quoted(fields):
        return [q(f) if rng.random() < 0.15 else f for f in fields]

    def malform(fields):
        kind = rng.randrange(7)
        f = list(fields)
        if kind == 0:
            return b",".join(f[:-1])
        if kind == 1:
            return b",".join(f + [b"x"])
        if kind == 2:
            return b""
        if kind == 3:
            k = rng.randrange(len(f))
            f[k] = f[k][:1] + b'"' + f[k][1:]
        elif kind == 4:
            f[rng.randrange(len(f))] = b'"'
        elif kind == 5:
            k = rng.randrange(len(f))
            f[k] = b'"' + f[k]
        else:
            return b",".join(f) + b","
        return b",".join(f)

    ev_lines, ck_lines, ln_lines = [], [], []
    for i in range(n_events):
        ui = min(rng.randrange(len(users)), rng.randrange(len(users) + len(users) // 3)) % len(users)
        t = pick_ts(ui)
        typ = rng.choice([b"view"] * 12 + [b"cart"] * 3 + [b"purchase"] * 5)
        if rng.random() < 0.02 * dirt:
            typ = rng.choice([b"Purchase", b"cart ", b"VIEW", b"", b"click"])
        prod = B(rng.choice(products)) if rng.random() > 0.01 * dirt else b""
        price = b""
        if typ.lower().strip() == b"purchase" or rng.random() < 0.1:
            price = f3(rng.choice([1, 100, 250, 999, 5000, 10_000_000, rng.randrange(1, 50_000)]))
            if rng.random() < 0.06 * dirt:
                price = rng.choice(BAD_PRICES + [g for g, _ in GOOD_PRICES if g[:1] != b'"'])
        eid = B(f"{id_prefix}e{i}")
        ing = ingest_for(t)
        fields = [eid, user_name(ui), render(t), f1(ing), typ, prod, price]
        copies = [fields]
        r = rng.random()
        if r < 0.10:
            offsets = rng.sample([-21, -14, -7, 7, 14, 21, 600], rng.randrange(1, 4))
            for off in offsets:
                f = list(fields)
                f[3] = f1(ing + off * S)
                m = rng.random()
                if m < 0.25:
                    f[1] = user_name(rng.randrange(len(users)))
                elif m < 0.40:
                    f[2] = rng.choice(BAD_TS)
                elif m < 0.50:
                    f[4] = b"Purchase"
                elif m < 0.60:
                    f[2] = render(pick_ts(ui))
                elif m < 0.65:
                    f[6] = b"-1"
                    f[4] = b"purchase"
                copies.append(f)
        elif r < 0.13:
            copies.append(list(fields))                       # identical copy, quoted differently
        for f in copies:
            if rng.random() < 0.02 * dirt:
                ev_lines.append(malform(f))
            else:
                ev_lines.append(b",".join(quoted(f)))
                if len(copies) == 1 and rng.random() < 0.004 * dirt:
                    ev_lines[-1] += b"\r"
    for i in range(n_clicks):
        ui = rng.randrange(len(users))
        t = home(ui) + rng.randrange(slots) * 30 * M - rng.choice([0, 0, 0, 1, -1, H, H, D, 3 * D, 7 * D, 7 * D + 1, 7 * D - 1, 6 * D, M, -M, 30 * M])
        if rng.random() < 0.02 * dirt:
            t = rng.choice([W0 - 7 * D, W0 - 7 * D - 1, W1 - 1, W1])
        cid = B(f"{id_prefix}{rng.choice(['k', 'K', 'k_', 'k-', ''])}{i}")
        fields = [cid, user_name(ui), B(rng.choice(camps)), render(t)]
        copies = [fields] + ([list(fields)] * rng.randrange(1, 3) if rng.random() < 0.06 else [])
        for f in copies:
            ck_lines.append(malform(f) if rng.random() < 0.02 * dirt else b",".join(quoted(f)))
    for a, b in link_pairs:
        a, b = B(a), B(b)
        r = rng.random()
        if r < 0.5:
            a, b = b, a
        ln_lines.append(b",".join(quoted([a, b])))
        r = rng.random()
        if r < 0.08:
            ln_lines.append(b",".join(quoted([b, a])))
        elif r < 0.10:
            ln_lines.append(a + b"," + a)
        elif r < 0.10 + 0.01 * dirt:
            ln_lines.append(a + b",")
        elif r < 0.10 + 0.02 * dirt:
            ln_lines.append(a.upper() + b"," + b)
        elif r < 0.10 + 0.03 * dirt:
            ln_lines.append(malform([a, b]))
    for kind, lines, count in (("events", ev_lines, n_files[0]), ("clicks", ck_lines, n_files[1]), ("links", ln_lines, n_files[2])):
        rng.shuffle(lines)
        cuts = sorted(rng.randrange(len(lines) + 1) for _ in range(count - 1))
        for n, (lo, hi) in enumerate(zip([0] + cuts, cuts + [len(lines)])):
            body = b"".join(line + b"\n" for line in lines[lo:hi])
            if body and n % 2 == 1:
                body = body[:-1]                              # every other file: no trailing newline
            c.raw(f"{kind}/part-{n:05d}.csv", HEADERS[kind] + b"\n" + body)


def fuzz():
    prods = [("p1", "shoes"), ("p2", "books"), ("p3", "Shoes")]
    camps = [("c_1", "search"), ("c_2", "social"), ("c_3", "email")]
    for seed in range(1, 13):
        rng = random.Random(1000 + seed)
        users = [f"u{n}" for n in range(10)] + ["U1", "u1 ", " u1", "ü1", "u01", "u10"]
        pairs = [tuple(rng.sample(users, 2)) for _ in range(rng.randrange(0, 6))]
        c = case(f"fuzz_{seed:02d}",
                 f"Seeded fuzz (seed {1000 + seed}): 400 event lines, 120 click lines and {len(pairs)} links over 16 near-equal user "
                 "IDs, with every dirt of the rules at once (all three `ts` forms and bad forms, window edges, duplicates with "
                 "changed fields, late events, bad types and prices, quoting, malformed lines, `\\r`, files without a final newline)",
                 "All of them together", products=prods, campaigns=camps)
        fuzz_fill(c, rng, users, 400, 120, pairs)
    for seed in range(1, 5):
        rng = random.Random(2000 + seed)
        users = [f"u{n}" for n in range(3)]
        c = case(f"fuzz_dense_{seed:02d}",
                 f"Seeded fuzz (seed {2000 + seed}): 3 linked user IDs sharing 1,500 event lines and 400 click lines spread over "
                 "the whole window on a 30-minute grid with +-1 ms jitter, so sessions, ties, duplicates and the 7-day window "
                 "interact heavily", "All of them together", products=prods, campaigns=camps)
        fuzz_fill(c, rng, users, 1500, 400, [("u0", "u1"), ("u2", "u1")] if seed % 2 else [("u0", "u1")], slots=660)


# ---------------------------------------------------------------------------
# 10. Scale-shaped cases
# ---------------------------------------------------------------------------
def filler_events(rng, prefix, users=3000):
    """An endless stream of valid-looking event lines (each ending in a newline)."""
    n = 0
    recent = []
    while True:
        t = W0 + rng.randrange(14 * D // S) * S
        if rng.random() < 0.3:
            t += rng.randrange(1000)
        ts = f3(t) if t % S or rng.random() < 0.3 else (f1(t) if rng.random() < 0.8 else f2(t, rng.choice([-600, -240, 60, 330, 720])))
        purchase = rng.random() < 0.1
        ingest = t - t % S + M
        fields = [B(f"{prefix}{n}"), B(f"fu{rng.randrange(users)}"), ts, f1(ingest), b"purchase" if purchase else b"view",
                  B(f"p{rng.randrange(1, 5)}"), f3(rng.randrange(100, 50_000)) if purchase else b""]
        n += 1
        yield b",".join(fields) + b"\n"
        if rng.random() < 0.04 and recent:                    # one later copy of a recent event, sometimes for another user
            copy, copy_ingest = recent.pop(rng.randrange(len(recent)))
            copy = list(copy)
            copy[3] = f1(copy_ingest + rng.randrange(1, 600) * S)
            if rng.random() < 0.5:
                copy[1] = B(f"fu{rng.randrange(users)}")
            yield b",".join(copy) + b"\n"
        recent.append((fields, ingest))
        if len(recent) > 50:
            recent.pop(0)


def build_exact(header, fillers, pad, blocks, tail=0):
    """Bytes in which every (offset, block) of `blocks` starts at exactly that offset."""
    out = bytearray(header)
    for offset, block in blocks:
        while offset - len(out) > 600:
            out += next(fillers)
        out += pad(offset - len(out))
        assert len(out) == offset, (len(out), offset)
        out += block
    for _ in range(tail):
        out += next(fillers)
    return bytes(out)


def big():
    pad_counter = itertools.count()

    def pad_event_exact(n):
        k = next(pad_counter)
        base = ev(f"pad{k}", "padder", T0, prod="") + b"\n"
        assert n > len(base), n
        line = ev(f"pad{k}", "padder", T0, prod="x" * (n - len(base))) + b"\n"
        assert len(line) == n
        return line

    def xy(tag):
        x = ev(f"x_{tag}", f"before_{tag}", T0, typ="purchase", price=111)
        y = ev(f"y_{tag}", f"after_{tag}", T0, typ="purchase", price=222)
        return x, y

    def probe(tag, boundary, delta, middle=b""):
        """Lines `x`, then `middle`, then `y`, placed so that `middle` (or `y` when it is empty)
        starts at file offset boundary + delta. x and y are purchases by users of their own."""
        x, y = xy(tag)
        return boundary + delta - len(x) - 1, x + b"\n" + middle + y + b"\n"

    def bad6(tag):
        return b"bad_" + B(tag) + b",bad_" + B(tag) + b"," + f1(T0) + b"," + f1(T0 + M) + b",view,p1\n"

    header = EH + b"\n"
    c = case("big_chunk_4mb",
             "Seven event files of about 4.2 MB (valid filler events, 4% of them duplicated). At byte 4,194,304 a line starts "
             "exactly there, one byte before, and one byte after; one file is exactly 4,194,304 bytes ending in `\\n`; one is "
             "exactly that long with no final `\\n`; one has its last line start at 4,194,304 with no final `\\n`; one is "
             "4,194,305 bytes with its final `\\n` at offset 4,194,304. Each file also has a line starting at, before or after "
             "byte 2,097,152. The lines on each side of every boundary are purchases by their own users",
             R_FILES + " (the fast binary cuts files into chunks: 4,194,304 bytes when this was written, 2,097,152 now)")
    rng = random.Random(31)
    specs = []
    specs.append(([probe("h0", UNIT, 1), probe("at", CHUNK, 0)], 800))
    specs.append(([probe("h1", UNIT, 0), probe("minus1", CHUNK, -1)], 800))
    specs.append(([probe("h2", UNIT, -1), probe("plus1", CHUNK, 1)], 800))
    x, y = xy("len_exact_nl")
    blk = x + b"\n" + y + b"\n"
    specs.append(([probe("h3", UNIT, 0, b"\n"), (CHUNK - len(blk), blk)], 0))
    x, y = xy("len_exact_no_nl")
    blk = x + b"\n" + y
    specs.append(([probe("h4", UNIT, -1, b"\n\n"), (CHUNK - len(blk), blk)], 0))
    x, y = xy("last_line_at")
    specs.append(([probe("h5", UNIT, 0, bad6("h5")), (CHUNK - len(x) - 1, x + b"\n" + y)], 0))
    x, y = xy("nl_at")
    blk = x + b"\n" + y + b"\n"
    specs.append(([probe("h6", UNIT, 1, b'"\n'), (CHUNK + 1 - len(blk), blk)], 0))
    for n, (blocks, tail) in enumerate(specs):
        data = build_exact(header, filler_events(rng, f"f{n}_"), pad_event_exact, blocks, tail)
        c.raw(f"events/part-{n:05d}.csv", data)
        if n in (0, 5):
            assert data[CHUNK - 1:CHUNK] == b"\n" and data[CHUNK:CHUNK + 2] == b"y_"
        if n == 1:
            assert data[UNIT - 1:UNIT + 2] == b"\ny_" and data[CHUNK - 2:CHUNK + 1] == b"\ny_"
        if n == 3:
            assert len(data) == CHUNK and data.endswith(b"\n")
        if n == 4:
            assert len(data) == CHUNK and not data.endswith(b"\n")
        if n == 6:
            assert len(data) == CHUNK + 1 and data.endswith(b"\n")
    c.ck(*[ck(f"k{n}", f"fu{n}", f"c_{1 + n % 2}", W0 + n * H) for n in range(300)])
    c.ln(*[lk(f"fu{n}", f"fu{n + 1500}") for n in range(0, 1500, 3)])

    c = case("big_chunk_8mb",
             "Three event files of about 8.5 MB, each crossing the boundaries at 2,097,152, 4,194,304, 6,291,456 and 8,388,608 "
             "bytes. At each, one of: a malformed 6-field line, a lone-quote line, one empty line, three consecutive empty "
             "lines, or a plain valid line, starting exactly at the boundary, one or two bytes before, or one byte after; valid "
             "purchases by their own users on both sides",
             R_FILES + " / " + R_EMPTY_LINE + " (chunk boundaries of the fast binary)")
    rng = random.Random(37)
    plans = [[(0, "bad"), (-1, "e3"), (1, ""), (0, "e1")],
             [(1, "e1"), (0, "quote"), (-1, "bad"), (-2, "e3")],
             [(-1, "quote"), (1, "bad"), (0, "e3"), (-1, "")]]
    for n, plan in enumerate(plans):
        blocks = []
        for k, (delta, kind) in enumerate(plan):
            tag = f"g{n}{k}"
            middle = {"bad": bad6(tag), "e3": b"\n\n\n", "e1": b"\n", "quote": b'"\n', "": b""}[kind]
            blocks.append(probe(tag, (k + 1) * UNIT, delta, middle))
        data = build_exact(header, filler_events(rng, f"g{n}_"), pad_event_exact, blocks, 500)
        c.raw(f"events/part-{n:05d}.csv", data)
    d = c.files["events/part-00000.csv"]
    assert d[UNIT:UNIT + 4] == b"bad_" and d[2 * UNIT - 2:2 * UNIT + 2] == b"\n\n\n\n" and d[3 * UNIT:3 * UNIT + 3] == b"\ny_"
    assert d[4 * UNIT - 1:4 * UNIT + 3] == b"\n\ny_"
    c.ck(*[ck(f"k{n}", f"fu{n}", f"c_{1 + n % 2}", W0 + n * H) for n in range(300)])

    c = case("big_chunk_step_64k",
             "Lines that start 100 bytes before a 2,097,152-byte boundary and end 65,536 bytes (and 131,072 bytes) past it, "
             "give or take a byte: the fast binary finishes such a line by reading on in 65,536-byte steps. Two files with "
             "three boundaries each, and three files that end (without a final `\\n`) exactly at, one byte before and one byte "
             "after the end of the first step",
             R_FILES + " (no limit on line length is given)")
    rng = random.Random(39)

    def long_line(tag, boundary, newline_at, newline=True):
        start = boundary - 100
        base = ev(f"long_{tag}", f"long_{tag}", T0, typ="purchase", prod="", price=77)
        line = ev(f"long_{tag}", f"long_{tag}", T0, typ="purchase", prod="L" * (newline_at - start - len(base)), price=77)
        assert start + len(line) == newline_at
        y = ev(f"y_{tag}", f"after_{tag}", T0, typ="purchase", price=222)
        return start, line + (b"\n" + y + b"\n" if newline else b"")

    plans = [[STEP - 2, STEP - 1, STEP], [STEP + 1, 2 * STEP - 1, 2 * STEP]]
    for n, plan in enumerate(plans):
        blocks = [long_line(f"s{n}{k}", (k + 1) * UNIT, (k + 1) * UNIT + extra) for k, extra in enumerate(plan)]
        c.raw(f"events/part-{n:05d}.csv", build_exact(header, filler_events(rng, f"s{n}_"), pad_event_exact, blocks, 300))
    for n, extra in enumerate((STEP - 1, STEP, STEP + 1)):
        blocks = [long_line(f"eof{n}", UNIT, UNIT + extra, newline=False)]
        data = build_exact(header, filler_events(rng, f"t{n}_"), pad_event_exact, blocks, 0)
        assert len(data) == UNIT + extra
        c.raw(f"events/part-{n + 2:05d}.csv", data)
    d = c.files["events/part-00000.csv"]
    assert d[UNIT + STEP - 2:UNIT + STEP - 1] == b"\n" and d[2 * UNIT + STEP - 1:2 * UNIT + STEP] == b"\n" and d[3 * UNIT + STEP:3 * UNIT + STEP + 1] == b"\n"
    c.ck(ck("k1", "after_s00", "c_1"), ck("k2", "long_s11", "c_2"), ck("k3", "long_eof1", "c_1"))

    c = case("big_chunk_clicks_links",
             "A 4.3 MB clicks file and a 4.3 MB links file, each with a line starting exactly at byte 4,194,304: the click on "
             "each side of the boundary credits its own user's session, the link on each side renames its own user",
             R_FILES + " (chunk boundaries apply to clicks and links files too)")
    # (4,194,304 is a boundary for both the 4 MiB and the 2 MiB chunking)
    rng = random.Random(41)

    def click_fillers():
        n = 0
        while True:
            yield ck(f"fk{n}", f"fu{rng.randrange(500)}", f"c_{1 + n % 2}", W0 - 7 * D + rng.randrange(21 * D // S) * S) + b"\n"
            n += 1

    def pad_click(n):
        k = next(pad_counter)
        base = ck(f"padk{k}_", "nobody", "c_1", T0) + b"\n"
        return ck(f"padk{k}_" + "x" * (n - len(base)), "nobody", "c_1", T0) + b"\n"

    def link_fillers():
        n = 0
        while True:
            yield lk(f"lf{n}", f"lf{n}_twin") + b"\n"
            n += 1

    def pad_link(n):
        k = next(pad_counter)
        base = lk(f"padl{k}_", f"padl{k}_b") + b"\n"
        return lk(f"padl{k}_" + "x" * (n - len(base)), f"padl{k}_b") + b"\n"

    x, y = ck("kx", "click_before", "c_1", T0 - H), ck("ky", "click_after", "c_2", T0 - H)
    c.raw("clicks/part-00000.csv", build_exact(CH + b"\n", click_fillers(), pad_click, [(CHUNK - len(x) - 1, x + b"\n" + y + b"\n")], 2000))
    x, y = lk("link_before", "a_before"), lk("link_after", "a_after")
    c.raw("links/part-00000.csv", build_exact(LH + b"\n", link_fillers(), pad_link, [(CHUNK - len(x) - 1, x + b"\n" + y + b"\n")], 2000))
    assert c.files["clicks/part-00000.csv"][CHUNK:CHUNK + 3] == b"ky," and c.files["links/part-00000.csv"][CHUNK:CHUNK + 10] == b"link_after"
    c.ev(ev("e1", "click_before", T0), ev("e2", "click_after", T0), ev("e3", "link_before", T0), ev("e4", "link_after", T0))
    for n in range(0, 500, 7):
        c.ev(ev(f"f{n}", f"fu{n}", W0 + n * 40 * M), ev(f"g{n}", f"lf{n}_twin", T0 + n * M))

    c = case("big_line_spans_chunks",
             "A valid view whose product ID is 9,000,000 bytes long (one line across three 4 MB chunks), with ordinary events "
             "before and after it; a second file with a 4,500,000-byte malformed line between valid ones",
             R_FILES + " (no limit on line length is given)")
    c.raw("events/part-00000.csv", EH + b"\n" + ev("e1", "before", T0) + b"\n" + ev("e2", "longline", T0, prod=b"p" * 9_000_000) + b"\n"
          + ev("e3", "after", T0, typ="purchase", price=333) + b"\n" + ev("e2", "longline_dup", T0, ingest=T0 + H) + b"\n")
    c.raw("events/part-00001.csv", EH + b"\n" + ev("e4", "before2", T0) + b"\n" + b"m" * 4_500_000 + b"\n" + ev("e5", "after2", T0, typ="purchase", price=444) + b"\n")
    c.ck(ck("k1", "after", "c_1"), ck("k2", "after2", "c_2"))

    c = case("big_no_newline_file",
             "An events file of 5,000,000 bytes with no `\\n` anywhere (so it is all header and has no data), next to a normal "
             "file; the same for a 3,000,000-byte clicks file", R_HEADER + " (beyond the rules: the first line is not the real header)")
    c.raw("events/part-00000.csv", b"x" * 5_000_000)
    c.raw("clicks/part-00000.csv", b"k1,u1,c_1," * 300_000)
    c.ev(ev("e1", "u1", T0), file="part-00001.csv").ck(ck("k2", "u1", "c_2"), file="part-00001.csv")

    c = case("big_one_person_200k",
             "One person (five linked IDs) with 200,000 events over about nine days: gaps mostly under four seconds, about 100 of "
             "exactly 30:00.000, 60 of 30:00.001 and 40 longer; 2% duplicated; shuffled over three files; 300 clicks",
             R_SESS_ALL + " / " + R_SESS)
    rng = random.Random(43)
    ids = ["pe", "pd", "pc", "pb", "pa"]
    c.ln(*[lk(a, b) for a, b in zip(ids, ids[1:])])
    t = W0 + 3 * H
    lines = []
    for n in range(200_000):
        r = rng.random()
        if r < 0.0005:
            t += 30 * M
        elif r < 0.0008:
            t += 30 * M + 1
        elif r < 0.0010:
            t += rng.randrange(30 * M, 50 * M)
        else:
            t += rng.randrange(0, 4000)
        assert t < W1
        purchase = rng.random() < 0.03
        ts = f3(t) if t % S or rng.random() < 0.5 else f1(t)
        line = ev(f"e{n}", rng.choice(ids), ts, at=t, typ="purchase" if purchase else rng.choice(["view", "view", "cart"]),
                  prod=f"p{rng.randrange(1, 4)}", price=rng.randrange(100, 50_000) if purchase else "")
        lines.append(line)
        if rng.random() < 0.02:
            lines.append(ev(f"e{n}", rng.choice(ids + ["stranger"]), ts, ingest=t - t % S + M + rng.randrange(1, 600) * S))
    end = t
    rng.shuffle(lines)
    third = len(lines) // 3
    for n in range(3):
        c.raw(f"events/part-{n:05d}.csv", EH + b"\n" + b"".join(l + b"\n" for l in lines[n * third:(n + 1) * third if n < 2 else None]))
    c.ck(*[ck(f"k{n}", rng.choice(ids), f"c_{1 + n % 2}", W0 - 7 * D + rng.randrange((end - W0 + 7 * D) // S) * S) for n in range(300)])

    camps = [(f"c_{n}", f"channel{n % 5}") for n in range(1, 21)]
    c = case("big_50k_people",
             "50,000 people with one event each (100-byte event IDs, so the file is about 9 MB), a fifth of them purchases; "
             "25,000 clicks of 20 campaigns, from 0 to 8 days before each event", R_ORGANIC + " / " + R_CREDIT, campaigns=camps)
    rng = random.Random(47)
    lines, cl = [], []
    for n in range(50_000):
        t = W0 + rng.randrange(14 * D // S) * S + rng.choice([0, 0, rng.randrange(1000)])
        purchase = n % 5 == 0
        lines.append(ev(f"e{n:05d}-" + "x" * 93, f"p{n:05d}", t, typ="purchase" if purchase else "view", prod=f"p{1 + n % 3}",
                        price=100 + n % 4000 if purchase else ""))
        if n % 2 == 0:
            back = rng.choice([0, 1, S, H, D, 6 * D, 7 * D, 7 * D + 1, 8 * D, rng.randrange(8 * D)])
            cl.append(ck(f"k{n}", f"p{n:05d}", f"c_{1 + rng.randrange(20)}", t - back))
    rng.shuffle(lines)
    c.ev(*lines[:30_000]).ev(*lines[30_000:], file="part-00001.csv")
    c.ck(*cl)

    c = case("big_chain_20k",
             "Three chains of 20,000 linked IDs each: `n00000`..`n19999` linked in order, `r...` linked in reverse order, `m...` "
             "linked in shuffled order and random direction over four files with 8% repeats; events by every fiftieth ID (never "
             "the smallest) on a 30-minute grid, a few clicks", R_LINK_JOIN + " / " + R_LINK_NAME)
    rng = random.Random(53)
    size = 20_000
    c.ln(*[lk(f"n{i:05d}", f"n{i + 1:05d}") for i in range(size - 1)], file="part-00000.csv")
    c.ln(*[lk(f"r{i:05d}", f"r{i - 1:05d}") for i in range(size - 1, 0, -1)], file="part-00001.csv")
    pairs = [(f"m{i:05d}", f"m{i + 1:05d}") for i in range(size - 1)]
    rng.shuffle(pairs)
    for n, (a, b) in enumerate(pairs):
        line = lk(a, b) if rng.random() < 0.5 else lk(b, a)
        c.ln(line, file=f"part-0000{2 + n % 4}.csv")
        if rng.random() < 0.08:
            c.ln(lk(b, a), file=f"part-0000{2 + rng.randrange(4)}.csv")
    n = 0
    for prefix in "nrm":
        for i in range(5, size, 50):
            c.ev(ev(f"{prefix}e{i}", f"{prefix}{i:05d}", W0 + (n * 7919 % (14 * 48)) * 30 * M))
            n += 1
        c.ck(ck(f"k{prefix}1", f"{prefix}19999", "c_1", W0 + 2 * D), ck(f"k{prefix}2", f"{prefix}00000", "c_2", W0 + 9 * D))

    camps = [(f"c_{n}", f"channel{n % 7}") for n in range(1, 501)]
    c = case("big_clicks_3000",
             "One person (three linked IDs) with 3,000 clicks of 500 campaigns over the 21-day click window, at 1,500 distinct "
             "instants (so half are ties, broken by mixed-case click IDs), 5% of them repeated; 160 sessions over the 14 days, some "
             "starting exactly on a click", R_CREDIT + " / " + R_ASSIST, campaigns=camps)
    rng = random.Random(59)
    ids = ["w3", "w2", "w1"]
    c.ln(lk("w3", "w2"), lk("w1", "w2"))
    instants = [W0 - 7 * D + rng.randrange(21 * D // S) * S for _ in range(1500)]
    alphabet = "abcXYZ019_-"
    cl = []
    for n in range(3000):
        cid = "".join(rng.choice(alphabet) for _ in range(rng.randrange(1, 4))) + f":{n}"
        line = ck(cid, rng.choice(ids), f"c_{1 + min(rng.randrange(500), rng.randrange(500))}", rng.choice(instants))
        cl.append(line)
        if rng.random() < 0.05:
            cl.append(b",".join(q(f) if rng.random() < 0.5 else f for f in line.split(b",")))
    rng.shuffle(cl)
    c.ck(*cl[:len(cl) // 2]).ck(*cl[len(cl) // 2:], file="part-00001.csv")
    for n in range(160):
        t = W0 + n * (14 * D // 160) // S * S
        if n % 8 == 0:
            t = rng.choice([i for i in instants if W0 <= i < W1])
        c.ev(ev(f"e{n}", rng.choice(ids), t, typ="purchase" if n % 3 == 0 else "view", price=100 + n if n % 3 == 0 else ""))

    c = case("big_fuzz_mix",
             "Seeded fuzz at scale (seed 3001): 260,000 event lines (about 20 MB) and 60,000 click lines over 30,000 user IDs "
             "linked into groups and chains, with every dirt of the rules, shuffled over 9 event, 5 click and 4 link files",
             "All of them together",
             products=[(f"p{n}", f"cat{n % 20:02d}") for n in range(1, 400)], campaigns=[(f"c_{n}", f"ch{n % 5}") for n in range(1, 60)])
    rng = random.Random(3001)
    users = [f"u{n}" for n in range(30_000)]
    pairs = []
    for i in range(1, 30_000):
        if rng.random() < 0.3:
            pairs.append((users[i], users[i - rng.randrange(1, min(i, 40) + 1)]))
    fuzz_fill(c, rng, users, 260_000, 60_000, pairs, n_files=(9, 5, 4),
              products=[f"p{n}" for n in range(1, 420)] + ["P1", ""], camps=[f"c_{n}" for n in range(1, 62)] + ["C_1"], dirt=0.5)


def keys():
    a15, a16 = "a" * 15, "a" * 16
    c = case("key_len_15_16_users",
             "User IDs of 14, 15, 16 and 17 bytes that share prefixes, linked so that the name must be picked across the 15/16 "
             "byte line: `a`x15 < `a`x16; `a`x16 < `a`x14+`b`; `a`x16 < `b`x15; `é`x7+`a` (15 bytes) vs `é`x8 (16 bytes); "
             "unlinked 15-, 16- and 17-byte IDs are three people; clicks by the 16-byte IDs",
             R_LINK_NAME + " (the fast binary stores IDs of up to 15 bytes inline and longer ones by pointer)")
    groups = [[a16, a15], ["a" * 14 + "b", a16 + "1"], ["b" * 15, "a" * 16 + "2"], ["é" * 8, "é" * 7 + "a"], ["z" * 15, "z" * 14],
              ["y" * 16, "y" * 17], ["x" * 15 + "\u00e9", "x" * 15], ["w" * 16, "W" * 16], ["v" * 15, "V" * 16], ["q" * 14 + "é", "q" * 15]]
    for n, ids in enumerate(groups):
        c.ln(lk(ids[0], ids[1]))
        c.ev(ev(f"e{n}", ids[0], T0), ev(f"f{n}", ids[1], T0 + 5 * M))
        c.ck(ck(f"k{n}", ids[n % 2], "c_1"))
    for n, uid in enumerate(["s" * 14, "s" * 15, "s" * 16, "s" * 17, "s" * 31, "s" * 32, "s" * 33]):
        c.ev(ev(f"s{n}", uid, T0))
        c.ck(ck(f"ks{n}", uid, "c_2"))

    camps = [(f"c_{n}", f"ch{n}") for n in range(1, 4)]
    c = case("key_len_15_16_clicks",
             "Ties between click IDs of 14, 15, 16 and 17 bytes sharing prefixes (the shorter prefix wins; a 16-byte ID starting "
             "with `a` beats a 15-byte ID starting with `b`), and copies of 15- and 16-byte click IDs quoted differently",
             R_CREDIT + " (the fast binary stores IDs of up to 15 bytes inline and longer ones by pointer)", campaigns=camps)
    ties = [["k" * 16, "k" * 15], ["j" * 15, "j" * 14 + "a"], ["b" * 15, "a" * 16], ["m" * 17, "m" * 16, "m" * 15], ["n" * 15 + "é", "n" * 15],
            ["Z" * 16, "a" * 15], ["p" * 14 + "\x7f", "p" * 14 + "é"], ["t" * 16 + "b", "t" * 16 + "a"], ["0" * 15, "0" * 16]]
    for n, ids in enumerate(ties):
        user = f"tie{n}"
        c.ev(ev(f"e{n}", user, T0))
        for k, cid in enumerate(ids):
            c.ck(ck(cid, user, f"c_{k + 1}", T0 - H), file=f"part-0000{k % 2}.csv")
            c.ck(ck(q(cid), q(user), f"c_{k + 1}", T0 - H), file=f"part-0000{(k + 1) % 2}.csv")

    c = case("key_len_15_16_events",
             "Event IDs of 14, 15, 16, 17, 31, 32 and 33 bytes sharing prefixes are different events; each has a later copy "
             "(quoted, other user) that must dedup; 16-byte IDs differing only in the last byte, the first byte, or by case",
             R_DEDUP + " (the fast binary stores IDs of up to 15 bytes inline and hashes longer ones)")
    ids = ["e" * n for n in (14, 15, 16, 17, 31, 32, 33)] + ["f" * 15 + "a", "f" * 15 + "b", "a" + "g" * 15, "b" + "g" * 15, "H" * 16, "h" * 16,
                                                             "é" * 8, "é" * 7 + "e\u0301"]
    for n, eid in enumerate(ids):
        c.ev(ev(eid, "u1", T0 + n * M, ingest=T0 + H, typ="purchase", price=100 + n))
        c.ev(ev(q(eid), "other", T0 + n * M, ingest=T0 + H + (n + 1) * S, typ="purchase", price=5000), file="part-00001.csv")

    c = case("csv_nul_bytes",
             "IDs holding NUL bytes (valid UTF-8, U+0000): users `a`, `a\\0`, `a\\0\\0`, `\\0`, `\\0a` are five people; `a\\0b` linked "
             "to `ab` is named `a\\0b`, `n` linked to `n\\0` is named `n`; a click tie between `k`, `k\\0` and `k\\x01`; event IDs "
             "`e` and `e\\0` are two events; a `ts`, type or purchase price with a NUL is bad; product `p1\\0` is unknown",
             R_STR + " (the fast binary zero-pads short IDs)")
    for n, u in enumerate([b"a", b"a\0", b"a\0\0", b"\0", b"\0a"]):
        c.ev(ev(f"u{n}", u, T0))
    c.ln(lk(b"ab", b"a\0b"), lk(b"n\0", b"n"), lk(b"m\0\0", b"m\0"))
    c.ev(ev("l1", b"ab", T0), ev("l2", b"n\0", T0), ev("l3", b"m\0\0", T0), ev("l4", b"m\0", T0 + M))
    c.ev(ev(b"e", "ids", T0), ev(b"e\0", "ids", T0 + M), ev(b"e\0\0", "ids", T0 + 2 * M), ev(b"\0e", "ids", T0 + 3 * M))
    c.ev(ev("t1", "bad", f1(T0) + b"\0"), ev("t2", "bad", b"\0" + f3(T0)), ev("t3", "bad", T0, typ=b"view\0"),
         ev("t4", "bad", T0, typ="purchase", price=b"5\0"), ev("t5", "bad", T0 + 3 * H, typ="purchase", prod=b"p1\0", price=5))
    c.ev(ev("s1", "tie", T0))
    c.ck(ck(b"k\x01", "tie", "c_1"), ck(b"k\0", "tie", "c_2"), ck(b"k", "tie", "c_1"), ck(b"k2", b"a\0", "c_2"), ck(b"k3", b"a", b"c_1\0"))


def big_long_ids():
    camps = [(f"c_{n}", f"channel{n % 5}") for n in range(1, 21)]
    c = case("big_long_user_ids",
             "150,000 events and 40,000 clicks whose user IDs are 16 to 48 bytes long (8,000 IDs in 5,000 people, linked), with "
             "24-byte click IDs and 20-byte event IDs, 3% of events duplicated",
             R_LINK_NAME + " (the fast binary interns every ID over 15 bytes in one shared table)", campaigns=camps)
    rng = random.Random(61)
    ids = [f"user-{n:05d}-" + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKL"[:5 + n % 33] for n in range(8000)]
    for n in range(5000, 8000):
        c.ln(lk(ids[n], ids[rng.randrange(5000)]) if n % 2 else lk(ids[rng.randrange(n)], ids[n]), file=f"part-0000{n % 3}.csv")
    lines = []
    for n in range(150_000):
        ui = min(rng.randrange(8000), rng.randrange(8000))
        t = W0 + ((ui * 7919) % 600) * 30 * M + rng.randrange(0, 4 * H // S) * S + rng.choice([0, 0, rng.randrange(1000)])
        purchase = rng.random() < 0.05
        lines.append(ev(f"event-{n:014d}", ids[ui], t, typ="purchase" if purchase else "view", prod=f"p{rng.randrange(1, 4)}",
                        price=rng.randrange(100, 9000) if purchase else ""))
        if rng.random() < 0.03:
            lines.append(ev(f"event-{n:014d}", ids[rng.randrange(8000)], t, ingest=t - t % S + M + rng.randrange(1, 300) * S))
    rng.shuffle(lines)
    cut = len(lines) // 4
    for n in range(4):
        c.ev(*lines[n * cut:(n + 1) * cut if n < 3 else None], file=f"part-0000{n}.csv")
    cl = []
    for n in range(40_000):
        ui = rng.randrange(8000)
        t = W0 + ((ui * 7919) % 600) * 30 * M + rng.randrange(0, 4 * H // S) * S - rng.choice([0, H, D, 6 * D, 7 * D, 8 * D])
        cl.append(ck(f"click-{n:018d}", ids[ui], f"c_{1 + rng.randrange(20)}", t))
    c.ck(*cl[:20_000]).ck(*cl[20_000:], file="part-00001.csv")


def all_cases():
    dialect()
    files()
    timestamps()
    windows()
    dedup()
    validation()
    links()
    sessions()
    clicks()
    aggregation()
    keys()
    fuzz()
    big()
    big_long_ids()


# ---------------------------------------------------------------------------
# README
# ---------------------------------------------------------------------------
def write_readme(notes):
    def cell(text):
        return text.replace("|", "\\|").replace("\n", " ")

    main = [c for c in CASES if not c.disputed]
    disputed = [c for c in CASES if c.disputed]
    out = ["# Adversarial cases", "",
           "Generated by `scripts/make_adversarial.py` (inputs) and by running both Python references "
           "(`--expected`); do not edit by hand. Same layout as `tests/fixtures`: `<case>/input/` is a `/data` folder, "
           "`<case>/expected/*.csv` the two tables. `expected/` exists only where `reference/a` and `reference/b` gave exactly "
           "the same rows.", "",
           "```sh",
           ".venv/bin/python scripts/make_adversarial.py --expected      # rewrite inputs and expected outputs",
           ".venv/bin/python scripts/run_fixtures.py tests/adversarial -- pipeline/target/release/pipeline",
           ".venv/bin/python scripts/make_adversarial.py --check-disputed -- pipeline/target/release/pipeline",
           "```", "",
           "Unless a row says otherwise: window 2026-10-01 to 2026-10-15, products `p1` (shoes) and `p2` (books), campaigns "
           "`c_1` (search) and `c_2` (social), events at 2026-10-03 10:00:00 UTC ingested a minute later, clicks an hour before.", "",
           "The `big_*` inputs are large (about 170 MB together; the other cases about 15 MB) and are meant to be regenerated, not committed.", "",
           f"## Cases ({len(main)})", "", "| Case | What it contains | Rule sentence it targets |", "| --- | --- | --- |"]
    for c in main:
        what = c.what + (f" **Note:** {c.note}" if c.note else "")
        out.append(f"| `{c.name}` | {cell(what)} | {cell(c.rule)} |")
    out += ["", f"## Disputed cases ({len(disputed)}), in `tests/adversarial-disputed/`", "",
            "The two references disagree on these (or one of them crashes), so there is no `expected/`. Each case folder holds "
            "`ref-a/` and `ref-b/` with what each reference wrote (or `crash.txt`).", "",
            "| Case | What it contains | Rule sentence it targets | The disagreement |", "| --- | --- | --- | --- |"]
    for c in disputed:
        what = c.what + (f" **Note:** {c.note}" if c.note else "")
        out.append(f"| `{c.name}` | {cell(what)} | {cell(c.rule)} | {cell(notes.get(c.name, ''))} |")
    out.append("")
    SUITE.mkdir(parents=True, exist_ok=True)
    (SUITE / "README.md").write_text("\n".join(out), encoding="utf-8")


DISPUTE_NOTES = {
    "csv_event_header_line_as_data":
        "ref-a stops with `ValueError: ingest_ts is not valid F1`; ref-b (and the fast binary) drop the line before dedup, so the "
        "later event `event_id` by `u2` is kept. The rules do not say: the line breaks their promise about `ingest_ts`. Dropping "
        "the line is the reading that never crashes; treating it as the kept copy (and so dropping `u2`'s event) is also defensible.",
    "files_hidden":
        "ref-a (and the fast binary) skip names starting with `.`, as a shell glob `*.csv` does; ref-b tests only the `.csv` "
        "suffix and reads them. Our reading: ref-a is right (`*` in a glob does not match a leading dot, and the Outputs section "
        "also ignores names starting with `.`), but engines differ here, so this is a judgement call.",
    "ingest_year_0000":
        "ref-a (and the fast binary) accept `0000-01-01T00:00:00Z` as valid F1; ref-b rejects every year-0000 time because "
        "Python's `datetime` starts at year 1, so it drops those copies. Our reading: ref-a is right: year 0000 has the F1 shape "
        "and is not among the impossible calendar values the rules list.",
}


# ---------------------------------------------------------------------------
# Expected outputs: both references must agree
# ---------------------------------------------------------------------------
def run_impl(cmd, data, env_extra=None):
    """Runs CMD DATA OUT; returns (rows per table, None) or (None, error text)."""
    sys.path.insert(0, str(ROOT / "scripts"))
    from run_fixtures import SCHEMAS, load_actual, rows
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as out:
        env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=out, **(env_extra or {}))
        try:
            p = subprocess.run([str(x) for x in cmd] + [str(data), out], env=env, capture_output=True, text=True, timeout=1800)
        except subprocess.TimeoutExpired:
            return None, "timeout"
        if p.returncode != 0:
            return None, f"exit {p.returncode}\n{p.stderr[-3000:]}"
        try:
            return {t: rows(load_actual(os.path.join(out, t), s)) for t, s in SCHEMAS.items()}, None
        except Exception as error:  # noqa: BLE001
            return None, f"unreadable output: {error}"


def write_csv(folder, tables):
    import polars as pl
    from run_fixtures import SCHEMAS, load_expected, rows
    folder.mkdir(parents=True, exist_ok=True)
    for table, schema in SCHEMAS.items():
        plain = {k: (pl.Int64 if v == pl.Int128 else v) for k, v in schema.items()}
        frame = pl.DataFrame(tables[table], schema=plain, orient="row")
        frame.write_csv(folder / f"{table}.csv")
        back = rows(load_expected(folder / f"{table}.csv", schema))
        if back != tables[table]:
            raise ValueError(f"{folder}/{table}.csv does not read back to the same rows")


def make_expected(c):
    data = c.root / "input"
    results = {}
    retried = False
    for name, script in REFERENCES.items():
        got, error = run_impl([PYTHON, script], data, c.ref_env)
        results[name] = (got, error)
    a, b = results["ref-a"], results["ref-b"]
    agree = a[0] is not None and a[0] == b[0]
    for sub in ("expected", "ref-a", "ref-b"):
        shutil.rmtree(c.root / sub, ignore_errors=True)
    if agree:
        try:
            write_csv(c.root / "expected", a[0])
        except ValueError as error:
            shutil.rmtree(c.root / "expected", ignore_errors=True)
            return c.name, "CSV-ROUNDTRIP", str(error)
        return c.name, "agree" if not c.disputed else "AGREE-BUT-MARKED-DISPUTED", ""
    detail = []
    for name, (got, error) in results.items():
        folder = c.root / name
        if got is None:
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "crash.txt").write_text(error, encoding="utf-8")
            detail.append(f"{name}: {error.strip().splitlines()[-1] if error.strip() else 'failed'}")
        else:
            try:
                write_csv(folder, got)
            except ValueError as error2:
                detail.append(f"{name}: {error2}")
    if a[0] is not None and b[0] is not None:
        for table in a[0]:
            only_a = [r for r in a[0][table] if r not in b[0][table]]
            only_b = [r for r in b[0][table] if r not in a[0][table]]
            if only_a or only_b:
                detail.append(f"{table}: only ref-a {only_a[:6]} / only ref-b {only_b[:6]}")
    return c.name, "DISAGREE" if c.disputed else "DISAGREE-UNMARKED", "\n    ".join(detail)


def check_disputed(cmd, selected):
    failed = 0
    for c in selected:
        if not c.disputed:
            continue
        from run_fixtures import SCHEMAS, load_expected, rows
        got, error = run_impl(cmd, c.root / "input")
        if got is None:
            print(f"{c.name}: command failed: {error}")
            continue
        verdict = []
        for name in REFERENCES:
            folder = c.root / name
            if (folder / "crash.txt").exists():
                verdict.append(f"{name} crashed")
                continue
            ref = {t: rows(load_expected(folder / f"{t}.csv", s)) for t, s in SCHEMAS.items()}
            verdict.append(f"{'matches' if ref == got else 'DIFFERS from'} {name}")
        print(f"{c.name}: " + ", ".join(verdict))
        print(f"    output: user_daily {got['user_daily'][:8]}\n            campaign_daily {got['campaign_daily'][:8]}")
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--expected", action="store_true", help="run both references and write expected/")
    parser.add_argument("--no-inputs", action="store_true", help="do not rewrite the inputs")
    parser.add_argument("--only", nargs="*", default=None, help="case name prefixes")
    parser.add_argument("--jobs", type=int, default=6)
    parser.add_argument("--check-disputed", action="store_true")
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    all_cases()
    selected = [c for c in CASES if args.only is None or any(c.name.startswith(p) for p in args.only)]
    if args.check_disputed:
        cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
        sys.path.insert(0, str(ROOT / "scripts"))
        return check_disputed(cmd, selected)
    if not args.no_inputs:
        for c in selected:
            c.write()
        current = {c.root for c in CASES}
        for folder in (SUITE, DISPUTED):
            if folder.is_dir():
                for entry in sorted(folder.iterdir()):
                    if entry.is_dir() and entry not in current:
                        print(f"note: {entry} is not a current case of this script (stale?)", file=sys.stderr)
        print(f"wrote {len(selected)} case inputs ({sum(not c.disputed for c in CASES)} in {SUITE.name}, "
              f"{sum(c.disputed for c in CASES)} in {DISPUTED.name})")
    write_readme(DISPUTE_NOTES)
    if args.expected:
        sys.path.insert(0, str(ROOT / "scripts"))
        bad = 0
        with ThreadPoolExecutor(args.jobs) as pool:
            for name, status, detail in pool.map(make_expected, selected):
                if status != "agree":
                    bad += 1
                    print(f"{status:<10} {name}\n    {detail}")
        print(f"{len(selected) - bad}/{len(selected)} cases: both references agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
