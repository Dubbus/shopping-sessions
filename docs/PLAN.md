# Shopping Sessions: plan to win

Status (2026-11-21, 07:10 UTC): two practice submissions scored, **1.09 s** then **0.89 s**; the
leader is at 0.65 s. The branch head is predicted at about 0.87 s. See
[Where we are](#where-we-are) for the current design and numbers; the sections before it are
the original plan.

## Goal

Lowest container run time on the scorer, with output that matches the official answer exactly.
One wrong number scores zero, and one final run over 3 minutes (or out of memory) scores zero, so
the order of priorities is fixed:

1. Exactly correct.
2. Never over the time or memory limits at 60M events.
3. Fast.

## Facts we are designing around

| Fact | Consequence |
|---|---|
| Score is container run time, start to exit | Tiny static binary in a minimal image; no interpreter or JVM startup |
| 8 vCPUs (Intel Xeon, Cascade Lake or newer, AVX-512), 24 GB, no swap | Build for that CPU; plan a memory budget for 60M events |
| No network at run time, read-only root, 64 MB `/tmp`, uid 1000 | Everything static; temp files only in `/scratch` |
| Practice: ~20M events, 2 submissions per hour. Final: ~60M events | Submissions are scarce; treat each one as an experiment |
| No newline ever appears inside a quoted field | Any `\n` is a line boundary, so files split freely across threads |
| Output may be several Parquet files, any row order | Each thread writes its own file |
| Dedup comes before validation; copies can name different users | Dedup by `event_id` cannot be merged with the per-person step |
| Whales (1–2% of events each) and shared devices (thousands of users as one person) | Per-person work must tolerate heavy skew |
| Dirt rates, file counts, window and whales all change with the seed | Nothing about a dataset is hard-coded; fast paths always have an exact fallback |

## Decisions

- **Language:** Rust, one binary.
- **Project:** our own repo. From the starters we keep only the harness: `scripts/docker_run.sh`,
  `run_fixtures.py`, `compare.py`, the `Makefile` targets and the 36 `fixtures/`.
- **Dependencies:** allowed at first to get a correct version running, then replaced by
  hand-written code one stage at a time, each swap justified by a benchmark.
- **Not doing:** Lean (cannot prove speed, and does not catch a misread rule); io_uring as a
  design basis (likely blocked in the scorer's container; at most one of the I/O variants we test).

## Pipeline design

1. **Read and split.** List every `*.csv`, cut large files into chunks at newline boundaries,
   hand chunks to 8 threads.
2. **Parse.** Our own line and field splitter from day one, because no library matches the
   dialect in `rules.md` (quotes, `\r`, empty-versus-null, malformed lines). Each line becomes a
   compact fixed-size record. Timestamps parse to epoch milliseconds through three paths (F1, F2, F3).
3. **Links to people.** Intern user IDs to dense integers; union-find over the links; each group
   is named by its byte-wise smallest ID.
4. **Dedup events.** Partition records by `event_id` hash during parsing; dedup each partition
   in parallel, keeping the smallest `ingest_ts`; then validate and drop late events.
5. **Sessionize.** Repartition survivors by person, sort by timestamp, split at gaps over 30 minutes.
6. **Clicks.** Parse, dedup, validate; sort per person. Walk clicks alongside sessions: the latest
   click within 7 days gets the credit; a sliding 7-day window gives the distinct assisting campaigns.
7. **Aggregate.** `user_daily` per person and day (with top category); `campaign_daily` in a small
   dense campaign × day array.
8. **Write Parquet.** One file per thread, uncompressed.

### Version 1 (dependencies allowed)

`memchr` for scanning, `rayon` for threads, `hashbrown` plus a fast hasher for tables, the
`parquet` crate for output, `mimalloc` as allocator. Straightforward code, tuned for clarity.

### Later versions (hand-written, each gated on a measured win)

- AVX-512 scanner producing newline, comma and quote bitmasks per 64-byte block; AVX2 and scalar
  fallbacks for our own machines.
- Fast paths for the common timestamp forms and for ID shapes the generator writes (`c_123`, etc.),
  always backed by the exact general path.
- Our own hash tables, radix partitioning and radix sorts.
- Our own Parquet writer.
- Memory taken up front in a few large blocks with huge pages; no general allocator in hot paths.
- Our own thread pool with skew-aware work splitting.
- Build tuning: target CPU, link-time and profile-guided optimisation, post-link layout optimisation.

### Design variants to benchmark rather than guess

| Stage | Variants |
|---|---|
| Reading | `mmap` · large `pread` · direct I/O |
| Dedup | Partition then per-partition table · one shared concurrent table |
| Per-person ordering | Comparison sort · radix sort · time bucketing |
| Pass structure | Separate passes · fused parse + partition |
| ID handling | Interned strings · hashed IDs with exact verification |

## Correctness strategy

- **Two independent reference implementations** in Python, each written from `rules.md` alone by a
  separate agent that sees neither the other reference nor the fast code. Disagreement between
  them means someone misread a rule.
- **Provided checks:** the 36 fixtures and the 1M-event dev answer.
- **Seed sweeps:** fast binary against both references on hundreds of generator seeds at several
  sizes, fanned out across the cluster.
- **Adversarial inputs:** hand-built files covering what the rules allow but the generator rarely
  or never writes (`\r`, non-ASCII digits, 20-digit timestamps, quote placement, exact boundaries,
  empty and header-only files, single huge files).
- **Parser fuzzing:** timestamp and CSV-line parsers fuzzed against the references.
- **Self-checks from the rules:** summed `sessions` and `revenue_cents` equal across both tables.
- **Rule:** no optimisation merges until fixtures, dev set and a seed sweep all pass.

## Safety for the final

- 60M-event runs on many seeds under the real limits (8 CPUs, 24 GB, no swap).
- Written memory budget per stage, with headroom.
- Target: practice run far below 60 s, final far below 3 minutes, on the slowest machine we test.

## Compute

| Machine | Use |
|---|---|
| Mac (ARM) | Editing, quick native runs, fixtures |
| `apt` (16 threads, 31 GB, Docker) | Native amd64 image builds; runs with the scorer's Docker flags |
| `glencoe` (6 identical cores, 31 GB) | Main local benchmark box, pinned to 8 threads; avoid backup windows |
| `rcs1`, `rcs2` (24 threads, 30 GB) | Seed sweeps; 60M safety runs |
| `course-server-1`, `-2`, `scoring` (20 threads, 15 GB) | Seed sweeps at small and medium sizes |
| GCP `n2-standard-8` (about $0.39/h, less on spot) | AVX-512 speed tuning and disk behaviour; final phase only |

None of our own machines has AVX-512. AVX-512 code is checked for correctness under Intel's
emulator and timed only on GCP and through practice submissions. The course machines are shared:
jobs run at low priority, from a user directory, with no system changes.

## Phases

| # | Phase | Done when |
|---|---|---|
| 0 | Foundation: repo, harness, generator built, dev set generated, Git remote | `make fixtures` and `make check-dev` run (and fail on an empty pipeline) |
| 1 | References: two independent Python implementations | Both pass all 36 fixtures and the dev answer, and agree on 20+ seeds |
| 2 | Version 1 in Rust with dependencies | Passes fixtures, dev answer and a seed sweep; image builds for amd64 |
| 3 | First submission | On the board; we know what feedback the scorer gives (timing, logs) |
| 4 | Test infrastructure at scale: cluster sweep runner, adversarial inputs, fuzzers, benchmark rig on `glencoe` | One command sweeps N seeds across the cluster and reports any mismatch |
| 5 | Profile and optimise stage by stage, competing variants, swapping dependencies for our own code | Each change has a before/after number and a passing sweep |
| 6 | GCP tuning: AVX-512 paths, I/O variant, build tuning | Best configuration chosen on scorer-like hardware |
| 7 | Final hardening: 60M runs on many seeds, memory budget verified, freeze | Chosen commit passes everything; submitted |

Phases 4 and 5 overlap. Phase 3 comes early on purpose: the scorer's behaviour shapes everything after it.

## Repository layout

```
Dockerfile, Makefile, README.md
docs/            this plan, the challenge description, the rules
pipeline/        the Rust pipeline (what gets scored)
reference/a, b/  the two independent Python references
tests/           the 36 fixtures and the published dev answer
tools/generator/ the organisers' dataset generator
scripts/         fixture runner, comparer, seed sweep, scorer-like docker run
data/, out/      generated datasets and outputs (ignored)
```

## Open questions

1. Is the input already in memory when a run starts, or read cold from disk? This sets the floor
   on run time and picks the I/O variant.
2. Does a practice submission return our stderr? If so we log per-stage timings.
3. What exactly does the timer cover (image pull, container start, exit)?
4. Is the final score one run or several, and on one dataset or several?
5. Does the scorer allow io_uring and huge pages?
6. Which GitHub account and repo name to submit from, and what goes in `TEAM.md`?

## Risks

| Risk | Mitigation |
|---|---|
| A misread rule passes our own tests | Two independent references; hand-written fixtures; adversarial cases |
| A fast path is wrong on a rare input | Every fast path has an exact fallback and is fuzzed against it |
| Out of memory or over time at 60M on an unlucky seed | Many-seed 60M runs under real limits; explicit memory budget |
| Tuning for our machines instead of the scorer's | GCP n2 for final tuning; practice submissions as ground truth |
| A late optimisation breaks correctness | Full sweep gates every merge; freeze before the final |

## Where we are

### Results

Whole container on `ss-bench` (GCP n2-standard-8, Cascade Lake, scorer's `docker run` flags,
input already in the page cache), 20M events unless noted:

| Commit | What changed | `ss-bench` | Leaderboard |
|---|---|---|---|
| version 1 | shared ID table, mapped input, Parquet library | 3.0 s | - |
| `478f4eb` | partitioned stages, AVX-512 tokenizer, 8-byte IDs, dictionary Parquet | 1.36 s | **1.09 s** |
| `c16006c` | hand-written Parquet writer | 1.29 s | - |
| `1f99d0e` | profile-guided image build | 1.23 s | - |
| `1609b8d` | word-at-a-time timestamps, campaign array, bucketed link numbering | about 1.16 s | **0.89 s** |
| `f3f9aaf` | two-pass dedup, constant-layout partition writer, integer sorts | 1.12 s | about 0.87 s (predicted) |

At 60M events `f3f9aaf` takes about 3.2 s on `ss-bench` with a peak of about 7 GB.
`master` (Adarsh's implementation) takes 1.99 s on the same machine and scored 1.94 s.

**Predicting the board.** Our two scored commits came in at 0.80 and about 0.77 of their
`ss-bench` time, so the prediction is `ss-bench` time x 0.78. `master`'s ratio is 0.97, so the
ratio depends on the code; it is a guide, not a law.

### The pipeline today

Every stage is parallel over partitions small enough to stay in cache; records move from one
stage to the next by being scattered on the hash of what the next stage looks up.

1. **Parse.** Files are read in 512 KB chunks into a reusable buffer (`io.rs`). One AVX-512
   pass records comma and newline positions and quote counts; lines are cut into fields from
   those with no per-byte work (`scan.rs`). IDs of up to 9 ASCII bytes pack exactly into a
   `u64`, others are interned (`key.rs`). One thread turns `links/` into the ID -> person-name
   table (`link.rs`) while the rest parse `events/`, scattered by `event_id`; `clicks/` are
   parsed last, get their person at once, and are scattered by person.
2. **Dedup.** Per `event_id` partition: two small bitmaps find the IDs that occur more than
   once, only those go through an exact table, and every survivor gets its person (rename
   table slot prefetched 16 ahead) and is scattered by person.
3. **Sessions.** Per person partition: people are numbered, their events and clicks grouped
   with a counting pass and sorted as plain integers, then one sweep per person builds
   sessions, credits clicks, counts assists and fills the daily rows.
4. **Write.** Our own Parquet writer (`output.rs`): every column dictionary-encoded, one file
   per thread for `user_daily`.

The partition writer (`scatter.rs`) stages records in a cache-resident line per partition and
flushes with non-temporal stores into huge-page blocks.

Dependencies left: `hashbrown` (three small tables), `memchr`, `xxhash-rust` (product lookup),
`mimalloc`, `libc` (one `madvise` call).

### What the measurements taught us

- **Docker returns only after the output is flushed to disk**, so output size is part of the
  score: plain Parquet cost 0.3 s at 20M and 1 s at 60M. Output is now 20 MB / 61 MB.
- **The exit itself is free** natively (15 ms); nothing is gained by freeing memory.
- **Hyperthreads add little**: 4 threads on 4 cores take 1.28 s where all 8 take 1.10 s.
- **Partition counts and chunk size barely matter** between 32 KB and 512 KB chunks and across
  a 16x range of partition counts; the cost is CPU work per record.
- **The rename lookup is the dearest step of dedup** (about half of it) even with prefetching;
  the survivor scatter is another quarter.
- **Sorting was 45% of the sessions stage** until events and clicks became integers.
- **`ss-bench` drifts by up to 10% over an hour.** Only alternating A/B runs (`scripts/ab.sh`)
  are trustworthy for changes under 5%.

### Correctness status

- 36/36 official fixtures and the published dev answer, natively and through the image.
- 180/183 adversarial cases (`tests/adversarial`, `scripts/make_adversarial.py`). The three
  failures are windows longer than 42 days, which this build refuses: timestamps are stored as
  `u32` milliseconds from `window_start - 7 days`. The generator always uses 14 days.
- Both Python references and the pipeline agree on 88 seeds at 300k events (the last 40 on
  `478f4eb`); reference A agrees at 20M.
- Output identical to `master`'s at 20M and 60M (seed 9), and at 60M for seeds 21 to 23 run
  through the image under the scorer's limits.
- Where the references disagree (`tests/adversarial-disputed`): hidden `.csv` files are not
  read; an `ingest_ts` that is not valid F1 drops its line (the rules say it cannot happen).

### Not done from the original plan

- Seed sweeps have not been rerun since `478f4eb`; everything since is checked against the
  fixtures, adversarial cases, dev answer and `master`'s output only.
- No written memory budget; measured peak is 7 GB of 24 GB at 60M.
- No cluster-wide sweep runner. The homelab is unused since a `sudo docker` wrapper script
  called itself without end on `apt` (NixOS `sudo` keeps PATH) and took the machine down.
- Windows over 42 days.

### Next, in order of expected gain

1. Rename lookup: a small fingerprint array in front of the table, so the two thirds of
   lookups that miss never leave the cache.
2. Sessions sweep: assists counted in their own compact array; cheaper person numbering.
3. Tokenizer: fewer port-5 operations per block (masks only, positions on demand).
4. Rerun the seed sweep on the head, and on every submitted commit from now on.

## Road to 0.65 s (2026-11-21, night)

**Calibration.** The scorer reads about 0.7x our GCP n2-standard-8 container time (this branch:
1.50 s container -> 1.09 s on the board; master: 2.96 s -> 2.05 s). Beating 0.65 s on the board
means about **0.85 s container / 0.65 s native** at 20M events on `ss-bench`. Today: 1.44 s
container, 1.28 s native. A 40-45% cut, every stage.

**Where the time goes now** (`ss-bench`, native, 8 threads pinned, 20M events):

| Stage | Now | Budget | How |
|---|---|---|---|
| open | 11 ms | 10 | - |
| parse | 730 ms | 330 | profile-driven: tokenizer + field conversion per line, read copy, faults |
| dedup | 266 ms | 120 | fewer passes over records; tables sized to L2; no per-record branches |
| sessions | 236 ms | 120 | grouping without hash tables; cheaper per-person sort; clicks |
| write | 39 ms | 30 | int32 count columns (smaller files flush faster) |
| exit + docker flush | ~160 ms | ~120 | smaller output; check teardown cost |

**Method.** One change at a time. Each one: `scripts/verify.sh` (fixtures, adversarial, dev
answer, 20M output identical to master's), then `scripts/ab.sh` on `ss-bench` alternating
base and new builds; kept only on a measured gain. A practice submission whenever the rig
shows a real step (>= 5%), so the board tracks the rig.

**Profile at the start** (perf, 8 threads): parse loop 33%, dedup 19%, sessions 12%,
timestamps 9%, kernel page zeroing 6%, read copy 4%, per-person sort 3%.
