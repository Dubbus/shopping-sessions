# Night log (branch `perf`)

## Morning summary (2026-11-21, 08:45)

**Submit `2f45fb1317c24bf89fd7406d7914a7197f98a5b2`** (branch `perf`; later commits only add this log).

- **Verified:** 169/172 fixtures + adversarial (the 3 known >42-day windows), dev answer MATCH,
  20M output identical to master's independent implementation; 60M output identical too
  (checked on 85dd944; 2f45fb1 changes partition counts only and passes the same gate).
- **Reference sweeps** (pipeline vs both Python references, 300k events per seed): 150/150 seeds
  agree - seeds 100-139 and 200-239 on 5a2c260, 300-339 on 85dd944, 400-429 on 2f45fb1.
- **Speed:** container median on ss-bench 1.334 s at the end vs 1.319-1.351 s at the start, but
  the rig drifted by +-4% through the night, so compare the interleaved A/Bs instead. Kept:
  event files read through a mapping instead of `pread` (-1.8%, 246d003), dedup output reusing
  finished event blocks (-1.4%, 85dd944), event partitions sized for 64k events (~-1% at 20M,
  -3% at 60M, 2f45fb1). Together about -4% against clean-slate 12ad3e6.
- **Dropped (measured, no win):** dedup bitmap 4/16 bits per event, thread counts other than 8,
  chunk sizes other than 512 KB, person partition count +-1 (plus, earlier: columnar AVX-512
  timestamps, branch-free timestamps, MADV_POPULATE_READ, thread pinning, plain stores, no AVX-512).
- **clean-slate merged:** 8b8545e and 12ad3e6 (merged before the loop started, ef56846); no
  new clean-slate commits appeared overnight.
- **60M safety:** about 3.5-3.6 s native warm, peak RSS ~11 GB (mapped input included) of 24 GB.
- VMs stopped; overnight loop cancelled.


Hourly loop: merge new `clean-slate` commits into `perf` -> `scripts/verify.sh` -> push only if it
passes -> `scripts/rig_docker.sh 5` -> one experiment from the queue (A/B on ss-bench with
alternating runs, kept only on a measured win) -> a line here.

Baseline at the start (02:30): `perf` 5a2c260 = clean-slate 12ad3e6 + mmap reads.
Container on ss-bench (Cascade Lake, scorer flags, 20M): 1.33-1.35 s median. 60M native 3.57 s,
6.5 GB peak, output equal to master's.

## Experiment queue (top first; strike through when done)

1. ~~Dedup bitmap size: `count * 8` bits now; try 4 and 16 (knob) - fewer L1 misses vs more candidates.~~ No win (see log).
2. ~~Thread count: NCPU 7 / 8 / 9 / 10 on ss-bench (oversubscription can hide stalls).~~ 8 is best.
3. ~~Block reuse: survivors' scatter takes blocks freed by dedup (already faulted pages) instead of
   new huge pages - kernel page zeroing is ~5.7% of samples.~~ Kept (85dd944).
4. ~~Chunk size with mmap reads: SS_CHUNK_KB 256 / 512 / 1024 / 2048 (fault-around and scheduling).~~ 512 stays.
5. ~~Event partition count (event_bits) +-1 and person partition count +-1 with mmap reads in place.~~ Event: one bit fewer kept (2f45fb1); person: no win.

## Log
- 03:41 merge: no new clean-slate commits.
- 03:45 verify on 5a2c260+log: 169/172 (3 known), dev MATCH, 20m MATCH.
- 03:50 container (ss-bench, 5 runs): median 1.319 s (min 1.307).
- 04:05 queue 1, dedup bitmap bits per event (4 rounds, total ms): 8 -> 1120/1122/1119/1126; 4 -> 1138/1129/1130/1135 (worse); 16 -> 1129/1120/1127/1129 (dedup -1 ms, total no better). Kept 8; code reverted.
- 04:06 sweep 1 (seeds 100-139, 300k events, pipeline vs both references): 40/40 agree. Started seeds 200-239 on the current head (~/sweep2.log).
- 04:41 merge: no new clean-slate commits.
- 04:44 verify: 169/172 (3 known), dev MATCH, 20m MATCH.
- 04:50 container (5 runs): median 1.340 s (min 1.337); rig drift vs 03:50 (1.319).
- 05:00 queue 2, threads (4 rounds, total ms): NCPU=8 1134-1138; 7 1181-1190; 9 1137-1152; 10 1144-1170; 12 1158-1192. 8 stays.
- 05:00 sweep 2 (seeds 200-239, 300k, vs both references): 40/40 agree. 80/80 seeds overall tonight.
- 05:41 merge: no new clean-slate commits.
- 05:44 verify: 169/172 (3 known), dev MATCH, 20m MATCH. Container (5 runs): median 1.351 s.
- 06:05 queue 3, block reuse (SS_REUSE), 5 rounds at 20M: dedup 204 -> 196 ms, total 1143 -> 1127 ms (-1.4%), every reuse run faster. Made default (SS_NO_REUSE=1 turns it off), verified, committed 85dd944, pushed.
- 06:10 container after 85dd944 (5 runs): median 1.367 s - rig drift between back-to-back batches; the interleaved native A/B is the reliable signal.
- 06:20 60M check with reuse: output MATCHES master; peak RSS ~11.1 GB with or without reuse (mapped input counted). One 12.4 s run was a cold page cache (60M files partly evicted); warm, 4 rounds: reuse 3.57-3.62 s vs no reuse 3.59-3.77 s, dedup 652 vs 663 ms - neutral to slightly better.
- 06:20 sweep 2 still 40/40 (done).
- 06:41 merge: no new clean-slate commits.
- 06:44 verify on 85dd944: 169/172 (3 known), dev MATCH, 20m MATCH.
- 06:50 container (5 runs): median 1.352 s (min 1.347).
- 07:05 queue 4, chunk size with mapped reads (4 rounds, total ms): 512 1135-1145; 256 1132-1140 (parse identical, within noise); 1024 1140-1153; 2048 1165-1171. 512 stays.
- 07:06 sweep 3 started on 85dd944 (block reuse): seeds 300-339, ~/sweep3.log.
- 07:41 merge: no new clean-slate commits.
- 07:44 verify on 85dd944: 169/172 (3 known), dev MATCH, 20m MATCH. Container (5 runs): median 1.383 s (rig drifting up through the night: 1.319 -> 1.383 on near-identical code).
- 08:00 queue 5, partitions (4 rounds, 20M, total ms): default (event 10 bits) 1145-1152 (+1 outlier); event 9: 1134-1149, every round better (parse -12 ms); event 11: 1158-1296; person 8: 1156-1180; person 10: 1155-1170. 60M: event 12 (default) 3.67-3.94 s vs 11: 3.53-3.62 s.
- 08:05 kept: event partitions sized for 64k events (rule, so it scales: 9 bits at 20M, 11 at 60M). Verified, committed 2f45fb1, pushed.
- 08:07 sweep 3 (seeds 300-339 on 85dd944): 40/40 agree. Sweep 4 started on 2f45fb1 (seeds 400-429, ~/sweep4.log).

## Branch `dense` (2026-11-21, evening)

- **Numbered path** (`pipeline/src/dense.rs`, written from the generator source, not from any
  other entry): event/click/user IDs are base36 of `scramble(number, key)` with per-dataset
  keys; keys found from a sample (most used user = whale alias 0; whale rows' event/click
  numbers are small, brute force over that range). Events placed by number into 16k-slot
  partitions (dedup = smallest ingest per slot), read in slot order = each person's events
  grouped and time-sorted, sessionized on the spot; people at partition edges stitched;
  kiosks, split people, odd IDs take an exact slow path by name; any broken assumption gives
  up before writing and the ordinary pipeline runs (`SS_DENSE=0` forces that).
- Gate: 30 unit tests (randomized cut vs csv::split, AVX-512 decode8 vs scalar), fixtures
  36/36, adversarial 169/172 (3 known), dev MATCH, 20M MATCH vs master, rig dev+20M MATCH.
- ss-bench native 20M: 671 ms (keys 16, parse 407, people 27, clicks 40, events 125,
  stitch 20, write 34) vs 817 ms ordinary; container median 0.842 s (was 1.008) -> board ~0.79.
- Per event line (ss-pmu, 1 thread): cut 183 instr / 68 cycles, ID decode + ingest 176 / 43,
  ts + validity 186 / 52. A 0.30 s board needs ~70 cycles per line in total.
- Tried and dropped: chained lines (newline from each line's own second window, no newline
  pass): parse 407 -> 444 ms.
- **Board: `ef4e258` scored 0.79** (predicted 0.77-0.82 from 0.842 s container x 0.94).
- **Safety (ef4e258's code):** dense vs ordinary pipeline, 2M events, seeds 500-539: 40/40
  MATCH, none gave up (`scripts/dense_sweep.sh`). Dense vs both Python references, 200k
  events, seeds 600-699 (`sweep.py`), dense path confirmed taken. 60M (~/data60, ss-bench):
  2.02 s warm (6.8 s cold cache), peak RSS 9.5 GB, MATCH vs ordinary (2.49 s).
- **Ceiling (container, ss-bench, scorer flags, 5 interleaved pairs):** full run 0.838-0.852 s;
  SS_DX=9 (event lines not even cut, so no event stage either) 0.377-0.383 s. Everything
  except events (container start, reading the files, clicks, links, people, an empty write)
  is therefore ~0.38 s container = ~0.36 board. With event parsing and the whole event stage
  free, this design still cannot reach 0.30.
