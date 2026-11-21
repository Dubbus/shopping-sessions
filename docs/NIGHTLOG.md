# Night log (branch `perf`)

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
