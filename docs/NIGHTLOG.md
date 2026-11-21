# Night log (branch `perf`)

Hourly loop: merge new `clean-slate` commits into `perf` -> `scripts/verify.sh` -> push only if it
passes -> `scripts/rig_docker.sh 5` -> one experiment from the queue (A/B on ss-bench with
alternating runs, kept only on a measured win) -> a line here.

Baseline at the start (02:30): `perf` 5a2c260 = clean-slate 12ad3e6 + mmap reads.
Container on ss-bench (Cascade Lake, scorer flags, 20M): 1.33-1.35 s median. 60M native 3.57 s,
6.5 GB peak, output equal to master's.

## Experiment queue (top first; strike through when done)

1. Dedup bitmap size: `count * 8` bits now; try 4 and 16 (knob) - fewer L1 misses vs more candidates.
2. Thread count: NCPU 7 / 8 / 9 / 10 on ss-bench (oversubscription can hide stalls).
3. Block reuse: survivors' scatter takes blocks freed by dedup (already faulted pages) instead of
   new huge pages - kernel page zeroing is ~5.7% of samples.
4. Chunk size with mmap reads: SS_CHUNK_KB 256 / 512 / 1024 / 2048 (fault-around and scheduling).
5. Event partition count (event_bits) +-1 and person partition count +-1 with mmap reads in place.

## Log
