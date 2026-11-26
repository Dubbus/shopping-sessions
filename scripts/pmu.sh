#!/bin/sh
# Hardware counters for this tree on ss-pmu (c4-standard-4, PMU enabled): instructions,
# cycles, branch and cache misses, single thread on the 20M set. Instructions are exact
# and noise-free, so small changes show. Usage: pmu.sh [NCPU=1]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
NAME=ss-pmu "$here/cloud.sh" sync
NAME=ss-pmu "$here/cloud.sh" ssh "source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && RUSTFLAGS='-C target-cpu=native' cargo build --release -q 2>&1 | grep -E '^error' -A5;
  sudo rm -rf ~/o1; sudo perf stat -x, -e instructions,cycles,branch-misses,L1-dcache-load-misses -- env NCPU=${1:-1} target/release/pipeline ~/data20 ~/o1 2>&1 | awk -F, '/ms\$/ || /^[a-z]+ +[0-9.]+ ms/ {print; next} /instructions|cycles|branch|L1/ {printf \"%-24s %8.3f G\\n\", \$3, \$1/1e9}'"
