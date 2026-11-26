#!/bin/sh
# Counters for a base commit (default HEAD) vs the working tree on ss-pmu, interleaved.
# Base is exported with git archive into its own directory, so the two builds cannot mix.
#   pmu_ab.sh [BASE_REV=HEAD]       env NCPU (default 1)
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
rev=${1:-HEAD}
NAME=ss-pmu "$here/cloud.sh" ssh 'rm -rf ~/ab-base && mkdir -p ~/ab-base'
git -C "$root" archive "$rev" pipeline | NAME=ss-pmu "$here/cloud.sh" ssh 'tar -x -C ~/ab-base'
NAME=ss-pmu "$here/cloud.sh" sync
NAME=ss-pmu "$here/cloud.sh" ssh "source ~/.cargo/env; export RUSTFLAGS='-C target-cpu=native';
  (cd ~/ab-base/pipeline && touch src/*.rs && cargo build --release -q --target-dir ~/ab-base-target 2>&1 | grep -E '^error' -A5; cp ~/ab-base-target/release/pipeline ~/pmu-base);
  (cd ~/shopping-sessions/pipeline && touch src/*.rs && cargo build --release -q 2>&1 | grep -E '^error' -A5; cp target/release/pipeline ~/pmu-new);
  md5sum ~/pmu-base ~/pmu-new | awk '{print \$1}' | uniq | wc -l | sed 's/^/distinct binaries: /';
  for r in 1 2; do for b in base new; do sudo rm -rf ~/o1; printf '%-5s' \$b; sudo env NCPU=${NCPU:-1} perf stat -x, -e instructions,cycles,branch-misses -- ~/pmu-\$b ~/data20 ~/o1 2>&1 | awk -F, '/instructions|cycles|branch/ {printf \"%s %.3fG  \", \$3, \$1/1e9} /^(parse|dedup|sessions|total) / {printf \"%s %s  \", \$1, \$2} END {print \"\"}'; done; done"
