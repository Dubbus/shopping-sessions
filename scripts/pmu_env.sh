#!/bin/sh
# Instructions/cycles/branch-misses for several env settings on ss-pmu, single thread.
#   pmu_env.sh "SS_ABLATE=1 SS_ABLATE=2 ..."   (each item may hold several VAR=x joined by ',')
set -eu
here=$(cd "$(dirname "$0")" && pwd)
NAME=ss-pmu "$here/cloud.sh" sync
NAME=ss-pmu "$here/cloud.sh" ssh "source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && RUSTFLAGS='-C target-cpu=native' cargo build --release -q 2>&1 | grep -E '^error' -A5;
  for cfg in $1; do sudo rm -rf ~/o1; printf '%-22s' \"\$cfg\";
    sudo env \$(echo \$cfg | tr ',' ' ') NCPU=1 perf stat -x, -e instructions,cycles,branch-misses -- target/release/pipeline ~/data20 ~/o1 2>&1 | awk -F, '/instructions|cycles|branch/ {printf \"%s %.2fG  \", \$3, \$1/1e9} END {print \"\"}'; done"
