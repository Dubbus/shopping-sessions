#!/bin/sh
# Instruction-weighted source lines of the stage whose symbol matches PATTERN, on ss-pmu.
#   pmu_lines.sh PATTERN [EVENT=instructions] [N=30]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
NAME=ss-pmu "$here/cloud.sh" sync
NAME=ss-pmu "$here/cloud.sh" ssh "source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && CARGO_PROFILE_RELEASE_DEBUG=true RUSTFLAGS='-C target-cpu=native' cargo build --release -q --target-dir ~/pt 2>&1 | grep -E '^error' -A5;
  sudo rm -rf ~/o1; sudo perf record -e ${2:-instructions} -c 20011 -o ~/pl.data -- env NCPU=1 ~/pt/release/pipeline ~/data20 ~/o1 >/dev/null 2>&1;
  S=\$(sudo perf report -i ~/pl.data --no-children --stdio 2>/dev/null | grep -E '^ +[0-9]' | grep -F '$1' | head -1 | sed -E 's/.*\] //');
  sudo perf annotate -i ~/pl.data --stdio -l --symbol=\"\$S\" 2>/dev/null | grep -E '^ +[0-9]+\.[0-9]+ +[a-z_]+\.rs:[0-9]+' | sort -rn | head -${3:-30}"
