#!/bin/bash
# Build the repo's Dockerfile at a commit and time it exactly like the scorer:
# container start to exit, scorer docker flags, 8 pinned vCPUs (4 cores + their HT siblings).
#   remote_bench.sh [N|perf] [commit]
set -euo pipefail
source ~/.cargo/env
MODE=${1:-5}; COMMIT=${2:-}
cd ~/shopping-sessions
git fetch -q origin && git checkout -q "${COMMIT:-origin/master}"
echo "commit $(git rev-parse --short HEAD)"
# datasets (generator lives outside the repo; build it once)
[ -d ~/data20 ] || ~/generate --seed 7 --events 20000000 --clicks 5000000 --output ~/data20 >/dev/null
[ -d ~/dev ] || ~/generate --seed 1 --events 1000000 --clicks 250000 --output ~/dev >/dev/null
# 8 vCPUs = 4 physical cores and their hyperthread siblings
CPUS=$(for c in 0 1 2 3; do cat /sys/devices/system/cpu/cpu$c/topology/thread_siblings_list; done | tr '\n' ',' | sed 's/,$//')
if [ "$MODE" = native ]; then
  # quick loop: native binary, same CPU set, env overrides via BENCH_ENV, median of 5
  RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release -q
  for cfg in ${BENCH_ENV:-NONE=1}; do
    for i in 1 2 3 4 5 6; do
      rm -rf ~/out-n; s=$(date +%s.%N)
      env $(echo $cfg | tr ';' ' ') DATA_DIR=$HOME/data20 OUTPUT_DIR=$HOME/out-n NCPU=8 taskset -c $CPUS target/release/sessions 2>~/last-n.log
      e=$(date +%s.%N); echo "$e - $s" | bc
    done | tail -5 | sort -n | awk -v c="$cfg" '{t[NR]=$1} END {printf "%-28s median %.3f  min %.3f  max %.3f\n", c, t[3], t[1], t[5]}'
  done
  grep -E 's (links done|parse|dedup|sessions|done)' ~/last-n.log | tr '\n' ' '; echo
  exit
fi
sg docker -c "docker build -q -t ss:bench ." >/dev/null
run() { # data out
  sudo rm -rf "$2" /tmp/ss-scratch; mkdir -p "$2" /tmp/ss-scratch; chmod 777 "$2" /tmp/ss-scratch
  sg docker -c "docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpuset-cpus $CPUS --memory 24g --memory-swap 24g --user 1000:1000 \
    -v $1:/data:ro -v $2:/output -v /tmp/ss-scratch:/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 ss:bench"
}
run ~/dev ~/out-dev 2>/dev/null
~/venv/bin/python scripts/compare.py ~/out-dev ~/dev-answer | tail -1
if [ "$MODE" = perf ]; then
  RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --profile profiling --features prof -q
  sudo perf record -F 2999 -g -o ~/perf.data -- env DATA_DIR=$HOME/data20 OUTPUT_DIR=$HOME/out-perf NCPU=8 \
    taskset -c $CPUS target/profiling/sessions 2>/dev/null
  sudo perf report -i ~/perf.data --no-children --percent-limit 1 --stdio 2>/dev/null | grep -E '^ +[0-9]' | head -40
  exit
fi
run ~/data20 ~/out20 >/dev/null 2>&1  # warm the page cache
for i in $(seq "$MODE"); do
  s=$(date +%s.%N); run ~/data20 ~/out20 2>~/last.log; e=$(date +%s.%N)
  echo "$e - $s" | bc
done | sort -n | awk '{t[NR]=$1} END {printf "runs %s  min %.3f  median %.3f  max %.3f\n", NR, t[1], t[int((NR+1)/2)], t[NR]}'
grep -E 's (links done|parse|dedup|sessions|done)' ~/last.log | tr '\n' ' '; echo
