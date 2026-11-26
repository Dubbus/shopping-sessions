#!/bin/sh
# Time the native binary on a dataset, on the machine this runs on.
# Usage: bench.sh DATA_DIR [RUNS=5] [cold]
#   cold: ask the kernel to drop the dataset from its file cache before each run.
# Env: CPUS (taskset list, default: all), NCPU (threads, default 8), MEM (default 24G),
#      BIN (default pipeline/target/release/pipeline).
# Every run sits in a systemd scope with a hard memory cap, so a mistake cannot
# take the host down, and at low priority.
set -eu
DATA=$1; RUNS=${2:-5}; MODE=${3:-warm}
BIN=${BIN:-pipeline/target/release/pipeline}
NCPU=${NCPU:-8}; MEM=${MEM:-24G}
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
pin=""; [ -n "${CPUS:-}" ] && pin="taskset -c $CPUS"
i=0
while [ "$i" -lt "$RUNS" ]; do
  i=$((i + 1))
  if [ "$MODE" = cold ]; then
    find "$DATA" -type f -exec dd if={} of=/dev/null iflag=nocache count=0 status=none \;
  fi
  rm -rf "$OUT/o"
  NCPU=$NCPU systemd-run --user --scope -q -p MemoryMax="$MEM" -p MemorySwapMax=0 \
    nice -n 5 $pin "$BIN" "$DATA" "$OUT/o" 2>&1 | awk '{printf "%s=%s ", $1, $2} END {print ""}'
done
