#!/bin/sh
# A/B two builds of the pipeline on the machine this runs on, alternating runs.
# Usage: ab.sh BASE_BIN NEW_BIN DATA_DIR [ROUNDS=4]
# Prints each run's total, then the best and median of each side.
set -eu
BASE=$1; NEW=$2; DATA=$3; ROUNDS=${4:-4}
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
: > "$OUT/base"; : > "$OUT/new"
i=0
while [ "$i" -lt "$ROUNDS" ]; do
  i=$((i + 1))
  for side in base new; do
    [ "$side" = base ] && BIN=$BASE || BIN=$NEW
    rm -rf "$OUT/o"
    NCPU=${NCPU:-8} "$BIN" "$DATA" "$OUT/o" 2>&1 | awk '/^total/ {print $2}' >> "$OUT/$side"
  done
done
for side in base new; do
  sort -n "$OUT/$side" | awk -v s=$side '{v[NR]=$1} END {printf "%-5s best %.1f  median %.1f  (ms, %d runs)\n", s, v[1], v[int((NR+1)/2)], NR}'
done
