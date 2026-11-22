#!/bin/sh
# Differential sweep of the numbered path against the ordinary pipeline (SS_DENSE=0), run on
# the rig. A seed counts only if the numbered path finished without giving up.
#   dense_sweep.sh FIRST LAST EVENTS
set -u
cd "$(dirname "$0")/.."
GEN=tools/generator/target/release/generate; BIN=pipeline/target/release/pipeline
W=/tmp/dsweep; mkdir -p $W
pass=0; fail=0; gaveup=0
for seed in $(seq "$1" "$2"); do
  d=$W/d$seed; rm -rf $d $W/a$seed $W/b$seed
  $GEN --seed $seed --events $3 --clicks $(($3 / 4)) --output $d >/dev/null 2>&1
  NCPU=8 $BIN $d $W/a$seed 2>$W/log$seed
  if ! grep -q "dense total" $W/log$seed; then gaveup=$((gaveup+1)); echo "seed $seed: dense gave up: $(grep dense $W/log$seed | tail -1)"; fi
  SS_DENSE=0 NCPU=8 $BIN $d $W/b$seed 2>/dev/null
  r=$(~/venv/bin/python scripts/compare.py $W/a$seed $W/b$seed | tail -1)
  if [ "$r" = "MATCH" ]; then pass=$((pass+1)); rm -rf $d $W/a$seed $W/b$seed $W/log$seed; else fail=$((fail+1)); echo "seed $seed: $r"; fi
done
echo "seeds $1-$2 at $3 events: $pass match, $fail differ, $gaveup gave up"
