#!/bin/sh
# Line-kernel loop on ss-pmu: build, unit tests (AVX-512), dev and 20M exactness, then
# single-thread instructions/cycles/branch misses per parse level, and 4-thread wall.
#   NAME=ss-pmu scripts/cloud.sh sync && NAME=ss-pmu scripts/cloud.sh ssh 'sh ~/shopping-sessions/scripts/pmu_kernel.sh [tests]'
source_env() { . ~/.cargo/env; }
source_env
cd ~/shopping-sessions/pipeline && touch src/*.rs && RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release -q 2>&1 | grep -E "^error" -A7
if [ "${1:-}" = tests ]; then RUSTFLAGS="-C target-cpu=cascadelake" cargo test --release --bin pipeline 2>&1 | grep -E "test result|panicked|left|right" | head -5; fi
cd ..; B=pipeline/target/release/pipeline
sudo rm -rf ~/o-dev ~/o1 ~/o4
$B ~/devset ~/o-dev >/dev/null 2>&1; echo "dev $(~/venv/bin/python scripts/compare.py ~/o-dev tests/dev-answer | tail -1)"
NCPU=4 $B ~/data20 ~/o4 >/dev/null 2>&1; echo "20m $(~/venv/bin/python scripts/compare.py ~/o4 ~/out20-ord | tail -1)"
for dx in 9 1 3 0; do
  sudo rm -rf ~/o1
  sudo perf stat -x, -e instructions,cycles,branch-misses -- env SS_DX=$dx NCPU=1 $B ~/data20 ~/o1 2>&1 |
    awk -F, -v dx=$dx '/instructions/ {i=$1} /cycles/ {c=$1} /branch-misses/ {b=$1} END {printf "dx=%s instr=%.3fG cycles=%.3fG bmiss=%.3fG\n", dx, i/1e9, c/1e9, b/1e9}'
done
for r in 1 2 3; do sudo rm -rf ~/o4; s=$(date +%s%N); NCPU=4 $B ~/data20 ~/o4 2>/tmp/l; e=$(date +%s%N); echo "4t wall $(( (e-s)/1000000 )) ms $(grep "dense parse" /tmp/l | tr -s " " | cut -d" " -f2-4)"; done
