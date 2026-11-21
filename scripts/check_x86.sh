#!/bin/sh
# Build the x86_64 AVX2 path and check it under Rosetta 2 on Apple silicon.
# BMI2 is left out: Rosetta mis-executes it (segfaults), real Cascade Lake scorers have it.
set -eu
cd "$(dirname "$0")/.."
export RUSTFLAGS="-C target-feature=+avx2,+fma,+bmi1,+lzcnt,+popcnt"
cargo test --release -q --target x86_64-apple-darwin
cargo build --release -q --target x86_64-apple-darwin
PY=${PY:-.venv/bin/python}
$PY scripts/run_fixtures.py fixtures -- target/x86_64-apple-darwin/release/sessions | tail -1
rm -rf out && DATA_DIR=dev OUTPUT_DIR=out target/x86_64-apple-darwin/release/sessions 2>/dev/null
$PY scripts/compare.py out dev-answer | tail -1
