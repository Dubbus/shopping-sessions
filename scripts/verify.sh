#!/bin/sh
# Build, unit tests, fixtures, dev answer, and the 20M differential check. Fails loudly.
set -eu
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
cargo build --release -q
cargo test --release -q 2>&1 | grep -q 'test result: ok' || { echo "unit tests FAILED"; exit 1; }
$PY scripts/run_fixtures.py fixtures -- target/release/sessions | tail -1 | grep -q '36/36' || { echo "fixtures FAILED"; exit 1; }
rm -rf out && DATA_DIR=dev OUTPUT_DIR=out target/release/sessions 2>/dev/null
$PY scripts/compare.py out dev-answer | tail -1 | grep -q MATCH || { echo "dev FAILED"; exit 1; }
if [ -n "${DIFF20:-}" ]; then
  rm -rf out2 && DATA_DIR="$DIFF20" OUTPUT_DIR=out2 target/release/sessions 2>/dev/null
  $PY scripts/compare.py out2 "${DIFF20_REF:-out3}" | tail -1 | grep -q MATCH || { echo "20M diff FAILED"; exit 1; }
fi
echo "verify OK"
