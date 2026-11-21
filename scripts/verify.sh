#!/bin/sh
# The gate every pipeline change must pass before it is timed or committed:
# unit tests, the 36 fixtures, the adversarial cases (once they exist), the dev
# answer, and reference A's output for the 20M set if it has been produced.
set -eu
cd "$(dirname "$0")/.."
PY=.venv/bin/python; BIN=pipeline/target/release/pipeline
(cd pipeline && cargo build --release 2>&1 | grep -E "^(warning|error)" -A8 || true)
(cd pipeline && cargo test --release 2>&1 | grep -E "test result|FAILED|panicked")
$PY scripts/run_fixtures.py tests/fixtures -- $BIN | tail -1
if [ -d tests/adversarial ] && ls tests/adversarial/*/expected >/dev/null 2>&1; then
  $PY scripts/run_fixtures.py tests/adversarial -- $BIN | grep -v "^ok" | tail -15
fi
rm -rf out && $BIN data/dev out 2>/dev/null && echo "dev: $($PY scripts/compare.py out tests/dev-answer | tail -1)"
if [ -d data/out-20m-ref ]; then
  rm -rf out && NCPU=8 $BIN data/20m out 2>/dev/null && echo "20m: $($PY scripts/compare.py out data/out-20m-ref | tail -1)"
fi
