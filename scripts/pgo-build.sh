#!/bin/sh
# Build a pipeline crate with profile guidance, natively, the way the image does.
# Usage: pgo-build.sh CRATE_DIR OUT_BIN [TRAIN_DATA_DIR=data/dev]
# Needs: rustup component add llvm-tools-preview
set -eu
CRATE=$(cd "$1" && pwd); OUT=$2; TRAIN=${3:-data/dev}
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
FLAGS="-C target-cpu=native"
PROFDATA=$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)
(cd "$CRATE" && RUSTFLAGS="$FLAGS -Cprofile-generate=$WORK/raw" cargo build --release --target-dir "$WORK/instrumented" 2>&1 | grep -E "^(error|warning)" -A8 || true)
"$WORK/instrumented/release/pipeline" "$TRAIN" "$WORK/out" 2>/dev/null
"$PROFDATA" merge -o "$WORK/merged.profdata" "$WORK/raw"
(cd "$CRATE" && RUSTFLAGS="$FLAGS -Cprofile-use=$WORK/merged.profdata" cargo build --release --target-dir "$WORK/final" 2>&1 | grep -E "^(error|warning)" -A8 || true)
cp "$WORK/final/release/pipeline" "$OUT"
