#!/bin/sh
# cargo test on ss-pmu (x86-64 with AVX-512), so the AVX-512 paths are the ones tested.
#   pmu_test.sh [TEST_FILTER]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
NAME=ss-pmu "$here/cloud.sh" sync
NAME=ss-pmu "$here/cloud.sh" ssh "source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && RUSTFLAGS='-C target-cpu=cascadelake' cargo test --release ${1:-} 2>&1 | grep -E '^error|test result|panicked|left|right|FAILED' -A4 | head -40"
