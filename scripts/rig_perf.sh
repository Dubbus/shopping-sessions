#!/bin/sh
# perf profile of this tree on the rig (debug-info build in its own target dir).
#   rig_perf.sh [funcs|lines PATTERN]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
"$here/cloud.sh" sync
"$here/cloud.sh" ssh 'source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && CARGO_PROFILE_RELEASE_DEBUG=true RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release -q --target-dir ~/perf-target 2>&1 | grep -E "^error" -A5;
  sudo rm -rf ~/o-p; sudo perf record -F 2999 -o ~/pp.data -- env NCPU=8 ~/perf-target/release/pipeline ~/data20 ~/o-p 2>&1 | grep ms;
  sudo perf report -i ~/pp.data --no-children --percent-limit 0.7 --stdio 2>/dev/null | grep -E "^ +[0-9]" | sed -E "s/ +pipeline +//; s/std::sys::backtrace::__rust_begin_short_backtrace::<pipeline::par::run<pipeline::([A-Za-z]+).*/stage<\1>/; s/pipeline::par::run::<pipeline::([A-Za-z]+),.*/stage<\1>/" | cut -c1-120 | head -30'
