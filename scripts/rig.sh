#!/bin/sh
# A/B on the GCP rig (ss-bench) through cloud.sh. The working tree is synced, so uncommitted
# changes can be measured before they are committed.
#   rig.sh base          build this tree there and keep it as ~/base-bin
#   rig.sh ab [ROUNDS]   build this tree as ~/new-bin, check it (dev answer, 20M output equal to
#                        ~/out20, the known-good output), then alternate base/new runs (ab.sh)
#   rig.sh stages        one run of ~/new-bin with its stage log
set -eu
here=$(cd "$(dirname "$0")" && pwd)
"$here/cloud.sh" sync
build='source ~/.cargo/env; cd ~/shopping-sessions/pipeline && touch src/*.rs && RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release -q 2>&1 | grep -E "^(error|warning: unused)" -A5; '
case "${1:?usage: rig.sh base|ab|stages}" in
  base) "$here/cloud.sh" ssh "$build cp target/release/pipeline ~/base-bin && echo base ready" ;;
  ab) "$here/cloud.sh" ssh "$build cp target/release/pipeline ~/new-bin && cd .. &&
        sudo rm -rf ~/o-dev ~/o-20 && ~/new-bin ~/dev ~/o-dev >/dev/null 2>&1 && ~/venv/bin/python scripts/compare.py ~/o-dev tests/dev-answer | tail -1 &&
        ~/new-bin ~/data20 ~/o-20 >/dev/null 2>&1 && ~/venv/bin/python scripts/compare.py ~/o-20 ~/out20 | tail -1 &&
        sh scripts/ab.sh ~/base-bin ~/new-bin ~/data20 ${2:-5}" ;;
  stages) "$here/cloud.sh" ssh "sudo rm -rf ~/o-s; ~/new-bin ~/data20 ~/o-s 2>&1" ;;
esac
