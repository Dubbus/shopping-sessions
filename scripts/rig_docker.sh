#!/bin/sh
# Whole-container timing on ss-bench, like the scorer: build this tree's Dockerfile there, check
# the dev answer through the image, then time N runs on the 20M set (median).
#   rig_docker.sh [N=5]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
"$here/cloud.sh" sync
"$here/cloud.sh" ssh "cd ~/shopping-sessions && sg docker -c 'docker build -q -t ss:rig .' >/dev/null && echo image built
mkdir -p /tmp/ss-scr && chmod 777 /tmp/ss-scr
run() { sudo rm -rf \$2; mkdir -p \$2; chmod 777 \$2; s=\$(date +%s.%N); sg docker -c \"docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpuset-cpus 0-7 --memory 24g --memory-swap 24g --user 1000:1000 -v \$1:/data:ro -v \$2:/output -v /tmp/ss-scr:/scratch -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 ss:rig\" >/dev/null 2>&1; e=\$(date +%s.%N); echo \"\$e - \$s\" | bc; }
run ~/dev ~/od-dev >/dev/null; ~/venv/bin/python scripts/compare.py ~/od-dev tests/dev-answer | tail -1
run ~/data20 ~/od-20 >/dev/null; ~/venv/bin/python scripts/compare.py ~/od-20 ~/out20 | tail -1
for i in \$(seq ${1:-5}); do run ~/data20 ~/od-20; done | sort -n | awk '{t[NR]=\$1} END {printf \"container runs %d  min %.3f  median %.3f  max %.3f\\n\", NR, t[1], t[int((NR+1)/2)], t[NR]}'"
