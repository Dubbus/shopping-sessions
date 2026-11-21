# Scorer-like benchmark

GCP n2-standard-16 (Intel Cascade Lake). The container runs the real Docker image with the
scorer's flags, pinned to 8 vCPUs (4 cores + hyperthread siblings), on a 20M-event dataset
(practice size), warm page cache, median of N runs. Also checks the dev answer.

    bench/vm.sh create && bench/vm.sh setup   # once (~5 min)
    bench/vm.sh bench 7                        # times origin/master
    COMMIT=abc123 bench/vm.sh bench 7          # times a specific pushed commit
    bench/vm.sh perf                           # perf profile of the native binary
    bench/vm.sh stop                           # stop billing for CPU when idle; delete when done
