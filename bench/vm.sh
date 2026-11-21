#!/bin/sh
# Scorer-like benchmark VM on GCP: n2 (Intel Cascade Lake), Linux, Docker.
# The container gets 8 pinned vCPUs (4 cores x 2 hyperthreads) of a 16-vCPU VM, like the scorer.
#   bench/vm.sh create | setup | bench [N] | perf | ssh | stop | start | delete
set -eu
NAME=${NAME:-ss-bench}
ZONE=${ZONE:-us-central1-a}
TYPE=${TYPE:-n2-standard-16}
here=$(cd "$(dirname "$0")" && pwd)
ssh_() { gcloud compute ssh "$NAME" --zone "$ZONE" --quiet --command "$1"; }
case "${1:-}" in
create)
  gcloud compute instances create "$NAME" --zone "$ZONE" --machine-type "$TYPE" \
    --min-cpu-platform "Intel Cascade Lake" \
    --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud \
    --boot-disk-size 100GB --boot-disk-type pd-ssd ;;
setup)
  spec="$here/../spec"
  tar -C "$spec/generator" --exclude target -czf /tmp/ss-gen.tgz generator
  tar -C "$spec/dev-answer" -czf /tmp/ss-ans.tgz dev-answer
  gcloud compute scp --zone "$ZONE" --quiet "$here/remote_setup.sh" "$here/remote_bench.sh" /tmp/ss-gen.tgz /tmp/ss-ans.tgz "$NAME":
  ssh_ "bash remote_setup.sh" ;;
bench) ssh_ "bash remote_bench.sh ${2:-5} ${COMMIT:-}" ;;
perf)  ssh_ "bash remote_bench.sh perf ${COMMIT:-}" ;;
ssh)   gcloud compute ssh "$NAME" --zone "$ZONE" ;;
stop)  gcloud compute instances stop "$NAME" --zone "$ZONE" ;;
start) gcloud compute instances start "$NAME" --zone "$ZONE" ;;
delete) gcloud compute instances delete "$NAME" --zone "$ZONE" --quiet ;;
*) echo "usage: $0 create|setup|bench [N]|perf|ssh|stop|start|delete"; exit 2 ;;
esac
