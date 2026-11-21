#!/usr/bin/env bash
# The GCP benchmark instance (an n2-standard-8, like the scorer's machines).
#   cloud.sh ssh [command]   run a command (or open a shell) on the instance
#   cloud.sh sync            copy this checkout there, as ~/shopping-sessions
#   cloud.sh start|stop|status|delete
# The instance was created to stop itself after 4 hours of running; `start` begins a new 4 hours.
set -euo pipefail
NAME=${NAME:-ss-bench}; ZONE=${ZONE:-us-central1-a}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ip() { gcloud compute instances describe "$NAME" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)'; }
SSH=(ssh -i "$HOME/.ssh/google_compute_engine" -o "UserKnownHostsFile=$HOME/.ssh/google_compute_known_hosts"
     -o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=15)
case "${1:?usage: cloud.sh ssh|sync|start|stop|status|delete}" in
  ssh) shift; "${SSH[@]}" "$USER@$(ip)" "$@" ;;
  sync)
    rsync -az --delete -e "${SSH[*]}" --exclude .git --exclude .venv --exclude target/ --exclude data/ \
      --exclude out/ --exclude .DS_Store --exclude __pycache__ "$ROOT/" "$USER@$(ip):shopping-sessions/" ;;
  start) gcloud compute instances start "$NAME" --zone "$ZONE" ;;
  stop) gcloud compute instances stop "$NAME" --zone "$ZONE" ;;
  status) gcloud compute instances describe "$NAME" --zone "$ZONE" --format='value(status,lastStartTimestamp)' ;;
  delete) gcloud compute instances delete "$NAME" --zone "$ZONE" ;;
  *) echo "unknown command: $1" >&2; exit 2 ;;
esac
