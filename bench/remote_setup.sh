#!/bin/bash
# One-time VM setup: Docker, Rust, the repo, the generator, a 20M (practice-size) and a 60M (final-size) dataset.
set -euxo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io git build-essential bc python3-venv linux-tools-common "linux-tools-$(uname -r)" >/dev/null
sudo usermod -aG docker "$USER"
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
source ~/.cargo/env
[ -d shopping-sessions ] || git clone -q https://github.com/Dubbus/shopping-sessions.git
tar -xzf ss-gen.tgz && tar -xzf ss-ans.tgz
(cd generator && cargo build --release -q && cp target/release/generate ~/generate)
python3 -m venv ~/venv && ~/venv/bin/pip install -q polars
lscpu | grep -E 'Model name|Thread|Core|Socket|NUMA node0'
cat /sys/kernel/mm/transparent_hugepage/enabled
