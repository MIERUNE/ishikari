#!/usr/bin/env bash
set -euo pipefail

trap 'jobs -pr | xargs -r kill 2>/dev/null || true; wait' INT TERM EXIT

NUM_NODES="${NUM_NODES:-3}"
BASE_HTTP_PORT=8080
BASE_GOSSIP_PORT=7946

if [[ ! "$NUM_NODES" =~ ^[0-9]+$ ]] || (( NUM_NODES < 1 )); then
  echo "NUM_NODES must be a positive integer" >&2
  exit 1
fi

cargo build

seed_addrs=()
for ((i = 0; i < NUM_NODES; i++)); do
  http_port=$((BASE_HTTP_PORT + i))
  gossip_port=$((BASE_GOSSIP_PORT + i))
  listen_addr="[::1]:${gossip_port}"
  args=(target/debug/ishikari --http-port "$http_port" --listen-addr "$listen_addr")
  if (( ${#seed_addrs[@]} > 0 )); then
    args+=(--seeds "$(IFS=,; echo "${seed_addrs[*]}")")
  fi
  "${args[@]}" &
  seed_addrs+=("$listen_addr")
done

wait
