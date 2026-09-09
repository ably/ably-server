#!/bin/sh
# Generate the local Docker Compose cluster's API key on first run.
#
# Run by the `init` service in docker-compose.yml against the bind-mounted
# .cluster directory, before any node starts. The key is written twice: as
# the [[keys]] config every node reads, and on its own so a client can read
# it back. Doing nothing when the config already exists keeps the key stable
# across restarts; delete .cluster to rotate it.
set -eu

dir=${ABLY_CLUSTER_DIR:-/etc/ably}
config="$dir/ably-server.toml"
keyfile="$dir/api-key"

if [ -f "$config" ]; then
  exit 0
fi

# 18 random bytes is 24 base64 characters with no padding.
key="app.key:$(head -c 18 /dev/urandom | base64)"

printf '%s\n' "$key" > "$keyfile"
printf '[[keys]]\nkey = "%s"\n' "$key" > "$config"
chmod 0644 "$keyfile" "$config"

echo "generated a new cluster API key in .cluster/api-key"
