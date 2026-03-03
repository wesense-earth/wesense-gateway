#!/bin/sh
# entrypoint.sh — Fix directory ownership then drop to PUID:PGID
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

mkdir -p /app/data/keys /app/data/archives
chown -R "$PUID:$PGID" /app/data

exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
    uvicorn wesense_gateway.app:create_app --factory --host 0.0.0.0 --port 8080 "$@"
