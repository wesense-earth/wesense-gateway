#!/bin/sh
# entrypoint.sh — Fix directory ownership then drop to PUID:PGID
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

mkdir -p /app/data/keys /app/data/archives
chown -R "$PUID:$PGID" /app/data

# Build uvicorn SSL args when TLS is enabled
UVICORN_SSL=""
if [ "${TLS_ENABLED}" = "true" ] && [ -f "${TLS_CERTFILE}" ] && [ -f "${TLS_KEYFILE}" ]; then
    UVICORN_SSL="--ssl-certfile=${TLS_CERTFILE} --ssl-keyfile=${TLS_KEYFILE}"
    echo "TLS enabled for storage broker (HTTPS on 8080)"
fi

exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
    uvicorn wesense_gateway.app:create_app --factory --host 0.0.0.0 --port 8080 ${UVICORN_SSL} "$@"
