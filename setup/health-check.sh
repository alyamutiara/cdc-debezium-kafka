#!/bin/sh
# Health-check sidecar loop: polls connector status and restarts FAILED connectors.
# Runs continuously, sleeping POLL_INTERVAL seconds between checks.

set -u

CONNECT_URL="${CONNECT_URL:-http://debezium-connect:8083}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"
RESTART_LOG="/tmp/cdc-restart-counts"

log() { echo "[health-check $(date -u +%H:%M:%S)] $*"; }

# Wait for Connect to be reachable
attempt=1
while ! wget -q --spider "${CONNECT_URL}/connectors"; do
    log "Kafka Connect not reachable (attempt $attempt)"
    sleep 10
    attempt=$((attempt + 1))
    if [ $attempt -gt 60 ]; then
        log "Kafka Connect never became reachable; exiting"
        exit 1
    fi
done

log "health-check sidecar started, polling every ${POLL_INTERVAL}s"

while true; do
    connectors=$(wget -qO- "${CONNECT_URL}/connectors" 2>/dev/null \
        | tr -d '[]"' | tr ',' ' ')

    if [ -z "$connectors" ]; then
        log "no connectors found"
    else
        for name in $connectors; do
            body=$(wget -qO- "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || echo '{}')
            state=$(echo "$body" | grep -oE '"state":"[^"]+"' | head -n1 | cut -d'"' -f4)
            trace=$(echo "$body" | grep -oE '"trace":"[^"]+"' | head -n1 | cut -d'"' -f4)

            if [ "$state" = "FAILED" ]; then
                log "connector $name is FAILED — restarting"
                [ -n "$trace" ] && log "  trace: $trace"
                wget -qO- "${CONNECT_URL}/connectors/${name}/restart" \
                    --header "Content-Type: application/json" \
                    --post-data '{}' >/dev/null 2>&1 \
                    || log "  restart request failed for $name"

                count=$(grep -c "^${name}:" "$RESTART_LOG" 2>/dev/null || echo 0)
                count=$((count + 1))
                grep -v "^${name}:" "$RESTART_LOG" 2>/dev/null > "${RESTART_LOG}.tmp" || true
                echo "${name}:${count}" >> "${RESTART_LOG}.tmp"
                mv "${RESTART_LOG}.tmp" "$RESTART_LOG"
                log "  restart count for $name: $count"
            else
                log "connector $name state=$state"
            fi
        done
    fi

    sleep "$POLL_INTERVAL"
done
