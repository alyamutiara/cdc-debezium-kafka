#!/usr/bin/env bash
# Register Kafka Connect connectors (source + sink) once Connect is ready.
# Polls the REST API with retry/backoff to handle the slow Connect startup.

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://debezium-connect:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/connectors}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-30}"
RETRY_DELAY="${RETRY_DELAY:-10}"

log() { echo "[setup $(date -u +%H:%M:%S)] $*"; }

wait_for_connect() {
    local attempt=1
    while [ $attempt -le $MAX_ATTEMPTS ]; do
        if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
            log "Kafka Connect is ready (attempt $attempt)"
            return 0
        fi
        log "Kafka Connect not ready (attempt $attempt/$MAX_ATTEMPTS) — sleeping ${RETRY_DELAY}s"
        sleep "$RETRY_DELAY"
        attempt=$((attempt + 1))
    done
    log "ERROR: Kafka Connect did not become ready"
    return 1
}

register_connector() {
    local file="$1"
    local name
    name=$(basename "$file" .json)
    log "registering connector: $name"

    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        --data @"$file" \
        "${CONNECT_URL}/connectors")

    case "$status" in
        201|200)
            log "registered $name (status $status)"
            ;;
        409)
            log "connector $name already exists — skipping"
            ;;
        *)
            log "ERROR: failed to register $name (status $status)"
            curl -s -X POST -H "Content-Type: application/json" \
                --data @"$file" "${CONNECT_URL}/connectors" || true
            return 1
            ;;
    esac
}

verify_running() {
    local name="$1"
    local attempt=1
    while [ $attempt -le 20 ]; do
        local state
        state=$(curl -fsS "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null \
            | grep -oE '"state":"[^"]+"' | head -n1 | cut -d'"' -f4)
        if [ "$state" = "RUNNING" ]; then
            log "$name is RUNNING"
            return 0
        fi
        log "$name state: ${state:-unknown} (attempt $attempt/20)"
        sleep 5
        attempt=$((attempt + 1))
    done
    log "WARN: $name did not reach RUNNING state"
    return 0
}

main() {
    wait_for_connect
    for file in "${CONNECTORS_DIR}"/*.json; do
        [ -f "$file" ] || continue
        register_connector "$file"
        name=$(basename "$file" .json)
        verify_running "$name" || true
    done
    log "setup complete"
}

main "$@"
