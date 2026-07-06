#!/bin/sh
# Register Kafka Connect connectors (source + sink) once Connect is ready.
# Polls the REST API with retry/backoff to handle the slow Connect startup.

set -eu

CONNECT_URL="${CONNECT_URL:-http://debezium-connect:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/connectors}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-30}"
RETRY_DELAY="${RETRY_DELAY:-10}"

log() { echo "[setup $(date -u +%H:%M:%S)] $*"; }

connector_name() {
    file="$1"
    grep -oE '"name"[[:space:]]*:[[:space:]]*"[^"]+"' "$file" | head -n1 | cut -d'"' -f4
}

wait_for_connect() {
    attempt=1
    while [ $attempt -le $MAX_ATTEMPTS ]; do
        if wget -q --spider "${CONNECT_URL}/connectors"; then
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
    file="$1"
    name=$(connector_name "$file")
    log "registering connector: $name"

    if wget -q -O /dev/null "${CONNECT_URL}/connectors/${name}"; then
        log "connector $name already exists — skipping"
        return 0
    fi

    if wget -qO- \
        --header "Content-Type: application/json" \
        --post-file "$file" \
        "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
        log "registered $name"
        return 0
    fi

    log "ERROR: failed to register $name"
    wget -S -O - \
        --header "Content-Type: application/json" \
        --post-file "$file" \
        "${CONNECT_URL}/connectors" || true
    return 1
}

verify_running() {
    name="$1"
    attempt=1
    while [ $attempt -le 20 ]; do
        state=$(wget -qO- "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null \
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
        name=$(connector_name "$file")
        verify_running "$name" || true
    done
    log "setup complete"
}

main "$@"
