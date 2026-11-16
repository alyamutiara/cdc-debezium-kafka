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

extract_config_json() {
    file="$1"
    awk '/"config"[[:space:]]*:[[:space:]]*\{/ { sub(/^[^{]*\{/, "{"); printing=1 } printing { print }' "$file" | sed '$d'
}

put_json() {
    url="$1"
    payload_file="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsS -X PUT -H "Content-Type: application/json" --data-binary @"$payload_file" "$url" >/dev/null
        return $?
    fi
    # BusyBox wget lacks --method=PUT; send HTTP/1.0 PUT over nc for http:// URLs.
    host_port="${url#http://}"
    path="/${host_port#*/}"
    host_port="${host_port%%/*}"
    host="${host_port%%:*}"
    port="${host_port#*:}"
    [ "$port" = "$host_port" ] && port=80
    len=$(wc -c < "$payload_file" | tr -d '[:space:]')
    resp=$(
        {
            printf 'PUT %s HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %s\r\nConnection: close\r\n\r\n' \
                "$path" "$host_port" "$len"
            cat "$payload_file"
        } | nc "$host" "$port" 2>/dev/null || true
    )
    status=$(printf '%s\n' "$resp" | head -n1 | awk '{print $2}')
    case "$status" in
        200|201) return 0 ;;
        *) return 1 ;;
    esac
}

register_connector() {
    file="$1"
    name=$(connector_name "$file")
    if [ -z "$name" ]; then
        log "ERROR: failed to extract connector name from $file"
        return 1
    fi
    log "registering connector: $name"

    if wget -q -O /dev/null "${CONNECT_URL}/connectors/${name}"; then
        tmp_cfg=$(mktemp)
        extract_config_json "$file" > "$tmp_cfg"
        if put_json "${CONNECT_URL}/connectors/${name}/config" "$tmp_cfg"; then
            rm -f "$tmp_cfg"
            log "updated existing connector $name"
            return 0
        fi
        rm -f "$tmp_cfg"
        log "WARN: failed to update existing connector $name"
        return 1
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
        body=$(wget -qO- "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || echo '')
        state=$(echo "$body" | grep -oE '"state"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | cut -d'"' -f4)
        if [ "$state" = "RUNNING" ] && ! echo "$body" | grep -qE '"state"[[:space:]]*:[[:space:]]*"FAILED"'; then
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
    # Register source connectors before sink connectors.
    for file in "${CONNECTORS_DIR}"/source-*.json "${CONNECTORS_DIR}"/*.json; do
        [ -f "$file" ] || continue
        case "$file" in
            "${CONNECTORS_DIR}"/source-*.json) ;;
            *)
                base=$(basename "$file")
                case "$base" in
                    source-*.json) continue ;;
                esac
                ;;
        esac
        register_connector "$file"
        name=$(connector_name "$file")
        if [ -z "$name" ]; then
            log "ERROR: failed to extract connector name from $file"
            return 1
        fi
        verify_running "$name" || true
    done
    log "setup complete"
}

main "$@"
