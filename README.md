# CDC Pipeline

A self-contained, locally-runnable **Change Data Capture** pipeline that demonstrates
production-grade data engineering: Postgres → Debezium → Kafka → Python transform →
Kafka → JDBC sink → Postgres. Built to show ecosystem knowledge and engineering
judgment in a portfolio context.

## What it does

```
┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────────┐
│ Postgres │───▶│ Debezium │───▶│    Kafka     │───▶│   Python     │
│ (source) │    │ Connect  │    │ + Registry   │    │  Transform   │
└──────────┘    └──────────┘    └──────────────┘    │   Service    │
                                                    └──────┬───────┘
                                                           │ produces to
                                                    ┌──────▼───────┐
                                                    │   Kafka      │
                                                    │ output topic │
                                                    └──────┬───────┘
                                                           │
┌──────────┐    ┌──────────┐                               │
│ Postgres │◀───│ Debezium │◀──────────────────────────────┘
│ (target) │    │ JDBC Sink│
└──────────┘    └──────────┘
```

A hiring manager can `docker compose up` and watch a real CDC pipeline run end-to-end:
the data generator INSERTs/UPDATEs/DELETEs on the source Postgres, the change events
flow through Kafka, the custom Python transform service applies column mapping and
filtering, and the JDBC sink writes to the target Postgres.

The custom transform service is where the engineering judgment lives — it's not
just YAML wiring together existing tools.

## Quick start

Prerequisites: Docker with **Compose v2** (`docker compose`, not `docker-compose`).

```bash
docker compose up
```

Then in a separate terminal, watch the data flow:

```bash
# Source Postgres (port 5433)
docker exec -it cdc-postgres-source psql -U source -d ecommerce

# Target Postgres (port 5434)
docker exec -it cdc-postgres-target psql -U target -d analytics

# Try this on the source:
INSERT INTO orders (customer_id, status, total_cents) VALUES (1, 'pending', 9999);

# And watch it land in the target:
SELECT order_id, cust_id, status, total_cents FROM public.orders ORDER BY order_id DESC LIMIT 5;
```

You'll notice the column rename in action: source `id` → target `order_id`,
source `customer_id` → target `cust_id`. Also notice the `internal_notes` column
is absent in the target — it was dropped by the transform.

## What's in the box

| Service | Role |
| --- | --- |
| `postgres-source` | Source DB. E-commerce schema (orders, customers, products, order_items). `wal_level=logical`. |
| `postgres-target` | Target DB. Receives transformed CDC events. |
| `kafka` | Confluent `cp-kafka:7.6.0` in KRAFT mode (no Zookeeper). |
| `schema-registry` | Apicurio Registry (in-memory). Sets the stage for Avro in Phase B. |
| `debezium-connect` | Kafka Connect + Debezium Postgres + JDBC sink plugins. |
| `transform-service` | **Custom Python FastAPI service.** Consumes CDC events, applies transforms, produces to output topics. Exposes `/metrics` and `/health`. |
| `data-generator` | Python sidecar. INSERTs every 2s, UPDATEs every 10s, DELETEs every 60s. |
| `setup` | One-shot Alpine. Polls Connect REST, registers source + sink connectors, exits. |
| `health-check-sidecar` | Alpine. Polls connector status every 30s, restarts FAILED connectors. |

## Architecture decisions

### Why Debezium + Kafka, not a from-scratch CDC tool?

Knowing the tools data teams actually use is a stronger portfolio signal than
rebuilding log-tailing. Debezium is the standard for Postgres CDC; Kafka is
the standard streaming bus. Demonstrating how to wire them up correctly is the
job.

### Why a custom Python transform service?

The "raw Debezium → sink" pipeline is a 30-line `docker-compose.yml`. The
interesting engineering starts where you need to:

- **Filter** events (e.g., drop soft-deleted rows before they hit the warehouse)
- **Rename / drop** columns (PII, internal fields, denormalization)
- **Enrich** (joins, lookups) — Phase B
- **Observe** what the pipeline is actually doing

A custom service that does these things — and exposes Prometheus metrics for
the work — is where production thinking shows up. The same applies whether
you build the service in Python, Go, or Java; the design is what matters.

### Why FastAPI, not Flask or raw HTTP?

The transform service needs to expose `/health` and `/metrics` to the outside
world. FastAPI gives us both with no boilerplate, and uvicorn runs in a
background thread inside the same process as the Kafka consumer — one
process, two responsibilities, low ops cost.

### Why at-least-once, not exactly-once?

The transform consumer commits the Kafka offset **after** a successful produce
to the output topic. On crash between consume and produce, the event is
replayed — duplicates are possible. The JDBC sink uses `insert.mode=upsert`,
so duplicates are harmless: the second write just overwrites the first.

Exactly-once would require Kafka transactions across the consumer-producer
boundary and a transactional sink. That's a real option in production, but
the operational complexity (transaction coordinator, isolation level,
downstream idempotency keys) isn't justified for a portfolio project.

### Why Pydantic for the transform config?

Transform YAMLs are easy to typo, and a typo in production means silent data
loss: the transform silently drops or misroutes events. Pydantic v2
validates the config at startup and rejects invalid field names, missing
required keys, or unsupported filter operators with a clear error before
the service accepts traffic.

### Why KRAFT, not Zookeeper?

KRAFT is the future of Kafka (Zookeeper is being deprecated). Single-node
local dev benefits from a shorter startup chain (no second container just
to manage cluster state). The setup is slightly different (no `KAFKA_ZOOKEEPER_CONNECT`)
but the operational model is simpler.

### Why a separate health-check sidecar?

Kafka Connect's REST API is the canonical way to introspect connector health,
but Connect itself doesn't auto-restart FAILED connectors. A small sidecar
polling the REST API and POSTing `/connectors/{name}/restart` on FAILED state
catches the cases that would otherwise need a human to notice. It's 30 lines
of bash and saves a 2am page.

## DDL handling

Debezium captures schema-change events alongside data events. The transform
service classifies them:

| DDL | Action |
| --- | --- |
| `ALTER TABLE ... ADD COLUMN` | Logged. JDBC sink's `auto.evolve=true` adds the column on the target. |
| `ALTER TABLE ... DROP COLUMN` | Logged as a warning. Auto-drop is dangerous — manual intervention required. |
| `CREATE TABLE` / `DROP TABLE` | Logged and skipped. JDBC sink auto-creates target tables. |
| Other `ALTER` (type changes, etc.) | Logged and skipped. |

DDL events are processed in Kafka order, before subsequent data events, so
schema consistency is maintained. The transform service's `DdlHandler`
emits a Prometheus counter (`ddl_events_total{kind,action}`) so you can
alert on unexpected schema changes.

## Topic naming

- `cdc.source.{table}` — Debezium source topics, e.g. `cdc.source.orders`
- `cdc.transformed.{table}` — Post-transform output topics, e.g. `cdc.transformed.orders`
- Partition count: 1 (single-broker local dev)
- Replication factor: 1

## Observability

```bash
# Transform service metrics (Prometheus format)
curl http://localhost:8000/metrics

# Health check
curl http://localhost:8000/health

# Kafka Connect status
curl http://localhost:8083/connectors/postgres-source-connector/status
```

Key metrics:

- `events_processed_total{topic, op}` — Successful events
- `events_failed_total{topic, reason}` — Failed events (envelope, unhandled)
- `events_filtered_total{topic}` — Events dropped by filter rules
- `transform_duration_seconds` — Histogram of transform work
- `ddl_events_total{kind, action}` — DDL events observed
- `service_uptime_seconds` — Service uptime

Kafka consumer-group lag is not exposed in Phase A (it requires an admin
client); this is documented as Phase B work.

## Tests

### Unit tests (fast, offline, no Docker required)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/test_config.py tests/test_envelope.py tests/test_transform.py \
       tests/test_ddl_handler.py tests/test_producer.py tests/test_metrics.py
```

70 tests covering:
- Pydantic config validation (valid config, missing fields, invalid field names, empty YAML)
- Debezium envelope parsing (insert/update/delete/snapshot, malformed JSON, DDL)
- Column mapping (rename, drop, pass-through)
- Filtering (=, !=, <, etc.)
- DDL handler (ADD/DROP/CREATE/ALTER classification, quoted identifiers)
- Producer (delivery callback, produce delegation, close/flush)
- Metrics endpoint (Prometheus format, counter increments, timer observations)

### Integration tests (require running stack)

```bash
docker compose up -d
INTEGRATION=1 pytest tests/test_e2e.py tests/test_ddl_e2e.py \
                    tests/test_restart.py tests/test_health_check.py
```

These verify end-to-end flow, ALTER TABLE auto-evolution, restart behavior,
and connector health-check recovery.

### Smoke test (CI)

A GitHub Actions workflow validates that `docker compose up` works and
rows actually land in the target Postgres on every push.

## Configuration

The transform config is at [`config/transform.yaml`](config/transform.yaml):

```yaml
source_topics:
  - cdc.source.orders
  - cdc.source.customers
output_topic_prefix: cdc.transformed
transforms:
  orders:
    column_map:
      id: order_id
      customer_id: cust_id
    drop_columns:
      - internal_notes
    filters:
      - field: status
        op: "="
        value: "deleted"
```

Each `transforms` block is keyed by the source table name. The transform
service applies `column_map`, then `drop_columns`, then evaluates each
`filter` rule in order. The first matching filter drops the event.

Supported filter ops: `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not in`.

## Known limitations

- **Single-broker local dev.** Topics have 1 partition. A production deployment
  would use 3+ brokers and 3+ partitions per topic.
- **In-memory Schema Registry.** State resets on restart. Phase B will move
  to a backed registry (Postgres) and Avro.
- **`ALTER TABLE DROP COLUMN` is not auto-evolved.** The transform service
  logs a warning; manual intervention is required to drop the column on the
  target. Auto-drop is dangerous (data loss) and rarely desired.
- **No consumer-group lag metric on the transform service.** Deferred to
  Phase B (requires `AdminClient.list_consumer_group_offsets`).
- **JSON message format.** Avro upgrade path is documented but not implemented.
- **No enrichment transforms.** Joins and external lookups are Phase B.

## Project layout

```
.
├── docker-compose.yml           # 9 services, full pipeline
├── Dockerfile.transform         # Python transform service image
├── Dockerfile.data-generator    # Data generator sidecar image
├── pyproject.toml               # Python project + tooling config
├── config/transform.yaml        # Transform rules (Pydantic-validated)
├── connectors/                  # Connector JSON configs
│   ├── source-postgres.json
│   └── jdbc-sink.json
├── sql/source/                  # Init SQL for source Postgres
│   └── 01_schema.sql
├── setup/                       # Shell scripts for setup + health-check
│   ├── register-connectors.sh
│   └── health-check.sh
├── src/
│   ├── data_generator/          # Traffic simulator
│   │   └── main.py
│   └── transform/               # The custom transform service
│       ├── config.py            # Pydantic config model
│       ├── envelope.py          # Debezium JSON parsing
│       ├── transform.py         # Column mapping + filtering
│       ├── ddl_handler.py       # Schema-evolution DDL handling
│       ├── producer.py          # Kafka producer wrapper
│       ├── metrics.py           # Prometheus metrics
│       └── service.py           # FastAPI + consumer loop
└── tests/                       # Unit + integration tests
    ├── conftest.py
    ├── test_config.py
    ├── test_envelope.py
    ├── test_transform.py
    ├── test_ddl_handler.py
    ├── test_producer.py
    ├── test_metrics.py
    ├── test_e2e.py              # INTEGRATION=1
    ├── test_ddl_e2e.py          # INTEGRATION=1
    ├── test_restart.py          # INTEGRATION=1
    └── test_health_check.py     # INTEGRATION=1
```

## Development

```bash
# Lint
ruff check src/transform/ tests/

# Type check
mypy src/transform/ --ignore-missing-imports

# Tests
pytest

# Reset everything (clears volumes)
docker compose down -v
```

## Phase B (roadmap)

- **Multi-source:** Add MySQL via a second Debezium connector.
- **ksqlDB:** Streaming SQL transforms (joins, aggregations).
- **Parquet sink:** Local file output for analytics.
- **Custom Java Kafka Connect sink:** A native Kafka Connect plugin for
  destinations the JDBC sink can't handle.
- **Avro + backed Schema Registry:** Replace JSON with Avro. Move the
  Schema Registry from in-memory to a Postgres-backed store.
- **Consumer-group lag metric** on the transform service.

## License

MIT
